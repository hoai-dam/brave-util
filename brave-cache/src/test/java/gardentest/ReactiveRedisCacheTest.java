package gardentest;

import brave.cache.redis.ReactiveRedisCache;
import brave.cache.util.Tuple;
import brave.extension.RedisServerExtension;
import garden.Fruit;
import garden.FruitCodec;
import garden.Seed;
import garden.SeedCodec;
import io.lettuce.core.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(classes = AppConfig.class)
@ExtendWith({SpringExtension.class, RedisServerExtension.class})
public class ReactiveRedisCacheTest {

    static ReactiveRedisCache<Seed, Fruit> cache;
    static List<Seed> nonExistFruit = List.of(new Seed("wolfberry"), new Seed("zucchini"), new Seed("zinfandel grape"), new Seed("yangmei"));
    static Map<Seed, Integer> volatileFruits = new HashMap<>();

    static {
        volatileFruits.put(new Seed("cucumber"), 0);
        volatileFruits.put(new Seed("damson plum"), 0);
        volatileFruits.put(new Seed("dinosaur eggs"), 0);
    }

    @Autowired
    RedisClient redisClient;

    @BeforeEach
    void beforeEach() {
        if (cache == null) {
            cache = new ReactiveRedisCache.Builder<Seed, Fruit>()
                    .keyCodec(new SeedCodec())
                    .valueCodec(new FruitCodec())
                    .defaultTimeToLive(Duration.ofMinutes(5))
                    .singleLoader(this::load)
                    .multiLoader(this::loadMany)
                    .redisClient(redisClient)
                    .build();
        }
    }

    Mono<Fruit> load(Seed seed) {
        log.warn("Loading {}", seed);
        Integer generation;
        if ((generation = volatileFruits.get(seed)) != null) {
            volatileFruits.put(seed, generation + 1);
            return Mono.just(new Fruit(seed, generation));
        }

        if (nonExistFruit.contains(seed))
            return Mono.empty();

        return Mono.just(new Fruit(seed.getName()));
    }

    Flux<Tuple<Seed, Fruit>> loadMany(Collection<Seed> seeds) {
        log.warn("Loading {} seeds: {}", seeds.size(), seeds);
        return Flux.fromIterable(seeds).map(seed -> {
            Integer generation;
            if ((generation = volatileFruits.get(seed)) != null) {
                volatileFruits.put(seed, generation + 1);
                return Tuple.tuple(seed, new Fruit(seed, generation));
            }
            if (nonExistFruit.contains(seed)) {
                return Tuple.tuple(seed);
            }
            return Tuple.tuple(seed, new Fruit(seed.getName()));
        });
    }

    @AfterAll
    static void afterAll() {
        ReactiveRedisCache<Seed, Fruit> capturedCache = cache;
        if (capturedCache != null) {
            capturedCache.close();
        }
    }

    @Test
    void testPeek() {
        Seed macintosh = new Seed("macintosh");

        StepVerifier.create(cache.peek(macintosh))
                .verifyComplete();

        // Given
        StepVerifier.create(cache.put(macintosh, new Fruit(macintosh)))
                .expectNext(true)
                .verifyComplete();

        // When
        StepVerifier.create(cache.peek(macintosh))
                // Then
                .expectNext(new Fruit(macintosh))
                .verifyComplete();
    }

    @Test
    void testPeekAll() {
        Seed apricots = new Seed("apricots");
        Seed lemon = new Seed("lemon");
        Seed clementine = new Seed("clementine");
        Seed elderberry = new Seed("elderberry");
        Seed hackberry = new Seed("hackberry");

        // Given
        for (Seed seed : List.of(lemon, clementine, apricots, elderberry)) {
            StepVerifier.create(cache.put(seed, new Fruit(seed)))
                    .expectNext(true)
                    .verifyComplete();
        }

        List<Seed> keys = List.of(lemon, clementine, apricots, elderberry, hackberry);

        // When
        StepVerifier.create(cache.peekAll(keys))
                .assertNext(result -> {
                    // Then
                    assertThat(result.keySet()).containsExactlyInAnyOrderElementsOf(keys);
                    assertThat(result.get(hackberry)).isNull();
                })
                .verifyComplete();
    }

    @Test
    void testGet_hitCacheItem() {
        Seed avocados = new Seed("avocados");

        StepVerifier.create(cache.peek(avocados)).verifyComplete();
        StepVerifier.create(cache.put(avocados, new Fruit(avocados))).expectNext(true).verifyComplete();
        StepVerifier.create(cache.get(avocados)).expectNext(new Fruit(avocados)).verifyComplete();
    }

    @Test
    void testGet_missCacheItem() {
        Seed boysenberries = new Seed("boysenberries");

        StepVerifier.create(cache.peek(boysenberries)).verifyComplete();
        StepVerifier.create(cache.get(boysenberries)).expectNext(new Fruit(boysenberries)).verifyComplete();
    }

    @Test
    void testGet_missCacheItem_thenNonExistItem() {
        Seed wolfberry = new Seed("wolfberry");
        StepVerifier.create(cache.peek(wolfberry)).verifyComplete();
        StepVerifier.create(cache.get(wolfberry)).verifyComplete();
    }

    @Test
    void testGetMany() {
        List<Seed> hittingItems = List.of(new Seed("blueberries"), new Seed("bing cherry"), new Seed("cherries"));
        List<Seed> missingButExistItems = List.of(new Seed("cantaloupe"), new Seed("crab apples"));
        List<Seed> missingAndNonExistItems = List.of(new Seed("zucchini"), new Seed("zinfandel grape"), new Seed("yangmei"));

        List<Seed> seeds = Stream.of(hittingItems, missingButExistItems, missingAndNonExistItems)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        // Background
        StepVerifier.create(cache.peekAll(seeds))
                .assertNext(result -> {
                    assertThat(result.keySet()).containsExactlyInAnyOrderElementsOf(seeds);
                    assertThat(result.values()).containsOnlyNulls();
                })
                .verifyComplete();

        // Given
        for (Seed hittingItem : hittingItems) {
            StepVerifier.create(cache.put(hittingItem, new Fruit(hittingItem))).expectNext(true).verifyComplete();
        }

        // When
        StepVerifier.create(cache.getAll(seeds))
                .assertNext(result -> {

                    // Then
                    assertThat(result.keySet()).containsAll(hittingItems);
                    assertThat(result.keySet()).containsAll(missingButExistItems);
                    
                    for (Seed missButExist: missingButExistItems) {
                        assertThat(result.get(missButExist)).isNotNull();
                    }

                    if (result.keySet().containsAll(missingAndNonExistItems)) {
                        for (Seed missAndNonExist : missingAndNonExistItems) {
                            assertThat(result.get(missAndNonExist)).isNull();
                        }
                    } else {
                        assertThat(result.keySet()).doesNotContainAnyElementsOf(missingAndNonExistItems);
                    }
                })
                .verifyComplete();
    }

    @Test
    void testReloadIfExist() {
        Seed cucumber = new Seed("cucumber");

        // Given
        StepVerifier.create(cache.get(cucumber))
                .assertNext(fruit -> assertThat(fruit.getName()).isEqualTo(cucumber.getName()))
                .verifyComplete();


        // When
        Mono<List<Fruit>> cucumberFruits = Flux.concat(
                cache.reloadIfExist(cucumber),
                cache.reloadIfExist(cucumber),
                cache.reloadIfExist(cucumber)
        ).collectList();

        // Then
        StepVerifier.create(cucumberFruits)
                .assertNext(cucumbers -> {
                    assertThat(cucumbers).hasSize(3);

                    Set<Integer> generations = cucumbers.stream()
                            .map(Fruit::getGeneration)
                            .collect(Collectors.toSet());

                    assertThat(generations).hasSize(3);
                })
                .verifyComplete();
    }

    @Test
    void testPut_withTimeToLive() throws InterruptedException {
        Seed dewberrySeed = new Seed("dewberries");
        Fruit dewberry = new Fruit(dewberrySeed);

        // Given
        StepVerifier.create(cache.put(dewberrySeed, dewberry, Duration.ofMillis(500)))
                .expectNext(true)
                .verifyComplete();

        StepVerifier.create(cache.peek(dewberrySeed))
                .expectNext(dewberry)
                .verifyComplete();

        // When
        Thread.sleep(750);

        // Then
        StepVerifier.create(cache.peek(dewberrySeed))
                .verifyComplete();
    }

    @Test
    void testPut_withExpireTimestamp() throws InterruptedException {
        Seed dragonFruitSeed = new Seed("dragon fruit");
        Fruit dragonFruit = new Fruit(dragonFruitSeed);

        // Given
        StepVerifier.create(cache.put(dragonFruitSeed, dragonFruit, System.currentTimeMillis() + 500L))
                .expectNext(true)
                .verifyComplete();

        StepVerifier.create(cache.peek(dragonFruitSeed))
                .expectNext(dragonFruit)
                .verifyComplete();

        // When
        Thread.sleep(750);

        // Then
        StepVerifier.create(cache.peek(dragonFruitSeed))
                .verifyComplete();
    }

    @Test
    void testRemove() {
        Seed eggfruitSeed = new Seed("eggfruit");
        Fruit eggfruit = new Fruit(eggfruitSeed);

        // Given
        StepVerifier.create(cache.put(eggfruitSeed, eggfruit))
                .expectNext(true)
                .verifyComplete();

        StepVerifier.create(cache.peek(eggfruitSeed))
                .expectNext(eggfruit)
                .verifyComplete();

        // When
        StepVerifier.create(cache.remove(eggfruitSeed))
                .expectNext(true)
                .verifyComplete();

        // Then
        StepVerifier.create(cache.peek(eggfruitSeed))
                .verifyComplete();
    }


    @Test
    void testRemoveMany() {
        List<Seed> toBeRemoved = List.of(new Seed("huckleberry"), new Seed("entawak"), new Seed("fig"));
        List<Seed> toBeKept = List.of(new Seed("evergreen "), new Seed("farkleberry"));
        List<Seed> seeds = Stream.of(toBeKept, toBeRemoved)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        // Given
        for (Seed seed : seeds) {
            StepVerifier.create(cache.put(seed, new Fruit(seed))).expectNext(true).verifyComplete();
        }

        StepVerifier.create(cache.peekAll(seeds))
                .assertNext(fruits -> {
                    assertThat(fruits.keySet()).containsExactlyInAnyOrderElementsOf(seeds);
                    assertThat(fruits.values()).doesNotContainNull();
                })
                .verifyComplete();

        // When
        StepVerifier.create(cache.remove(toBeRemoved.toArray(Seed[]::new)))
                .expectNext((long) toBeRemoved.size())
                .verifyComplete();

        // Then
        StepVerifier.create(cache.peekAll(seeds))
                .assertNext(fruits -> {
                    assertThat(fruits.keySet()).containsExactlyInAnyOrderElementsOf(seeds);

                    for (Seed kept : toBeKept) {
                        assertThat(fruits.get(kept)).isNotNull();
                    }

                    for (Seed removed : toBeRemoved) {
                        assertThat(fruits.get(removed)).isNull();
                    }
                })
                .verifyComplete();
    }

    @Test
    void testExpireAtTimestamp() throws InterruptedException {
        Seed fingerLimeSeed = new Seed("finger lime");
        Fruit fingerLime = new Fruit(fingerLimeSeed);

        // Given
        StepVerifier.create(cache.put(fingerLimeSeed, fingerLime))
                .expectNext(true)
                .verifyComplete();

        // When
        long expireAtTimestamp = System.currentTimeMillis() + 2000;
        StepVerifier.create(cache.expireAt(fingerLimeSeed, expireAtTimestamp))
                .expectNext(true)
                .verifyComplete();

        Thread.sleep(500);

        // Then
        StepVerifier.create(cache.peek(fingerLimeSeed))
                .expectNext(fingerLime)
                .verifyComplete();

        // When
        Thread.sleep(2000);

        // Then
        StepVerifier.create(cache.peek(fingerLimeSeed))
                .verifyComplete();
    }

}


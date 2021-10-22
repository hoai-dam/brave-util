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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import redis.embedded.RedisServer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(classes = AppConfig.class)
@ExtendWith({SpringExtension.class, RedisServerExtension.class})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ReactiveRedisCacheErrorTest {

    static ReactiveRedisCache<Seed, Fruit> cache;

    @Autowired
    RedisClient redisClient;

    @BeforeEach
    void beforeEach() {
        if (cache == null) {
            cache = new ReactiveRedisCache.Builder<Seed, Fruit>()
                    .keyCodec(new SeedCodec())
                    .valueCodec(new FruitCodec())
                    .defaultTimeToLive(Duration.ofMinutes(5))
                    .singleLoader(seed -> {
                        log.warn("Loading {}", seed);
                        return Mono.just(new Fruit(seed.getName()));
                    })
                    .multiLoader(seeds -> {
                        log.warn("Loading {} seeds: {}", seeds.size(), seeds);
                        return Flux.fromIterable(seeds)
                                .map(seed -> Tuple.tuple(seed, new Fruit(seed.getName())));
                    })
                    .redisClient(redisClient)
                    .build();
        }
    }

    @AfterAll
    static void afterAll() {
        ReactiveRedisCache<Seed, Fruit> capturedCache = cache;
        if (capturedCache != null) {
            capturedCache.close();
        }
    }

    @Order(value = 0x7fffff00)
    @Test
    void testPeek_shouldBeOkWithRedisError(RedisServer redisServer) {
        Seed adafruit = new Seed("adafruit");

        StepVerifier
                .create(cache.put(adafruit, new Fruit(adafruit)))
                .expectNext(true)
                .verifyComplete();

        StepVerifier
                .create(cache.peek(adafruit))
                .expectNext(new Fruit(adafruit))
                .verifyComplete();

        redisServer.stop();

        StepVerifier
                .create(cache.peek(adafruit))
                .verifyComplete();

        redisServer.start();
    }

    @Order(value = 0x7fffff00 + 1)
    @Test
    void testPeekAll_shouldBeOkWithRedisError(RedisServer redisServer) {
        Seed[] seeds = {new Seed("lemon"), new Seed("clementine"), new Seed("apricots"), new Seed("elderberry"), new Seed("hackberry")};

        for (Seed seed : seeds) {
            StepVerifier.create(cache.put(seed, new Fruit(seed))).expectNext(true).verifyComplete();
        }

        StepVerifier.create(cache.peekAll(seeds))
                .assertNext(result -> assertThat(result.keySet()).containsExactlyInAnyOrder(seeds))
                .verifyComplete();

        redisServer.stop();

        StepVerifier.create(cache.peekAll(seeds))
                .expectNext(Collections.emptyMap())
                .verifyComplete();

        redisServer.start();
    }

    @Order(value = 0x7fffff00 + 2)
    @Test
    void testGet_shouldBeOkWithRedisError(RedisServer redisServer) {
        Seed grapefruitSeed =  new Seed("grapefruit");

        //  Given
        StepVerifier.create(cache.get(grapefruitSeed))
                .expectNext(new Fruit(grapefruitSeed))
                .verifyComplete();

        // When
        redisServer.stop();

        // Then
        StepVerifier.create(cache.get(grapefruitSeed))
                .expectNext(new Fruit(grapefruitSeed))
                .verifyComplete();

        // Restore
        redisServer.start();
    }

    @Order(value = 0x7fffff00 + 3)
    @Test
    void testGetMany_shouldBeOkWithRedisError(RedisServer redisServer) {
        List<Seed> seeds = List.of(
                new Seed("grapes"), new Seed("gooseberries"), new Seed("guava"),
                new Seed("honeydew melon"), new Seed("honeycrisp apples"), new Seed("indian prune")
        );

        // Given
        StepVerifier.create(cache.getAll(seeds))
                .assertNext(fruits -> {
                    assertThat(fruits).hasSize(seeds.size());
                    assertThat(fruits.values()).doesNotContainNull();
                })
                .verifyComplete();

        // When
        redisServer.stop();

        // Then
        StepVerifier.create(cache.getAll(seeds))
                .assertNext(fruits -> {
                    assertThat(fruits).hasSize(seeds.size());
                    assertThat(fruits.values()).doesNotContainNull();
                })
                .verifyComplete();

        // Restore
        redisServer.start();
    }

    @Order(value = 0x7fffff00 + 4)
    @Test
    void testReloadIfExist_shouldFailWithoutThrowing(RedisServer redisServer) {
        Seed indonesianLimeSeed = new Seed("indonesian lime");
        Fruit indonesianLime = new Fruit(indonesianLimeSeed);

        // Given
        StepVerifier.create(cache.put(indonesianLimeSeed, indonesianLime))
                .expectNext(true)
                .verifyComplete();

        // When
        StepVerifier.create(cache.reloadIfExist(indonesianLimeSeed))
                // Then
                .expectNext(indonesianLime)
                .verifyComplete();

        // When
        redisServer.stop();

        // Then
        StepVerifier.create(cache.reloadIfExist(indonesianLimeSeed))
                .verifyComplete();

        // Restore
        redisServer.start();
    }

    @Order(value = 0x7fffff00 + 5)
    @Test
    void testPut_shouldFailWithoutThrowing(RedisServer redisServer) {
        Seed imbeSeed = new Seed("imbe");

        // Given
        StepVerifier.create(cache.peek(imbeSeed)).verifyComplete();
        redisServer.stop();

        // When
        StepVerifier.create(cache.put(imbeSeed, new Fruit(imbeSeed)))
                // Then
                .expectNext(false)
                .verifyComplete();

        // Restore
        redisServer.start();
    }

    @Order(value = 0x7fffff00 + 6)
    @Test
    void testRemove_shouldFailWithoutThrowing(RedisServer redisServer) {
        Seed indianFig = new Seed("indian fig");

        // Given
        StepVerifier.create(cache.peek(indianFig)).verifyComplete();
        redisServer.stop();

        // When
        StepVerifier.create(cache.remove(indianFig))
                // Then
                .expectNext(false)
                .verifyComplete();

        // Restore
        redisServer.start();
    }

    @Order(value = 0x7fffff00 + 7)
    @Test
    void testRemoveMany_shouldFailWithoutThrowing(RedisServer redisServer) {

        Seed[] seeds = {
                new Seed("jackfruit"),
                new Seed("java "),
                new Seed("apple"),
                new Seed("jambolan"),
                new Seed("kaffir lime"),
        };

        // Given
        StepVerifier.create(cache.peek(seeds[0])).verifyComplete();
        redisServer.stop();

        // When
        StepVerifier.create(cache.remove(seeds))
                // Then
                .expectNext(0L)
                .verifyComplete();

        // Restore
        redisServer.start();
    }

}

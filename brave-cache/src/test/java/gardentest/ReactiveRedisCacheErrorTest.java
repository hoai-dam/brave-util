package gardentest;

import brave.cache.redis.ReactiveRedisCache;
import brave.cache.util.Tuple;
import brave.extension.RedisServerExtension;
import garden.Fruit;
import garden.FruitCodec;
import garden.Seed;
import garden.SeedCodec;
import io.lettuce.core.RedisClient;
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

import static org.assertj.core.api.Assertions.assertThat;

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
                    .singleLoader(seed -> Mono.just(new Fruit(seed.getName())))
                    .multiLoader(seeds -> Flux.fromIterable(seeds)
                            .map(seed -> Tuple.tuple(seed, new Fruit(seed.getName()))))
                    .redisClient(redisClient)
                    .build();
        }
    }

    @AfterEach
    void afterEach() {
    }

    @Order(value = 0x7fffff00)
    @Test
    void testPeek_shouldBeOkWithException(RedisServer redisServer) {
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

    @Order(value = 0x7fffff01)
    @Test
    void testPeekAll_shouldBeOkWithException(RedisServer redisServer) {
        Seed[] seeds = {
                new Seed("lemon"),
                new Seed("clementine"),
                new Seed("apricots"),
                new Seed("elderberry"),
                new Seed("hackberry")
        };

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



}

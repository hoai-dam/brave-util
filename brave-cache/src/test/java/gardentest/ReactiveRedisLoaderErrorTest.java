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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(classes = AppConfig.class)
@ExtendWith({SpringExtension.class, RedisServerExtension.class})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ReactiveRedisLoaderErrorTest {

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
                    .singleLoader(this::load)
                    .multiLoader(this::loadMany)
                    .redisClient(redisClient)
                    .build();
        }
    }

    Mono<Fruit> load(Seed seed) {
        log.warn("Loading {}", seed);

        if (seed.getName().contains("boom")) {
            return Mono.error(new IllegalStateException(seed.getName()));
        }

        return Mono.just(new Fruit(seed.getName()));
    }

    Flux<Tuple<Seed, Fruit>> loadMany(Collection<Seed> seeds) {
        log.warn("Loading {} seeds: {}", seeds.size(), seeds);

        if (seeds.contains(new Seed("big boooom"))) {
            return Flux.error(new IllegalStateException("big boooom"));
        }

        return Flux.fromIterable(seeds)
                .map(seed -> {

                    if (seed.getName().contains("boom")) {
                        throw new IllegalStateException(seed.getName());
                    }

                    return Tuple.tuple(seed, new Fruit(seed.getName()));
                });
    }

    @Test
    void loadException_shouldBePropagated() {

        StepVerifier.create(cache.get(new Seed("small boom")))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(IllegalStateException.class);
                    assertThat(error.getMessage()).isEqualTo("small boom");
                })
                .verify();

    }

    @Test
    void loadMany_errorInMidStream_shouldBePropagated() {

        StepVerifier.create(cache.getAll(new Seed[]{new Seed("small boom"), new Seed("another small boom")}))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(IllegalStateException.class);
                    assertThat(error.getMessage()).isIn("small boom", "another small boom");
                })
                .verify();

    }

    @Test
    void loadMany_errorInBeforeStream_shouldBePropagated() {

        StepVerifier.create(cache.getAll(new Seed[]{new Seed("small boom"), new Seed("big boooom")}))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(IllegalStateException.class);
                    assertThat(error.getMessage()).isEqualTo("big boooom");
                })
                .verify();

    }
}

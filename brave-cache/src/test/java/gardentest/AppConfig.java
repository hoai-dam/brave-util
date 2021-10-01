package gardentest;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TimeoutOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
@ComponentScan(basePackages = {"brave", "garden"})
public class AppConfig {

    public int redisPort() {
        return 6789;
    }

    @Bean
    RedisClient redisClient() {
        RedisClient redisClient = RedisClient.create("redis://localhost:" + redisPort());
        redisClient.setOptions(ClientOptions.builder()
                .timeoutOptions(TimeoutOptions.builder()
                        .timeoutCommands(true)
                        .fixedTimeout(Duration.ofSeconds(1))
                        .build())
                .build());
        return redisClient;
    }
}

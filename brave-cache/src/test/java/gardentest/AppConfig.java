package gardentest;

import io.lettuce.core.RedisClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {"brave", "garden"})
public class AppConfig {

    @Bean
    RedisClient redisClient() {
        return RedisClient.create("redis://localhost:6379");
    }
}

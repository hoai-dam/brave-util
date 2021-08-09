package gardentest;

import io.lettuce.core.RedisClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {"brave", "garden"})
public class AppConfig {

    public int redisPort() {
        return 6789;
    }

    @Bean
    RedisClient redisClient() {
        return RedisClient.create("redis://localhost:" + redisPort());
    }
}

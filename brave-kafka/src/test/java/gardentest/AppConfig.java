package gardentest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ComponentScan(basePackages = {"brave", "garden"})
public class AppConfig {

    @Bean
    @Primary
    ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}

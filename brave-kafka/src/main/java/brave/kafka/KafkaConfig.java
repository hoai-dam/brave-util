package brave.kafka;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.EmbeddedValueResolver;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.StringValueResolver;

@Getter
@Configuration
@SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
public class KafkaConfig {

    @Autowired
    private ConfigurableApplicationContext context;

    @Bean
    StringValueResolver stringValueResolver() {
        ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
        return new EmbeddedValueResolver(beanFactory);
    }

    @Bean
    ExpressionParser expressionParser() {
        return new SpelExpressionParser();
    }

    void registerBean(String beanName, Object bean) {
        context.getBeanFactory().registerSingleton(beanName, bean);
    }

}

package brave.cache.annotation;

import brave.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.*;
import org.springframework.beans.factory.config.ConstructorArgumentValues.ValueHolder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.stereotype.Component;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_SINGLETON;

@Slf4j
@Component
public class CacheableAnnotationProcessor implements BeanDefinitionRegistryPostProcessor {

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        for (String cacheableBeanName : registry.getBeanDefinitionNames()) {
            BeanDefinition cacheableBeanDef = registry.getBeanDefinition(cacheableBeanName);
            String cacheableBeanClassName = cacheableBeanDef.getBeanClassName();
            if (StringUtils.isBlank(cacheableBeanClassName)) continue;

            Class<?> cacheableBeanClass;
            try {
                cacheableBeanClass = Class.forName(cacheableBeanClassName);
            } catch (ClassNotFoundException cnfex) {
                continue;
            }

            Cacheable cacheable = cacheableBeanClass.getAnnotation(Cacheable.class);
            if (cacheable == null) continue;

            log.warn("Got bean {} of type {} with {} annotation", cacheableBeanName, cacheableBeanClassName, Cacheable.class.getName());

            String cacheableContextBeanName = cacheable.name() + "Context";
            GenericBeanDefinition cacheContextBeanDef = new GenericBeanDefinition();
            cacheContextBeanDef.setScope(SCOPE_SINGLETON);
            cacheContextBeanDef.setBeanClass(CacheableContext.class);
            cacheContextBeanDef.setDependsOn(cacheableBeanName);

            ConstructorArgumentValues cacheContextConstructorArgs = new ConstructorArgumentValues();
            cacheContextConstructorArgs.addGenericArgumentValue(new ValueHolder(new RuntimeBeanReference(cacheableBeanName)));

            cacheContextBeanDef.setConstructorArgumentValues(cacheContextConstructorArgs);

            registry.registerBeanDefinition(cacheableContextBeanName, cacheContextBeanDef);

            String cacheBeanName = cacheable.name();
            GenericBeanDefinition cacheBeanDef = new GenericBeanDefinition();
            cacheBeanDef.setScope(SCOPE_SINGLETON);
            cacheBeanDef.setBeanClass(Cache.class);
            cacheBeanDef.setDependsOn(cacheableContextBeanName);


            ConstructorArgumentValues cacheFactoryArgs = new ConstructorArgumentValues();
            cacheFactoryArgs.addGenericArgumentValue(new ValueHolder(new RuntimeBeanReference(cacheableContextBeanName)));

            cacheBeanDef.setConstructorArgumentValues(cacheFactoryArgs);
            cacheBeanDef.setFactoryMethodName("fromContext");

            registry.registerBeanDefinition(cacheBeanName, cacheBeanDef);
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

    }

}

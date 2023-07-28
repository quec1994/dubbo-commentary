/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.spring.beans.factory.annotation;

import com.alibaba.spring.util.PropertySourcesUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.alibaba.spring.beans.factory.annotation.ConfigurationBeanBindingPostProcessor.initBeanMetadataAttributes;
import static com.alibaba.spring.beans.factory.annotation.EnableConfigurationBeanBinding.DEFAULT_IGNORE_INVALID_FIELDS;
import static com.alibaba.spring.beans.factory.annotation.EnableConfigurationBeanBinding.DEFAULT_IGNORE_UNKNOWN_FIELDS;
import static com.alibaba.spring.beans.factory.annotation.EnableConfigurationBeanBinding.DEFAULT_MULTIPLE;
import static com.alibaba.spring.util.AnnotationUtils.getAttribute;
import static com.alibaba.spring.util.AnnotationUtils.getRequiredAttribute;
import static com.alibaba.spring.util.BeanRegistrar.registerInfrastructureBean;
import static com.alibaba.spring.util.PropertySourcesUtils.getSubProperties;
import static com.alibaba.spring.util.PropertySourcesUtils.normalizePrefix;
import static java.lang.Boolean.valueOf;
import static java.util.Collections.singleton;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;

/**
 * The {@link ImportBeanDefinitionRegistrar} implementation for {@link EnableConfigurationBeanBinding @EnableConfigurationBinding}
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 1.0.3
 */
public class ConfigurationBeanBindingRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    final static Class ENABLE_CONFIGURATION_BINDING_CLASS = EnableConfigurationBeanBinding.class;

    private final static String ENABLE_CONFIGURATION_BINDING_CLASS_NAME = ENABLE_CONFIGURATION_BINDING_CLASS.getName();

    private final Log log = LogFactory.getLog(getClass());

    private ConfigurableEnvironment environment;

    @Override
    public void registerBeanDefinitions(AnnotationMetadata metadata, BeanDefinitionRegistry registry) {

        Map<String, Object> attributes = metadata.getAnnotationAttributes(ENABLE_CONFIGURATION_BINDING_CLASS_NAME);

        registerConfigurationBeanDefinitions(attributes, registry);
    }

    public void registerConfigurationBeanDefinitions(Map<String, Object> attributes, BeanDefinitionRegistry registry) {

        String prefix = getRequiredAttribute(attributes, "prefix");
        // dubbo.protocols

        // 利用环境变量解析占位符
        prefix = environment.resolvePlaceholders(prefix);
        // dubbo.protocols

        Class<?> configClass = getRequiredAttribute(attributes, "type");
        // org.apache.dubbo.config.ProtocolConfig

        boolean multiple = getAttribute(attributes, "multiple", valueOf(DEFAULT_MULTIPLE));
        // true

        boolean ignoreUnknownFields = getAttribute(attributes, "ignoreUnknownFields", valueOf(DEFAULT_IGNORE_UNKNOWN_FIELDS));
        // true

        boolean ignoreInvalidFields = getAttribute(attributes, "ignoreInvalidFields", valueOf(DEFAULT_IGNORE_INVALID_FIELDS));
        // true

        registerConfigurationBeans(prefix, configClass, multiple, ignoreUnknownFields, ignoreInvalidFields, registry);
    }


    private void registerConfigurationBeans(String prefix, Class<?> configClass, boolean multiple,
                                            boolean ignoreUnknownFields, boolean ignoreInvalidFields,
                                            BeanDefinitionRegistry registry) {

        // 从spring读取到的配置源中根据前缀筛选出对应的配置项，并且将配置项的前缀去除
        Map<String, Object> configurationProperties = PropertySourcesUtils.getSubProperties(environment.getPropertySources(), environment, prefix);

        // 如果没有相关的配置项，则不需要注册BeanDefinition
        if (CollectionUtils.isEmpty(configurationProperties)) {
            if (log.isDebugEnabled()) {
                log.debug("There is no property for binding to configuration class [" + configClass.getName()
                        + "] within prefix [" + prefix + "]");
            }
            return;
        }

        // 根据配置项生成beanNames，为什么会有多个？
        // 普通情况一个dubbo.protocol前缀拿到的configurationProperties是 {"name":"dubbo","port":"20880"}，
        // 对应一个ProtocolConfig类型的Bean；
        // 特殊情况下，比如dubbo.protocols前缀最后拿到的configurationProperties是 {"p1.name":"dubbo","p1.port":"20880","p2.name":"dubbo","p2.port":"20880"}
        // 那么就对应两个ProtocolConfig类型的Bean，那么就有两个beanName:p1和p2

        // 这里就是multiple为true或false的区别，bean名字的区别，根据multiple用来判断是否从配置项中获取beanName
        // 如果multiple为false，则看有没有配置id属性，如果没有配置则自动生成一个beanName.
        Set<String> beanNames = multiple ? resolveMultipleBeanNames(configurationProperties) :
                singleton(resolveSingleBeanName(configurationProperties, configClass, registry));

        for (String beanName : beanNames) {

            // 为每个beanName,注册一个空的带配置项属性的BeanDefinition
            registerConfigurationBean(beanName, configClass, multiple, ignoreUnknownFields, ignoreInvalidFields,
                    configurationProperties, registry);
        }

        // 注册一个ConfigurationBeanBindingPostProcessor的Bean后置处理器，用来将配置项和XxxConfig对象实例属性绑定
        registerConfigurationBindingBeanPostProcessor(registry);
    }

    private void registerConfigurationBean(String beanName, Class<?> configClass, boolean multiple,
                                           boolean ignoreUnknownFields, boolean ignoreInvalidFields,
                                           Map<String, Object> configurationProperties,
                                           BeanDefinitionRegistry registry) {

        BeanDefinitionBuilder builder = rootBeanDefinition(configClass);

        AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();

        // 做记号，把 EnableConfigurationBeanBinding.class 放入beanDefinition的source
        setSource(beanDefinition);

        // 筛选beanName配置项，并且去除配置项key的beanName前缀
        Map<String, Object> subProperties = resolveSubProperties(multiple, beanName, configurationProperties);

        // 将数据仿佛beanDefinition的属性集合中
        initBeanMetadataAttributes(beanDefinition, subProperties, ignoreUnknownFields, ignoreInvalidFields);

        registry.registerBeanDefinition(beanName, beanDefinition);

        if (log.isInfoEnabled()) {
            log.info("The configuration bean definition [name : " + beanName + ", content : " + beanDefinition
                    + "] has been registered.");
        }
    }

    private Map<String, Object> resolveSubProperties(boolean multiple, String beanName,
                                                     Map<String, Object> configurationProperties) {
        if (!multiple) {
            return configurationProperties;
        }

        // 构造一个动态多属性源
        MutablePropertySources propertySources = new MutablePropertySources();

        // 将配置项包装成属性源放入
        propertySources.addLast(new MapPropertySource("_", configurationProperties));

        // 从配置项Map中筛选出beanName前缀的配置项，并且将配置项的beanName前缀去除
        return getSubProperties(propertySources, environment, normalizePrefix(beanName));
    }

    private void setSource(AbstractBeanDefinition beanDefinition) {
        beanDefinition.setSource(ENABLE_CONFIGURATION_BINDING_CLASS);
    }

    private void registerConfigurationBindingBeanPostProcessor(BeanDefinitionRegistry registry) {
        registerInfrastructureBean(registry, ConfigurationBeanBindingPostProcessor.BEAN_NAME,
                ConfigurationBeanBindingPostProcessor.class);
    }

    @Override
    public void setEnvironment(Environment environment) {

        Assert.isInstanceOf(ConfigurableEnvironment.class, environment);

        this.environment = (ConfigurableEnvironment) environment;

    }

    private Set<String> resolveMultipleBeanNames(Map<String, Object> properties) {

        Set<String> beanNames = new LinkedHashSet<String>();

        // 比如dubbo.protocols.p1.name=dubbo的propertyName为p1.name

        for (String propertyName : properties.keySet()) {
            // propertyName为p1.name

            int index = propertyName.indexOf(".");

            if (index > 0) {

                // 截取beanName名字为p1
                String beanName = propertyName.substring(0, index);

                beanNames.add(beanName);
            }

        }

        return beanNames;

    }

    private String resolveSingleBeanName(Map<String, Object> properties, Class<?> configClass,
                                         BeanDefinitionRegistry registry) {

        // 配置了dubbo.application.id=appl，那么appl就是beanName
        String beanName = (String) properties.get("id");

        if (!StringUtils.hasText(beanName)) {
            // 如果beanName为null，则会进入if分支，由spring自动生成一个beanName,比如org.apache.dubbo.config.ApplicationConfig#0
            BeanDefinitionBuilder builder = rootBeanDefinition(configClass);
            beanName = BeanDefinitionReaderUtils.generateBeanName(builder.getRawBeanDefinition(), registry);
        }

        return beanName;

    }
}

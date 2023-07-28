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

import com.alibaba.spring.context.config.ConfigurationBeanBinder;
import com.alibaba.spring.context.config.ConfigurationBeanCustomizer;
import com.alibaba.spring.context.config.DefaultConfigurationBeanBinder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.core.PriorityOrdered;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.spring.beans.factory.annotation.ConfigurationBeanBindingRegistrar.ENABLE_CONFIGURATION_BINDING_CLASS;
import static com.alibaba.spring.util.WrapperUtils.unwrap;
import static org.springframework.beans.factory.BeanFactoryUtils.beansOfTypeIncludingAncestors;
import static org.springframework.core.annotation.AnnotationAwareOrderComparator.sort;
import static org.springframework.util.ClassUtils.getUserClass;
import static org.springframework.util.ObjectUtils.nullSafeEquals;

/**
 * The {@link BeanPostProcessor} class to bind the configuration bean
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 1.0.3
 */
@SuppressWarnings("unchecked")
public class ConfigurationBeanBindingPostProcessor implements BeanPostProcessor, BeanFactoryAware, PriorityOrdered {

    /**
     * The bean name of {@link ConfigurationBeanBindingPostProcessor}
     */
    public static final String BEAN_NAME = "configurationBeanBindingPostProcessor";

    static final String CONFIGURATION_PROPERTIES_ATTRIBUTE_NAME = "configurationProperties";

    static final String IGNORE_UNKNOWN_FIELDS_ATTRIBUTE_NAME = "ignoreUnknownFields";

    static final String IGNORE_INVALID_FIELDS_ATTRIBUTE_NAME = "ignoreInvalidFields";

    private final Log log = LogFactory.getLog(getClass());

    private ConfigurableListableBeanFactory beanFactory = null;

    private ConfigurationBeanBinder configurationBeanBinder = null;

    private List<ConfigurationBeanCustomizer> configurationBeanCustomizers = null;

    private int order = LOWEST_PRECEDENCE;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {

        // 根据Bean名称获取BeanDefinition
        BeanDefinition beanDefinition = getNullableBeanDefinition(beanName);

        // 判断需不需要处理
        if (isConfigurationBean(bean, beanDefinition)) {
            bindConfigurationBean(bean, beanDefinition);
            // 对bean做定制化处理
            // 默认的定制化处理：用来把某个XxConfig所对应的beanName设置到name属性中去——已废弃状态
            customize(beanName, bean);
        }

        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    /**
     * Set the order for current instance
     *
     * @param order the order
     */
    public void setOrder(int order) {
        this.order = order;
    }

    public ConfigurationBeanBinder getConfigurationBeanBinder() {
        if (configurationBeanBinder == null) {
            initConfigurationBeanBinder();
        }
        return configurationBeanBinder;
    }

    public void setConfigurationBeanBinder(ConfigurationBeanBinder configurationBeanBinder) {
        this.configurationBeanBinder = configurationBeanBinder;
    }

    /**
     * Get the {@link List} of {@link ConfigurationBeanCustomizer ConfigurationBeanCustomizers}
     *
     * @return non-null
     * @since 1.0.6
     */
    public List<ConfigurationBeanCustomizer> getConfigurationBeanCustomizers() {
        if (configurationBeanCustomizers == null) {
            initBindConfigurationBeanCustomizers();
        }
        return configurationBeanCustomizers;
    }

    public void setConfigurationBeanCustomizers(Collection<ConfigurationBeanCustomizer> configurationBeanCustomizers) {
        List<ConfigurationBeanCustomizer> customizers =
                new ArrayList<ConfigurationBeanCustomizer>(configurationBeanCustomizers);
        sort(customizers);
        this.configurationBeanCustomizers = Collections.unmodifiableList(customizers);
    }

    private BeanDefinition getNullableBeanDefinition(String beanName) {
        return beanFactory.containsBeanDefinition(beanName) ? beanFactory.getBeanDefinition(beanName) : null;
    }

    private boolean isConfigurationBean(Object bean, BeanDefinition beanDefinition) {
        return beanDefinition != null &&
                // 判断BeanDefinition上有没有标记，处理源是不是这个类
                ENABLE_CONFIGURATION_BINDING_CLASS.equals(beanDefinition.getSource())
                // 自己使用@Bean方式交给spring管理的Bean没有BeanDefinition
                && nullSafeEquals(getBeanClassName(bean), beanDefinition.getBeanClassName());
    }

    private String getBeanClassName(Object bean) {
        return getUserClass(bean.getClass()).getName();
    }

    private void bindConfigurationBean(Object configurationBean, BeanDefinition beanDefinition) {

        // 从beanDefinition属性中取出保存的配置项
        Map<String, Object> configurationProperties = getConfigurationProperties(beanDefinition);

        boolean ignoreUnknownFields = getIgnoreUnknownFields(beanDefinition);

        boolean ignoreInvalidFields = getIgnoreInvalidFields(beanDefinition);

        // 将配置项和配置Bean的属性绑定，后面spring生成Bean的时候会自动赋值
        getConfigurationBeanBinder().bind(configurationProperties, ignoreUnknownFields, ignoreInvalidFields, configurationBean);

        if (log.isInfoEnabled()) {
            log.info("The configuration bean [" + configurationBean + "] have been binding by the " +
                    "configuration properties [" + configurationProperties + "]");
        }
    }

    private void initConfigurationBeanBinder() {
        if (configurationBeanBinder == null) {
            try {
                configurationBeanBinder = beanFactory.getBean(ConfigurationBeanBinder.class);
            } catch (BeansException ignored) {
                if (log.isInfoEnabled()) {
                    log.info("configurationBeanBinder Bean can't be found in ApplicationContext.");
                }
                // Use Default implementation
                configurationBeanBinder = defaultConfigurationBeanBinder();
            }
        }
    }

    private void initBindConfigurationBeanCustomizers() {
        Collection<ConfigurationBeanCustomizer> customizers =
                beansOfTypeIncludingAncestors(beanFactory, ConfigurationBeanCustomizer.class).values();
        setConfigurationBeanCustomizers(customizers);
    }

    private void customize(String beanName, Object configurationBean) {
        for (ConfigurationBeanCustomizer customizer : getConfigurationBeanCustomizers()) {
            customizer.customize(beanName, configurationBean);
        }
    }

    /**
     * Create {@link ConfigurationBeanBinder} instance.
     *
     * @return {@link DefaultConfigurationBeanBinder}
     */
    private ConfigurationBeanBinder defaultConfigurationBeanBinder() {
        return new DefaultConfigurationBeanBinder();
    }

    static void initBeanMetadataAttributes(AbstractBeanDefinition beanDefinition,
                                           Map<String, Object> configurationProperties,
                                           boolean ignoreUnknownFields, boolean ignoreInvalidFields) {
        // 放入配置项
        beanDefinition.setAttribute(CONFIGURATION_PROPERTIES_ATTRIBUTE_NAME, configurationProperties);
        beanDefinition.setAttribute(IGNORE_UNKNOWN_FIELDS_ATTRIBUTE_NAME, ignoreUnknownFields);
        beanDefinition.setAttribute(IGNORE_INVALID_FIELDS_ATTRIBUTE_NAME, ignoreInvalidFields);
    }

    private static <T> T getAttribute(BeanDefinition beanDefinition, String attributeName) {
        return (T) beanDefinition.getAttribute(attributeName);
    }

    private static Map<String, Object> getConfigurationProperties(BeanDefinition beanDefinition) {
        return getAttribute(beanDefinition, CONFIGURATION_PROPERTIES_ATTRIBUTE_NAME);
    }

    private static boolean getIgnoreUnknownFields(BeanDefinition beanDefinition) {
        return getAttribute(beanDefinition, IGNORE_UNKNOWN_FIELDS_ATTRIBUTE_NAME);
    }

    private static boolean getIgnoreInvalidFields(BeanDefinition beanDefinition) {
        return getAttribute(beanDefinition, IGNORE_INVALID_FIELDS_ATTRIBUTE_NAME);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = unwrap(beanFactory);
    }

    @Override
    public int getOrder() {
        return order;
    }
}

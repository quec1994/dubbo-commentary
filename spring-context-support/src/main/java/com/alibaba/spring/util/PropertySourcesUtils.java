package com.alibaba.spring.util;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertyResolver;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySources;
import org.springframework.core.env.PropertySourcesPropertyResolver;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static com.alibaba.spring.util.ObjectUtils.EMPTY_STRING_ARRAY;
import static java.util.Collections.unmodifiableMap;

/**
 * {@link PropertySources} Utilities
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see PropertySources
 * @since 2017.01.19
 */
public abstract class PropertySourcesUtils {

    /**
     * Get Sub {@link Properties}
     *
     * @param propertySources {@link PropertySource} Iterable
     * @param prefix          the prefix of property name
     * @return Map
     * @see Properties
     */
    public static Map<String, Object> getSubProperties(Iterable<PropertySource<?>> propertySources, String prefix) {

        MutablePropertySources mutablePropertySources = new MutablePropertySources();

        for (PropertySource<?> source : propertySources) {
            mutablePropertySources.addLast(source);
        }

        return getSubProperties(mutablePropertySources, prefix);

    }

    /**
     * Get Sub {@link Properties}
     *
     * @param environment {@link ConfigurableEnvironment}
     * @param prefix      the prefix of property name
     * @return Map
     * @see Properties
     */
    public static Map<String, Object> getSubProperties(ConfigurableEnvironment environment, String prefix) {
        return getSubProperties(environment.getPropertySources(), environment, prefix);
    }

    /**
     * Normalize the prefix
     *
     * @param prefix the prefix
     * @return the prefix
     */
    public static String normalizePrefix(String prefix) {
        return prefix.endsWith(".") ? prefix : prefix + ".";
    }

    /**
     * Get prefixed {@link Properties}
     *
     * @param propertySources {@link PropertySources}
     * @param prefix          the prefix of property name
     * @return Map
     * @see Properties
     * @since 1.0.3
     */
    public static Map<String, Object> getSubProperties(PropertySources propertySources, String prefix) {
        return getSubProperties(propertySources, new PropertySourcesPropertyResolver(propertySources), prefix);
    }

    /**
     * Get prefixed {@link Properties}
     *
     * @param propertySources  {@link PropertySources}
     * @param propertyResolver {@link PropertyResolver} to resolve the placeholder if present
     * @param prefix           the prefix of property name
     * @return Map
     * @see Properties
     * @since 1.0.3
     */
    public static Map<String, Object> getSubProperties(PropertySources propertySources, PropertyResolver propertyResolver, String prefix) {
        // 比如根据dubbo.protocol前缀，就可以匹配到配置项：
        // dubbo.protocol.name=dubbo
        // dubbo.protocol.port=20880
        // 最后拿到的结果为 {"name":"dubbo","port":"20880"}
        // 如果根据dubbo.protocols对应对应的配置项为：
        //    dubbo.protocols.p1.name=dubbo
        //    dubbo.protocols.p1.port=20880
        //    dubbo.protocols.p2.name=rest
        //    dubbo.protocols.p2.port=8080
        // 最后拿到的结果为 {"p1.name":"dubbo","p1.port":"20880","p2.name":"dubbo","p2.port":"20880"}

        // 这个方法返回的读取到的配置
        Map<String, Object> subProperties = new LinkedHashMap<String, Object>();

        // 归一化前缀，将prefix归一化为以.结尾
        String normalizedPrefix = normalizePrefix(prefix);

        // spring读取的配置源
        Iterator<PropertySource<?>> iterator = propertySources.iterator();

        while (iterator.hasNext()) {
            PropertySource<?> source = iterator.next();
            // 配置源中配置的配置key
            for (String name : getPropertyNames(source)) {
                // 判断配置项的key和前缀是否相符
                if (!subProperties.containsKey(name) && name.startsWith(normalizedPrefix)) {

                    // 截取前缀后面的字符
                    // 比如 dubbo.application.name=dubbo-demo-annotation-provider => name=dubbo-demo-annotation-provider
                    // 比如 dubbo.protocols.p1.name=dubbo => p1.name=dubbo
                    String subName = name.substring(normalizedPrefix.length());
                    if (!subProperties.containsKey(subName)) { // take first one
                        // 获取配置的值
                        Object value = source.getProperty(name);
                        if (value instanceof String) {
                            // Resolve placeholder
                            // 解析spring占位符
                            value = propertyResolver.resolvePlaceholders((String) value);
                        }
                        // 放入方法返回值中
                        subProperties.put(subName, value);
                    }
                }
            }
        }

        return unmodifiableMap(subProperties);
    }

    /**
     * Get the property names as the array from the specified {@link PropertySource} instance.
     *
     * @param propertySource {@link PropertySource} instance
     * @return non-null
     * @since 1.0.10
     */
    public static String[] getPropertyNames(PropertySource propertySource) {
        String[] propertyNames = propertySource instanceof EnumerablePropertySource ?
                ((EnumerablePropertySource) propertySource).getPropertyNames() : null;

        if (propertyNames == null) {
            propertyNames = EMPTY_STRING_ARRAY;
        }

        return propertyNames;
    }
}

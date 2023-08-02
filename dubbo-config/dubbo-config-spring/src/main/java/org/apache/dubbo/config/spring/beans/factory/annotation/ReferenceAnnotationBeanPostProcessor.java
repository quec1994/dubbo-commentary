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
package org.apache.dubbo.config.spring.beans.factory.annotation;

import com.alibaba.spring.beans.factory.annotation.AbstractAnnotationBeanPostProcessor;
import com.alibaba.spring.util.AnnotationUtils;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.spring.ReferenceBean;
import org.apache.dubbo.config.spring.ServiceBean;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.InjectionMetadata;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.util.ObjectUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.alibaba.spring.util.AnnotationUtils.getAttribute;
import static com.alibaba.spring.util.AnnotationUtils.getAttributes;
import static org.apache.dubbo.config.spring.beans.factory.annotation.ServiceBeanNameBuilder.create;
import static org.springframework.util.StringUtils.hasText;

/**
 * {@link org.springframework.beans.factory.config.BeanPostProcessor} implementation
 * that Consumer service {@link Reference} annotated fields
 *
 * @see DubboReference
 * @see Reference
 * @see com.alibaba.dubbo.config.annotation.Reference
 * @since 2.5.7
 */
public class ReferenceAnnotationBeanPostProcessor extends AbstractAnnotationBeanPostProcessor implements
        ApplicationContextAware, ApplicationListener {

    private static final Logger logger = LoggerFactory.getLogger(ReferenceAnnotationBeanPostProcessor.class);

    /**
     * The bean name of {@link ReferenceAnnotationBeanPostProcessor}
     */
    public static final String BEAN_NAME = "referenceAnnotationBeanPostProcessor";

    /**
     * Cache size
     */
    private static final int CACHE_SIZE = Integer.getInteger(BEAN_NAME + ".cache.size", 32);

    private final ConcurrentMap<String, ReferenceBean<?>> referenceBeanCache =
            new ConcurrentHashMap<>(CACHE_SIZE);

    private final ConcurrentMap<InjectionMetadata.InjectedElement, ReferenceBean<?>> injectedFieldReferenceBeanCache =
            new ConcurrentHashMap<>(CACHE_SIZE);

    private final ConcurrentMap<InjectionMetadata.InjectedElement, ReferenceBean<?>> injectedMethodReferenceBeanCache =
            new ConcurrentHashMap<>(CACHE_SIZE);

    private ApplicationContext applicationContext;

    private static Map<String, TreeSet<String>> referencedBeanNameIdx = new HashMap<>();

    /**
     * {@link com.alibaba.dubbo.config.annotation.Reference @com.alibaba.dubbo.config.annotation.Reference} has been supported since 2.7.3
     * <p>
     * {@link DubboReference @DubboReference} has been supported since 2.7.7
     */
    public ReferenceAnnotationBeanPostProcessor() {
        super(DubboReference.class, Reference.class, com.alibaba.dubbo.config.annotation.Reference.class);
        setClassValuesAsString(false);
        setNestedAnnotationsAsMap(false);
    }

    /**
     * Gets all beans of {@link ReferenceBean}
     *
     * @return non-null read-only {@link Collection}
     * @since 2.5.9
     */
    public Collection<ReferenceBean<?>> getReferenceBeans() {
        return referenceBeanCache.values();
    }

    /**
     * Get {@link ReferenceBean} {@link Map} in injected field.
     *
     * @return non-null {@link Map}
     * @since 2.5.11
     */
    public Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> getInjectedFieldReferenceBeanMap() {
        return Collections.unmodifiableMap(injectedFieldReferenceBeanCache);
    }

    /**
     * Get {@link ReferenceBean} {@link Map} in injected method.
     *
     * @return non-null {@link Map}
     * @since 2.5.11
     */
    public Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> getInjectedMethodReferenceBeanMap() {
        return Collections.unmodifiableMap(injectedMethodReferenceBeanCache);
    }

    @Override
    protected Object doGetInjectedBean(AnnotationAttributes attributes, Object bean, String beanName, Class<?> injectedType,
                                       InjectionMetadata.InjectedElement injectedElement) throws Exception {
        // 这个方法返回的引用对象最终会赋值给@DubboReference注解修饰的Bean属性/方法

        /**
         * The name of bean that annotated Dubbo's {@link Service @Service} in local Spring {@link ApplicationContext}
         */
        // 按ServiceBean的beanName生成规则来生成referencedBeanName
        // referencedBeanName 被引用的Bean对象实例名称
        String referencedBeanName = buildReferencedBeanName(attributes, injectedType);
        // ServiceBean:org.apache.dubbo.demo.DemoService

        /**
         * The name of bean that is declared by {@link Reference @Reference} annotation injection
         */
        // 根据@DubboReference注解的信息生成referenceBeanName
        // referenceBeanName 引用Bean对象实例名称
        String referenceBeanName = getReferenceBeanName(attributes, injectedType);
        // @Reference org.apache.dubbo.demo.DemoService

        // 保存referencedBeanName和referenceBeanName的映射关系
        referencedBeanNameIdx.computeIfAbsent(referencedBeanName, k -> new TreeSet<String>()).add(referenceBeanName);

        // 生成一个ReferenceBean对象
        ReferenceBean referenceBean = buildReferenceBeanIfAbsent(referenceBeanName, attributes, injectedType);

        // 判断被引用的Bean对象实例（ServiceBean）是不是要使用本JVM中的Bean对象实例
        boolean localServiceBean = isLocalServiceBean(referencedBeanName, referenceBean, attributes);

        // 预处理
        prepareReferenceBean(referencedBeanName, referenceBean, localServiceBean);

        // 在spring上下文中注册referenceBeanName对应的对象
        registerReferenceBean(referencedBeanName, referenceBean, localServiceBean, referenceBeanName);

        // 缓存
        cacheInjectedReferenceBean(referenceBean, injectedElement);

        // 执行referenceBean.get()创建一个代理对象，DemoServiceComponent.demoService属性被注入的就是这个代理对象
        return getBeanFactory().applyBeanPostProcessorsAfterInitialization(referenceBean.get(), referenceBeanName);
    }

    /**
     * Register an instance of {@link ReferenceBean} as a Spring Bean
     *
     * @param referencedBeanName The name of bean that annotated Dubbo's {@link Service @Service} in the Spring {@link ApplicationContext}
     * @param referenceBean      the instance of {@link ReferenceBean} is about to register into the Spring {@link ApplicationContext}
     * @param attributes         the {@link AnnotationAttributes attributes} of {@link Reference @Reference}
     * @param localServiceBean   Is Local Service bean or not
     * @param interfaceClass     the {@link Class class} of Service interface
     * @since 2.7.3
     */
    private void registerReferenceBean(String referencedBeanName, ReferenceBean referenceBean,
                                       boolean localServiceBean, String beanName) {

        ConfigurableListableBeanFactory beanFactory = getBeanFactory();

        if (localServiceBean) {  // If @Service bean is local one
            // 被引用的Bean对象实例（ServiceBean）使用本JVM中的Bean对象实例

            /**
             * Get  the @Service's BeanDefinition from {@link BeanFactory}
             * Refer to {@link ServiceClassPostProcessor#buildServiceBeanDefinition}
             */
            // ServiceBean 的 beanDefinition
            AbstractBeanDefinition beanDefinition = (AbstractBeanDefinition) beanFactory.getBeanDefinition(referencedBeanName);
            // 获取ServiceBean对应的服务接口实现，也就是 DemoServiceComponent 类的对象实例
            RuntimeBeanReference runtimeBeanReference = (RuntimeBeanReference) beanDefinition.getPropertyValues().get("ref");

            // The name of bean annotated @Service
            // DemoServiceComponent对应的beanName
            String serviceBeanName = runtimeBeanReference.getBeanName();
            // demoServiceComponent

            // register Alias rather than a new bean name, in order to reduce duplicated beans
            // DemoServiceComponent Bean对象实例多了一个别名，比如 demoServiceComponent 和 @Reference org.apache.dubbo.demo.DemoService
            beanFactory.registerAlias(serviceBeanName, beanName);
        } else { // Remote @Service Bean
            // 被引用的Bean对象实例（ServiceBean）在远端

            if (!beanFactory.containsBean(beanName)) {
                // 在spring上下文中注册referenceBean
                beanFactory.registerSingleton(beanName, referenceBean);
            }
        }
    }

    /**
     * Get the bean name of {@link ReferenceBean} if {@link Reference#id() id attribute} is present,
     * or {@link #generateReferenceBeanName(AnnotationAttributes, Class) generate}.
     *
     * @param attributes     the {@link AnnotationAttributes attributes} of {@link Reference @Reference}
     * @param interfaceClass the {@link Class class} of Service interface
     * @return non-null
     * @since 2.7.3
     */
    private String getReferenceBeanName(AnnotationAttributes attributes, Class<?> interfaceClass) {
        // id attribute appears since 2.7.3
        // 如果配置了id则取id
        String beanName = getAttribute(attributes, "id");
        if (!hasText(beanName)) {
            // 没有配置id则自动构造一个
            beanName = generateReferenceBeanName(attributes, interfaceClass);
            // @Reference org.apache.dubbo.demo.DemoService
        }
        return beanName;
    }

    /**
     * Build the bean name of {@link ReferenceBean}
     *
     * @param attributes     the {@link AnnotationAttributes attributes} of {@link Reference @Reference}
     * @param interfaceClass the {@link Class class} of Service interface
     * @return
     * @since 2.7.3
     */
    private String generateReferenceBeanName(AnnotationAttributes attributes, Class<?> interfaceClass) {
        // 构造ReferenceBean的名字
        // 格式：@Reference 的配置信息+服务接口类全限定名
        // @Reference(parameters=[key1=value1, key2=value2],version=version) org.apache.dubbo.demo.DemoService

        StringBuilder beanNameBuilder = new StringBuilder("@Reference");

        if (!attributes.isEmpty()) {
            beanNameBuilder.append('(');
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                String value;
                if ("parameters".equals(entry.getKey())) {
                    ArrayList<String> pairs = getParameterPairs(entry);
                    value = convertAttribute(pairs.stream().sorted().toArray());
                } else {
                    value = convertAttribute(entry.getValue());
                }
                beanNameBuilder.append(entry.getKey())
                        .append('=')
                        .append(value)
                        .append(',');
            }
            // replace the latest "," to be ")"
            beanNameBuilder.setCharAt(beanNameBuilder.lastIndexOf(","), ')');
        }

        beanNameBuilder.append(" ").append(interfaceClass.getName());

        return beanNameBuilder.toString();
    }

    private ArrayList<String> getParameterPairs(Map.Entry<String, Object> entry) {
        String[] entryValues = (String[]) entry.getValue();
        ArrayList<String> pairs = new ArrayList<>();
        // parameters spec is {key1,value1,key2,value2}
        for (int i = 0; i < entryValues.length / 2 * 2; i = i + 2) {
            pairs.add(entryValues[i] + "=" + entryValues[i + 1]);
        }
        return pairs;
    }

    private String convertAttribute(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Annotation) {
            AnnotationAttributes attributes = AnnotationUtils.getAnnotationAttributes((Annotation) obj, true);
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                entry.setValue(convertAttribute(entry.getValue()));
            }
            return String.valueOf(attributes);
        } else if (obj.getClass().isArray()) {
            Object[] array = ObjectUtils.toObjectArray(obj);
            String[] newArray = new String[array.length];
            for (int i = 0; i < array.length; i++) {
                newArray[i] = convertAttribute(array[i]);
            }
            return Arrays.toString(Arrays.stream(newArray).sorted().toArray());
        } else {
            return String.valueOf(obj);
        }
    }

    /**
     * Is Local Service bean or not?
     *
     * @param referencedBeanName the bean name to the referenced bean
     * @return If the target referenced bean is existed, return <code>true</code>, or <code>false</code>
     * @since 2.7.6
     */
    private boolean isLocalServiceBean(String referencedBeanName, ReferenceBean referenceBean, AnnotationAttributes attributes) {
        // 当前Spring容器中是否存在referencedBeanName
        // 在@DubboReference 注解中的injvm属性是否配置为false
        return existsServiceBean(referencedBeanName) && !isRemoteReferenceBean(referenceBean, attributes);
    }

    /**
     * Check the {@link ServiceBean} is exited or not
     *
     * @param referencedBeanName the bean name to the referenced bean
     * @return if exists, return <code>true</code>, or <code>false</code>
     * @revised 2.7.6
     */
    private boolean existsServiceBean(String referencedBeanName) {
        return applicationContext.containsBean(referencedBeanName) &&
                applicationContext.isTypeMatch(referencedBeanName, ServiceBean.class);

    }

    private boolean isRemoteReferenceBean(ReferenceBean referenceBean, AnnotationAttributes attributes) {
        boolean remote = Boolean.FALSE.equals(referenceBean.isInjvm()) || Boolean.FALSE.equals(attributes.get("injvm"));
        return remote;
    }

    /**
     * Prepare {@link ReferenceBean}
     *
     * @param referencedBeanName The name of bean that annotated Dubbo's {@link DubboService @DubboService}
     *                           in the Spring {@link ApplicationContext}
     * @param referenceBean      the instance of {@link ReferenceBean}
     * @param localServiceBean   Is Local Service bean or not
     * @since 2.7.8
     */
    private void prepareReferenceBean(String referencedBeanName, ReferenceBean referenceBean, boolean localServiceBean) {
        //  Issue : https://github.com/apache/dubbo/issues/6224
        if (localServiceBean) { // If the local @Service Bean exists
            // 被引用的Bean对象实例（ServiceBean）使用本项目中的Bean对象实例

            // referenceBean的injvm属性赋值为true
            referenceBean.setInjvm(Boolean.TRUE);
            exportServiceBeanIfNecessary(referencedBeanName); // If the referenced ServiceBean exits, export it immediately
        }
    }


    private void exportServiceBeanIfNecessary(String referencedBeanName) {
        // 再次判断ServiceBean是否存在
        if (existsServiceBean(referencedBeanName)) {
            ServiceBean serviceBean = getServiceBean(referencedBeanName);
            if (!serviceBean.isExported()) {
                // 触发ServiceBean的暴露逻辑
                serviceBean.export();
            }
        }
    }

    private ServiceBean getServiceBean(String referencedBeanName) {
        return applicationContext.getBean(referencedBeanName, ServiceBean.class);
    }

    @Override
    protected String buildInjectedObjectCacheKey(AnnotationAttributes attributes, Object bean, String beanName,
                                                 Class<?> injectedType, InjectionMetadata.InjectedElement injectedElement) {
        // 引用的bean名称
        return buildReferencedBeanName(attributes, injectedType) +
                // 引用方式
                "#source=" + (injectedElement.getMember()) +
                // 注解配置信息
                "#attributes=" + getAttributes(attributes, getEnvironment());
    }

    /**
     * @param attributes           the attributes of {@link Reference @Reference}
     * @param serviceInterfaceType the type of Dubbo's service interface
     * @return The name of bean that annotated Dubbo's {@link Service @Service} in local Spring {@link ApplicationContext}
     */
    private String buildReferencedBeanName(AnnotationAttributes attributes, Class<?> serviceInterfaceType) {
        ServiceBeanNameBuilder serviceBeanNameBuilder = create(attributes, serviceInterfaceType, getEnvironment());
        return serviceBeanNameBuilder.build();
    }

    private ReferenceBean buildReferenceBeanIfAbsent(String referenceBeanName, AnnotationAttributes attributes,
                                                     Class<?> referencedType)
            throws Exception {

        ReferenceBean<?> referenceBean = referenceBeanCache.get(referenceBeanName);

        if (referenceBean == null) {

            // 创建ReferenceBean对象建造器
            ReferenceBeanBuilder beanBuilder = ReferenceBeanBuilder
                    // attributes是@DubboReference注解的配置信息
                    .create(attributes, applicationContext)
                    // 远端服务接口类
                    .interfaceClass(referencedType)
                    // 引用对象的bean名称
                    .beanName(referenceBeanName);
            // 生成了一个ReferenceBean对象
            referenceBean = beanBuilder.build();
            referenceBeanCache.put(referenceBeanName, referenceBean);
        } else if (!referencedType.isAssignableFrom(referenceBean.getInterfaceClass())) {
            throw new IllegalArgumentException("reference bean name " + referenceBeanName + " has been duplicated, but interfaceClass " +
                    referenceBean.getInterfaceClass().getName() + " cannot be assigned to " + referencedType.getName());
        }
        return referenceBean;
    }

    private void cacheInjectedReferenceBean(ReferenceBean referenceBean,
                                            InjectionMetadata.InjectedElement injectedElement) {
        if (injectedElement.getMember() instanceof Field) {
            injectedFieldReferenceBeanCache.put(injectedElement, referenceBean);
        } else if (injectedElement.getMember() instanceof Method) {
            injectedMethodReferenceBeanCache.put(injectedElement, referenceBean);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void destroy() throws Exception {
        super.destroy();
        this.referenceBeanCache.clear();
        this.injectedFieldReferenceBeanCache.clear();
        this.injectedMethodReferenceBeanCache.clear();
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            referencedBeanNameIdx.entrySet().stream().filter(e -> e.getValue().size() > 1).forEach(e -> {
                String logPrefix = e.getKey() + " has " + e.getValue().size() + " reference instances, there are: ";
                logger.warn(e.getValue().stream().collect(Collectors.joining(", ", logPrefix, "")));
            });
            referencedBeanNameIdx.clear();
        }
    }
}

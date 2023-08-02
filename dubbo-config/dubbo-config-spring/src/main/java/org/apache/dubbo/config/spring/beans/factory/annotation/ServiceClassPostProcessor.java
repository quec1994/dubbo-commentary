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

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.dubbo.config.annotation.Method;
import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.spring.ServiceBean;
import org.apache.dubbo.config.spring.context.DubboBootstrapApplicationListener;
import org.apache.dubbo.config.spring.context.annotation.DubboClassPathBeanDefinitionScanner;
import org.apache.dubbo.config.spring.schema.AnnotationBeanDefinitionParser;
import org.apache.dubbo.config.spring.util.DubboAnnotationUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.AnnotationBeanNameGenerator;
import org.springframework.context.annotation.AnnotationConfigUtils;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.context.annotation.ConfigurationClassPostProcessor;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.alibaba.spring.util.BeanRegistrar.registerInfrastructureBean;
import static com.alibaba.spring.util.ObjectUtils.of;
import static java.util.Arrays.asList;
import static org.apache.dubbo.config.spring.beans.factory.annotation.ServiceBeanNameBuilder.create;
import static org.apache.dubbo.config.spring.util.DubboAnnotationUtils.resolveServiceInterfaceClass;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;
import static org.springframework.context.annotation.AnnotationConfigUtils.CONFIGURATION_BEAN_NAME_GENERATOR;
import static org.springframework.core.annotation.AnnotatedElementUtils.findMergedAnnotation;
import static org.springframework.core.annotation.AnnotationUtils.getAnnotationAttributes;
import static org.springframework.util.ClassUtils.resolveClassName;

/**
 * {@link BeanFactoryPostProcessor} used for processing of {@link DubboService @Service} annotated classes. it's also the
 * infrastructure class of XML {@link BeanDefinitionParser} on &lt;dubbo:annotation /&gt;
 *
 * @see AnnotationBeanDefinitionParser
 * @see BeanDefinitionRegistryPostProcessor
 * @since 2.7.7
 */
public class ServiceClassPostProcessor implements BeanDefinitionRegistryPostProcessor, EnvironmentAware,
        ResourceLoaderAware, BeanClassLoaderAware {

    private static final List<Class<? extends Annotation>> serviceAnnotationTypes = asList(
            // @since 2.7.7 Add the @DubboService , the issue : https://github.com/apache/dubbo/issues/6007
            DubboService.class,
            // @since 2.7.0 the substitute @com.alibaba.dubbo.config.annotation.Service
            Service.class,
            // @since 2.7.3 Add the compatibility for legacy Dubbo's @Service , the issue : https://github.com/apache/dubbo/issues/4330
            com.alibaba.dubbo.config.annotation.Service.class
    );

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final Set<String> packagesToScan;


    private Environment environment;

    private ResourceLoader resourceLoader;

    private ClassLoader classLoader;

    public ServiceClassPostProcessor(String... packagesToScan) {
        this(asList(packagesToScan));
    }

    public ServiceClassPostProcessor(Collection<String> packagesToScan) {
        this(new LinkedHashSet<>(packagesToScan));
    }

    public ServiceClassPostProcessor(Set<String> packagesToScan) {
        this.packagesToScan = packagesToScan;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {

        // @since 2.7.5
        // 注册DubboBootstrapApplicationListener监听器
        // 继承了OnceApplicationContextEventListener类，OnceApplicationContextEventListener实现了ApplicationListener接口，
        // 所以在Spring启动完后会调用OnceApplicationContextEventListener的onApplicationEvent方法，
        // 然后调用DubboBootstrapApplicationListener的onApplicationContextEvent方法，启动dubbo框架，进行服务暴露
        registerInfrastructureBean(registry, DubboBootstrapApplicationListener.BEAN_NAME, DubboBootstrapApplicationListener.class);

        // 根据环境变量对包路径进行占位符替换
        Set<String> resolvedPackagesToScan = resolvePackagesToScan(packagesToScan);

        if (!CollectionUtils.isEmpty(resolvedPackagesToScan)) {
            // 扫描包，进行Bean注册
            registerServiceBeans(resolvedPackagesToScan, registry);
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("packagesToScan is empty , ServiceBean registry will be ignored!");
            }
        }

    }

    /**
     * Registers Beans whose classes was annotated {@link DubboService}
     *
     * @param packagesToScan The base packages to scan
     * @param registry       {@link BeanDefinitionRegistry}
     */
    private void registerServiceBeans(Set<String> packagesToScan, BeanDefinitionRegistry registry) {

        DubboClassPathBeanDefinitionScanner scanner =
                new DubboClassPathBeanDefinitionScanner(registry, environment, resourceLoader);

        // 获得spring的beanName创造器
        BeanNameGenerator beanNameGenerator = resolveBeanNameGenerator(registry);

        scanner.setBeanNameGenerator(beanNameGenerator);

        // refactor @since 2.7.7
        serviceAnnotationTypes.forEach(annotationType -> {
            // 添加要扫描的DubboService（@DubboService、@Service）注解
            scanner.addIncludeFilter(new AnnotationTypeFilter(annotationType));
        });

        for (String packageToScan : packagesToScan) {

            // Registers @Service Bean first
            // 在之前得到包路径下扫描被@DubboService注解标注的类
            scanner.scan(packageToScan);

            // Finds all BeanDefinitionHolders of @Service whether @ComponentScan scans or not.
            // 查找被@DubboService注解的类的BeanDefinition（无论这个类有没有被@ComponentScan注解标注了）
            Set<BeanDefinitionHolder> beanDefinitionHolders =
                    findServiceBeanDefinitionHolders(scanner, packageToScan, registry, beanNameGenerator);

            if (!CollectionUtils.isEmpty(beanDefinitionHolders)) {

                // 得到BeanDefinition开始处理它
                for (BeanDefinitionHolder beanDefinitionHolder : beanDefinitionHolders) {
                    registerServiceBean(beanDefinitionHolder, registry, scanner);
                }

                if (logger.isInfoEnabled()) {
                    logger.info(beanDefinitionHolders.size() + " annotated Dubbo's @DubboService Components { " +
                            beanDefinitionHolders +
                            " } were scanned under package[" + packageToScan + "]");
                }

            } else {

                if (logger.isWarnEnabled()) {
                    logger.warn("No Spring Bean annotating Dubbo's @DubboService was found under package["
                            + packageToScan + "]");
                }

            }

        }

    }

    /**
     * It'd better to use BeanNameGenerator instance that should reference
     * {@link ConfigurationClassPostProcessor#componentScanBeanNameGenerator},
     * thus it maybe a potential problem on bean name generation.
     *
     * @param registry {@link BeanDefinitionRegistry}
     * @return {@link BeanNameGenerator} instance
     * @see SingletonBeanRegistry
     * @see AnnotationConfigUtils#CONFIGURATION_BEAN_NAME_GENERATOR
     * @see ConfigurationClassPostProcessor#processConfigBeanDefinitions
     * @since 2.5.8
     */
    private BeanNameGenerator resolveBeanNameGenerator(BeanDefinitionRegistry registry) {

        BeanNameGenerator beanNameGenerator = null;

        if (registry instanceof SingletonBeanRegistry) {
            SingletonBeanRegistry singletonBeanRegistry = SingletonBeanRegistry.class.cast(registry);
            beanNameGenerator = (BeanNameGenerator) singletonBeanRegistry.getSingleton(CONFIGURATION_BEAN_NAME_GENERATOR);
        }

        if (beanNameGenerator == null) {

            if (logger.isInfoEnabled()) {

                logger.info("BeanNameGenerator bean can't be found in BeanFactory with name ["
                        + CONFIGURATION_BEAN_NAME_GENERATOR + "]");
                logger.info("BeanNameGenerator will be a instance of " +
                        AnnotationBeanNameGenerator.class.getName() +
                        " , it maybe a potential problem on bean name generation.");
            }

            beanNameGenerator = new AnnotationBeanNameGenerator();

        }

        return beanNameGenerator;

    }

    /**
     * Finds a {@link Set} of {@link BeanDefinitionHolder BeanDefinitionHolders} whose bean type annotated
     * {@link DubboService} Annotation.
     *
     * @param scanner       {@link ClassPathBeanDefinitionScanner}
     * @param packageToScan package to scan
     * @param registry      {@link BeanDefinitionRegistry}
     * @return non-null
     * @since 2.5.8
     */
    private Set<BeanDefinitionHolder> findServiceBeanDefinitionHolders(
            ClassPathBeanDefinitionScanner scanner, String packageToScan, BeanDefinitionRegistry registry,
            BeanNameGenerator beanNameGenerator) {
        // 取出路径下扫描到的被@DubboService注解修饰的类的BeanDefinition
        Set<BeanDefinition> beanDefinitions = scanner.findCandidateComponents(packageToScan);

        Set<BeanDefinitionHolder> beanDefinitionHolders = new LinkedHashSet<>(beanDefinitions.size());

        for (BeanDefinition beanDefinition : beanDefinitions) {
            // 通过spring的bean名称生成器生成一个服务实现类的BeanName
            String beanName = beanNameGenerator.generateBeanName(beanDefinition, registry);
            // 组装成BeanDefinitionHolder
            BeanDefinitionHolder beanDefinitionHolder = new BeanDefinitionHolder(beanDefinition, beanName);
            beanDefinitionHolders.add(beanDefinitionHolder);

        }

        return beanDefinitionHolders;

    }

    /**
     * Registers {@link ServiceBean} from new annotated {@link DubboService} {@link BeanDefinition}
     *
     * @param beanDefinitionHolder
     * @param registry
     * @param scanner
     * @see ServiceBean
     * @see BeanDefinition
     */
    private void registerServiceBean(BeanDefinitionHolder beanDefinitionHolder, BeanDefinitionRegistry registry,
                                     DubboClassPathBeanDefinitionScanner scanner) {

        // 服务实现类
        Class<?> beanClass = resolveClass(beanDefinitionHolder);

        // @DubboService注解配置
        Annotation service = findServiceAnnotation(beanClass);

        /**
         * The {@link AnnotationAttributes} of @DubboService annotation
         */
        // @DubboService注解上配置的属性信息
        AnnotationAttributes serviceAnnotationAttributes = getAnnotationAttributes(service, false, false);

        // 服务实现类对应的接口
        Class<?> interfaceClass = resolveServiceInterfaceClass(serviceAnnotationAttributes, beanClass);
        // 服务实现类对应的bean的名字，比如：demoServiceImpl
        String annotatedServiceBeanName = beanDefinitionHolder.getBeanName();
        // ServiceBean:org.apache.dubbo.demo.DemoService

        // 构造一个ServiceBean的BeanDefinition
        AbstractBeanDefinition serviceBeanDefinition =
                buildServiceBeanDefinition(service, serviceAnnotationAttributes, interfaceClass, annotatedServiceBeanName);

        // ServiceBean Bean name
        // 构根据服务的接口类和注解的配置构造一个ServiceBean的BeanName
        String beanName = generateServiceBeanName(serviceAnnotationAttributes, interfaceClass);
        // ServiceBean:org.apache.dubbo.demo.DemoService

        // 判断serviceBeanDefinition是不是已经注册过了
        if (scanner.checkCandidate(beanName, serviceBeanDefinition)) { // check duplicated candidate bean
            // 注册serviceBeanDefinition，好让spring生成并管理serviceBean
            registry.registerBeanDefinition(beanName, serviceBeanDefinition);

            // 如果没有配置id，将构造好的beanName赋值给id
            if (!serviceBeanDefinition.getPropertyValues().contains("id")) {
                serviceBeanDefinition.getPropertyValues().addPropertyValue("id", beanName);
            }

            if (logger.isInfoEnabled()) {
                logger.info("The BeanDefinition[" + serviceBeanDefinition +
                        "] of ServiceBean has been registered with name : " + beanName);
            }

        } else {

            if (logger.isWarnEnabled()) {
                logger.warn("The Duplicated BeanDefinition[" + serviceBeanDefinition +
                        "] of ServiceBean[ bean name : " + beanName +
                        "] was be found , Did @DubboComponentScan scan to same package in many times?");
            }

        }

    }

    /**
     * Find the {@link Annotation annotation} of @Service
     *
     * @param beanClass the {@link Class class} of Bean
     * @return <code>null</code> if not found
     * @since 2.7.3
     */
    private Annotation findServiceAnnotation(Class<?> beanClass) {
        return serviceAnnotationTypes
                .stream()
                .map(annotationType -> findMergedAnnotation(beanClass, annotationType))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    /**
     * Generates the bean name of {@link ServiceBean}
     *
     * @param serviceAnnotationAttributes
     * @param interfaceClass              the class of interface annotated {@link DubboService}
     * @return ServiceBean@interfaceClassName#annotatedServiceBeanName
     * @since 2.7.3
     */
    private String generateServiceBeanName(AnnotationAttributes serviceAnnotationAttributes, Class<?> interfaceClass) {
        ServiceBeanNameBuilder builder = create(interfaceClass, environment)
                .group(serviceAnnotationAttributes.getString("group"))
                .version(serviceAnnotationAttributes.getString("version"));
        return builder.build();
    }

    private Class<?> resolveClass(BeanDefinitionHolder beanDefinitionHolder) {

        BeanDefinition beanDefinition = beanDefinitionHolder.getBeanDefinition();

        return resolveClass(beanDefinition);

    }

    private Class<?> resolveClass(BeanDefinition beanDefinition) {

        String beanClassName = beanDefinition.getBeanClassName();

        return resolveClassName(beanClassName, classLoader);

    }

    private Set<String> resolvePackagesToScan(Set<String> packagesToScan) {
        Set<String> resolvedPackagesToScan = new LinkedHashSet<String>(packagesToScan.size());
        for (String packageToScan : packagesToScan) {
            if (StringUtils.hasText(packageToScan)) {
                // 对包路径进行占位符替换
                String resolvedPackageToScan = environment.resolvePlaceholders(packageToScan.trim());
                resolvedPackagesToScan.add(resolvedPackageToScan);
            }
        }
        return resolvedPackagesToScan;
    }

    /**
     * Build the {@link AbstractBeanDefinition Bean Definition}
     *
     * @param serviceAnnotation
     * @param serviceAnnotationAttributes
     * @param interfaceClass
     * @param annotatedServiceBeanName
     * @return
     * @since 2.7.3
     */
    private AbstractBeanDefinition buildServiceBeanDefinition(Annotation serviceAnnotation,
                                                              AnnotationAttributes serviceAnnotationAttributes,
                                                              Class<?> interfaceClass,
                                                              String annotatedServiceBeanName) {
        // 生成一个ServiceBean对应的BeanDefinition
        BeanDefinitionBuilder builder = rootBeanDefinition(ServiceBean.class);

        AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();

        MutablePropertyValues propertyValues = beanDefinition.getPropertyValues();

        // 这些bean类型的配置属性，在转存基本数据类型的配置属性时排除，后面单独转存
        String[] ignoreAttributeNames = of("provider", "monitor", "application", "module", "registry", "protocol",
                "interface", "interfaceName", "parameters");

        // 先转存基本数据类型的配置属性
        // 把serviceAnnotation中的参数值赋值给ServiceBean的属性
        propertyValues.addPropertyValues(new AnnotationPropertyValuesAdapter(serviceAnnotation, environment, ignoreAttributeNames));

        // 再一个一个转存bean类型的属性
        // References "ref" property to annotated-@Service Bean
        // ref属性赋值为另外一个bean, 对应的就是被@DubboService注解的服务实现类对应的bean
        addPropertyReference(builder, "ref", annotatedServiceBeanName);
        // Set interface
        // 服务接口类名赋值给ServiceBean的interface属性
        builder.addPropertyValue("interface", interfaceClass.getName());
        // Convert parameters into map
        // @DubboService注解上配置的parameters赋值给ServiceBean的parameters属性
        builder.addPropertyValue("parameters", DubboAnnotationUtils.convertParameters(serviceAnnotationAttributes.getStringArray("parameters")));
        // Add methods parameters
        // 将@DubboService注解上配置的methods转换成MethodConfig
        List<MethodConfig> methodConfigs = convertMethodConfigs(serviceAnnotationAttributes.get("methods"));
        if (!methodConfigs.isEmpty()) {
            // 配置了methods属性，则给ServiceBean对应的methods属性赋值
            builder.addPropertyValue("methods", methodConfigs);
        }

        /**
         * Add {@link org.apache.dubbo.config.ProviderConfig} Bean reference
         */
        String providerConfigBeanName = serviceAnnotationAttributes.getString("provider");
        if (StringUtils.hasText(providerConfigBeanName)) {
            // 配置了provider属性，则给ServiceBean对应的provider属性赋指定的ProviderConfig bean
            addPropertyReference(builder, "provider", providerConfigBeanName);
        }

        /**
         * Add {@link org.apache.dubbo.config.MonitorConfig} Bean reference
         */
        String monitorConfigBeanName = serviceAnnotationAttributes.getString("monitor");
        if (StringUtils.hasText(monitorConfigBeanName)) {
            // 配置了monitor属性，则给ServiceBean对应的provider属性赋指定的MonitorConfig bean
            addPropertyReference(builder, "monitor", monitorConfigBeanName);
        }

        /**
         * Add {@link org.apache.dubbo.config.ApplicationConfig} Bean reference
         */
        String applicationConfigBeanName = serviceAnnotationAttributes.getString("application");
        if (StringUtils.hasText(applicationConfigBeanName)) {
            // 配置了application属性，则给ServiceBean对应的application属性赋指定的ApplicationConfig bean
            addPropertyReference(builder, "application", applicationConfigBeanName);
        }

        /**
         * Add {@link org.apache.dubbo.config.ModuleConfig} Bean reference
         */
        String moduleConfigBeanName = serviceAnnotationAttributes.getString("module");
        if (StringUtils.hasText(moduleConfigBeanName)) {
            // 配置了module属性，则给ServiceBean对应的module属性赋指定的ModuleConfig bean
            addPropertyReference(builder, "module", moduleConfigBeanName);
        }


        /**
         * Add {@link org.apache.dubbo.config.RegistryConfig} Bean reference
         */
        String[] registryConfigBeanNames = serviceAnnotationAttributes.getStringArray("registry");

        // 将配置的registry属性值转换成spring 的 RuntimeBeanReference，后期spring会自动转换
        List<RuntimeBeanReference> registryRuntimeBeanReferences = toRuntimeBeanReferences(registryConfigBeanNames);

        if (!registryRuntimeBeanReferences.isEmpty()) {
            // 配置了registry属性，则给ServiceBean对应的registries属性赋指定的RegistryConfig bean集合
            builder.addPropertyValue("registries", registryRuntimeBeanReferences);
        }

        /**
         * Add {@link org.apache.dubbo.config.ProtocolConfig} Bean reference
         */
        String[] protocolConfigBeanNames = serviceAnnotationAttributes.getStringArray("protocol");

        // 将配置的protocol属性值转换成spring 的 RuntimeBeanReference，后期spring会自动转换
        List<RuntimeBeanReference> protocolRuntimeBeanReferences = toRuntimeBeanReferences(protocolConfigBeanNames);

        if (!protocolRuntimeBeanReferences.isEmpty()) {
            // 配置了protocol属性，则给ServiceBean对应的protocols属性赋指定的ProtocolConfig bean集合
            builder.addPropertyValue("protocols", protocolRuntimeBeanReferences);
        }

        return builder.getBeanDefinition();

    }

    private List convertMethodConfigs(Object methodsAnnotation) {
        if (methodsAnnotation == null) {
            return Collections.EMPTY_LIST;
        }
        return MethodConfig.constructMethodConfig((Method[]) methodsAnnotation);
    }

    private ManagedList<RuntimeBeanReference> toRuntimeBeanReferences(String... beanNames) {

        ManagedList<RuntimeBeanReference> runtimeBeanReferences = new ManagedList<>();

        if (!ObjectUtils.isEmpty(beanNames)) {

            for (String beanName : beanNames) {

                String resolvedBeanName = environment.resolvePlaceholders(beanName);

                runtimeBeanReferences.add(new RuntimeBeanReference(resolvedBeanName));
            }

        }

        return runtimeBeanReferences;

    }

    private void addPropertyReference(BeanDefinitionBuilder builder, String propertyName, String beanName) {
        // 解析beanName上的占位符
        String resolvedBeanName = environment.resolvePlaceholders(beanName);
        // 给builder添加引用类型的属性，后期spring会把resolvedBeanName对应的bean赋值给propertyName
        builder.addPropertyReference(propertyName, resolvedBeanName);
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }
}

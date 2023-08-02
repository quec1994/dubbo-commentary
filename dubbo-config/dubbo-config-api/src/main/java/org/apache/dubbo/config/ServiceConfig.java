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
package org.apache.dubbo.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.config.event.ServiceConfigExportedEvent;
import org.apache.dubbo.config.event.ServiceConfigUnexportedEvent;
import org.apache.dubbo.config.invoker.DelegateProviderMetaDataInvoker;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.config.utils.ConfigValidationUtils;
import org.apache.dubbo.event.Event;
import org.apache.dubbo.event.EventDispatcher;
import org.apache.dubbo.metadata.ServiceNameMapping;
import org.apache.dubbo.registry.client.metadata.MetadataUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.ConfiguratorFactory;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.REGISTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.*;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.SERVICE_REGISTRY_PROTOCOL;
import static org.apache.dubbo.common.utils.NetUtils.*;
import static org.apache.dubbo.config.Constants.*;
import static org.apache.dubbo.remoting.Constants.BIND_IP_KEY;
import static org.apache.dubbo.remoting.Constants.BIND_PORT_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.*;
import static org.apache.dubbo.rpc.cluster.Constants.EXPORT_KEY;

public class ServiceConfig<T> extends ServiceConfigBase<T> {

    private static final long serialVersionUID = -412700146501624375L;

    public static final Logger logger = LoggerFactory.getLogger(ServiceConfig.class);

    /**
     * A random port cache, the different protocols who has no port specified have different random port
     */
    private static final Map<String, Integer> RANDOM_PORT_MAP = new HashMap<String, Integer>();

    /**
     * A delayed exposure service timer
     */
    private static final ScheduledExecutorService DELAY_EXPORT_EXECUTOR =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("DubboServiceDelayExporter", true));

    private String serviceName;

    private static final Protocol PROTOCOL = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

    /**
     * A {@link ProxyFactory} implementation that will generate a exported service proxy,the JavassistProxyFactory is its
     * default implementation
     */
    private static final ProxyFactory PROXY_FACTORY = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    /**
     * Whether the provider has been exported
     */
    private transient volatile boolean exported;

    /**
     * The flag whether a service has unexported ,if the method unexported is invoked, the value is true
     */
    private transient volatile boolean unexported;

    private DubboBootstrap bootstrap;

    /**
     * The exported services
     */
    private final List<Exporter<?>> exporters = new ArrayList<Exporter<?>>();

    private static final String CONFIG_INITIALIZER_PROTOCOL = "configInitializer://";

    private static final String TRUE_VALUE = "true";

    private static final String FALSE_VALUE = "false";

    private static final String STUB_SUFFIX = "Stub";

    private static final String LOCAL_SUFFIX = "Local";

    private static final String CONFIG_POST_PROCESSOR_PROTOCOL = "configPostProcessor://";

    private static final String RETRY_SUFFIX = ".retry";

    private static final String RETRIES_SUFFIX = ".retries";

    private static final String ZERO_VALUE = "0";

    public ServiceConfig() {
    }

    public ServiceConfig(Service service) {
        super(service);
    }

    @Parameter(excluded = true)
    @Override
    public boolean isExported() {
        return exported;
    }

    @Parameter(excluded = true)
    @Override
    public boolean isUnexported() {
        return unexported;
    }

    @Override
    public void unexport() {
        if (!exported) {
            return;
        }
        if (unexported) {
            return;
        }
        if (!exporters.isEmpty()) {
            for (Exporter<?> exporter : exporters) {
                try {
                    exporter.unexport();
                } catch (Throwable t) {
                    logger.warn("Unexpected error occurred when unexport " + exporter, t);
                }
            }
            exporters.clear();
        }
        unexported = true;

        // dispatch a ServiceConfigUnExportedEvent since 2.7.4
        dispatch(new ServiceConfigUnexportedEvent(this));
    }

    @Override
    public synchronized void export() {
        if (bootstrap == null) {
            bootstrap = DubboBootstrap.getInstance();
            // compatible with api call.
            if (null != this.getRegistry()) {
                bootstrap.registries(this.getRegistries());
            }
            bootstrap.initialize();
        }

        checkAndUpdateSubConfigs();

        initServiceMetadata(provider);
        serviceMetadata.setServiceType(getInterfaceClass());
        serviceMetadata.setTarget(getRef());
        serviceMetadata.generateServiceKey();

        // 检查服务是否需要暴露
        if (!shouldExport()) {
            return;
        }

        // 检查是否需要延迟暴露
        if (shouldDelay()) {
            DELAY_EXPORT_EXECUTOR.schedule(() -> {
                try {
                    // Delay export server should print stack trace if there are exception occur.
                    this.doExport();
                } catch (Exception e) {
                    logger.error("delay export server occur exception, please check it.", e);
                }
            }, getDelay(), TimeUnit.MILLISECONDS);
        } else {
            // 暴露服务
            doExport();
        }
        // ServiceBean 覆盖实现了 exported() 方法，因此此处调的是 ServiceBean.exported() 方法执行暴露完成之后的操作，
        exported();
    }

    public void exported() {
        List<URL> exportedURLs = this.getExportedUrls();
        exportedURLs.forEach(url -> {
            // dubbo2.7.x does not register serviceNameMapping information with the registry by default.
            // Only when the user manually turns on the service introspection, can he register with the registration center.
            if (url.getParameters().containsKey(SERVICE_NAME_MAPPING_KEY)) {
                Map<String, String> parameters = getApplication().getParameters();
                ServiceNameMapping.getExtension(parameters != null ? parameters.get(MAPPING_KEY) : null).map(url);
            }
        });
        // dispatch a ServiceConfigExportedEvent since 2.7.4
        // 发布一个 Dubbo 事件 ServiceConfigExportedEvent
        dispatch(new ServiceConfigExportedEvent(this));
    }

    private void checkAndUpdateSubConfigs() {
        // Use default configs defined explicitly with global scope
        // ServiceConfig中的某些属性如果是空的，那么就从服务里配置的 ProviderConfig、ModuleConfig、ApplicationConfig 中获取
        // 补全ServiceConfig中的属性
        completeCompoundConfigs();
        // 从启动配置里取provider的配置，或者创建一个默认的
        checkDefault();
        // 如果服务和provider没有配置协议，取配置的默认协议集合或者protocolIds
        checkProtocol();
        // 执行自定义的配置初始化逻辑添加属性
        // init some null configuration.
        List<ConfigInitializer> configInitializers = ExtensionLoader.getExtensionLoader(ConfigInitializer.class)
                .getActivateExtension(URL.valueOf(CONFIG_INITIALIZER_PROTOCOL), (String[]) null);
        configInitializers.forEach(e -> e.initServiceConfig(this));

        // if protocol is not injvm checkRegistry
        // 如果protocol不是只有injvm协议，表示服务调用不是只在本机jvm里面调用，那就需要用到注册中心
        if (!isOnlyInJvm()) {
            checkRegistry();
        }
        // 刷新ServiceConfig
        this.refresh();

        if (StringUtils.isEmpty(interfaceName)) {
            throw new IllegalStateException("<dubbo:service interface=\"\" /> interface not allow null!");
        }

        // 当前服务对应的实现类是一个GenericService，表示没有特定的接口
        if (ref instanceof GenericService) {
            interfaceClass = GenericService.class;
            if (StringUtils.isEmpty(generic)) {
                generic = TRUE_VALUE;
            }
        } else {
            // 加载接口
            try {
                interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
                        .getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            // 刷新MethodConfig，并判断MethodConfig中对应的方法在接口中是否存在
            checkInterfaceAndMethods(interfaceClass, getMethods());
            // 实现类是不是该接口类型
            checkRef();
            generic = FALSE_VALUE;
        }

        // 检查local和stub
        checkStubAndLocal(interfaceClass);
        // 检查mock
        ConfigValidationUtils.checkMock(interfaceClass, this);
        // 检查ServiceConfig
        ConfigValidationUtils.validateServiceConfig(this);
        postProcessConfig();
    }


    protected synchronized void doExport() {
        if (unexported) {
            throw new IllegalStateException("The service " + interfaceClass.getName() + " has already unexported!");
        }
        // 已经暴露了，就不再暴露了
        if (exported) {
            return;
        }
        exported = true;

        if (StringUtils.isEmpty(path)) {
            path = interfaceName;
        }
        doExportUrls();
        bootstrap.setReady(true);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void doExportUrls() {
        // ServiceRepository表示应用中有哪些服务提供者和引用了哪些服务
        ServiceRepository repository = ApplicationModel.getServiceRepository();
        // ServiceDescriptor中存在服务提供者访问路径，实现类，接口，以及接口中的各个方法对应的MethodDescriptor
        // MethodDescriptor表示某一个方法，方法名，所属的服务
        ServiceDescriptor serviceDescriptor = repository.registerService(getInterfaceClass());
        repository.registerProvider(
                getUniqueServiceName(),
                ref,
                serviceDescriptor,
                this,
                serviceMetadata
        );

        // 一个 registryURL 表示一个注册中心
        List<URL> registryURLs = ConfigValidationUtils.loadRegistries(this, true);

        int protocolConfigNum = protocols.size();
        for (ProtocolConfig protocolConfig : protocols) {
            // pathKey = group/contextpath/path:version
            // 例子：myGroup/user/org.apache.dubbo.demo.DemoService:1.0.1
            String pathKey = URL.buildKey(getContextPath(protocolConfig)
                    .map(p -> p + "/" + path)
                    .orElse(path), group, version);
            // In case user specified path, register service one more time to map it to path.
            // 再根据路径注册一次服务以将其映射到路径
            repository.registerService(pathKey, interfaceClass);
            // 重点，将单个协议注册到每个注册中心上去
            doExportUrlsFor1Protocol(protocolConfig, registryURLs, protocolConfigNum);
        }
    }

    private void doExportUrlsFor1Protocol(ProtocolConfig protocolConfig, List<URL> registryURLs, int protocolConfigNum) {
        // protocolConfig表示某个协议，registryURLs表示所有的注册中心，protocolConfigNum表示这个服务有多少个协议

        // 如果配置的某个协议，没有配置name，那么默认为dubbo
        String name = protocolConfig.getName();
        if (StringUtils.isEmpty(name)) {
            name = DUBBO;
        }

        // 这个map表示服务url的参数
        Map<String, String> map = new HashMap<String, String>();
        map.put(SIDE_KEY, PROVIDER_SIDE);

        // 把dubbo的版本信息和pid放入map中
        ServiceConfig.appendRuntimeParameters(map);

        /* 对map里的值做继承和覆盖操作，获取优先级最高最全的参数值 */
        // 使用指标参数覆盖参数值，如果没有配置则取默认的指标
        AbstractConfig.appendParameters(map, getMetrics());
        // 使用应用相关参数覆盖参数值，如果没有配置则取默认的应用
        AbstractConfig.appendParameters(map, getApplication());
        // 使用模块相关参数覆盖参数值，如果没有配置则取默认的模块
        AbstractConfig.appendParameters(map, getModule());
        // remove 'default.' prefix for configs from ProviderConfig
        // appendParameters(map, provider, Constants.DEFAULT_KEY);
        // 使用提供者相关参数覆盖参数值
        AbstractConfig.appendParameters(map, provider);
        // 使用协议相关参数覆盖参数值
        AbstractConfig.appendParameters(map, protocolConfig);
        // 使用服务本身相关参数覆盖参数值
        AbstractConfig.appendParameters(map, this);

        MetadataReportConfig metadataReportConfig = getMetadataReportConfig();
        if (metadataReportConfig != null && metadataReportConfig.isValid()) {
            // 向元数据中心报告
            map.putIfAbsent(METADATA_KEY, REMOTE_METADATA_STORAGE_TYPE);
        }
        // 服务中某些方法参数
        if (CollectionUtils.isNotEmpty(getMethods())) {
            for (MethodConfig method : getMethods()) {
                // 某个方法的配置参数，注意有prefix
                AbstractConfig.appendParameters(map, method, method.getName());
                String retryKey = method.getName() + RETRY_SUFFIX;
                // 如果某个方法配置存在xx.retry=false，则改成xx.retry=0
                if (map.containsKey(retryKey)) {
                    String retryValue = map.remove(retryKey);
                    if (FALSE_VALUE.equals(retryValue)) {
                        map.put(method.getName() + RETRIES_SUFFIX, ZERO_VALUE);
                    }
                }
                List<ArgumentConfig> arguments = method.getArguments();
                if (CollectionUtils.isNotEmpty(arguments)) {
                    // 遍历当前方法配置中的参数配置
                    for (ArgumentConfig argument : arguments) {
                        /*
                         如果配置了type，则遍历当前接口的所有方法，然后找到方法名和当前方法名相等的方法，可能存在多个
                         如果配置了index,则看index对应位置的参数类型是否等于type,如果相等，则向map中存入argument对象中的属性
                         如果没有配置index，那么则遍历方法所有的参数类型，等于type则向map中存入argument对象中的属性
                         如果没有配置type,但配置了index,则把对应位置的argument放入map
                         */
                        // convert argument type
                        // 有没有配置type
                        if (argument.getType() != null && argument.getType().length() > 0) {
                            // 配置了type
                            Method[] methods = interfaceClass.getMethods();
                            // visit all methods
                            if (methods.length > 0) {
                                for (int i = 0; i < methods.length; i++) {
                                    String methodName = methods[i].getName();
                                    // target the method, and get its signature
                                    // 是不是配置的方法
                                    if (methodName.equals(method.getName())) {
                                        Class<?>[] argtypes = methods[i].getParameterTypes();
                                        // one callback in the method
                                        if (argument.getIndex() != -1) {
                                            // 配置了index

                                            // 对应位置的参数类型是不是跟type相同
                                            if (argtypes[argument.getIndex()].getName().equals(argument.getType())) {
                                                // 向map中存入argument对象中的属性
                                                AbstractConfig
                                                        .appendParameters(map, argument, method.getName() + "." + argument.getIndex());
                                            } else {
                                                throw new IllegalArgumentException(
                                                        "Argument config error : the index attribute and type attribute not match :index :" +
                                                                argument.getIndex() + ", type:" + argument.getType());
                                            }
                                        } else {
                                            // 没有配置index

                                            // 遍历方法参数类型
                                            // multiple callbacks in the method
                                            for (int j = 0; j < argtypes.length; j++) {
                                                Class<?> argclazz = argtypes[j];
                                                // 参数类型是不是跟type相同
                                                if (argclazz.getName().equals(argument.getType())) {
                                                    // 向map中存入argument对象中的属性
                                                    AbstractConfig.appendParameters(map, argument, method.getName() + "." + j);
                                                    if (argument.getIndex() != -1 && argument.getIndex() != j) {
                                                        throw new IllegalArgumentException(
                                                                "Argument config error : the index attribute and type attribute not match :index :" +
                                                                        argument.getIndex() + ", type:" + argument.getType());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else if (argument.getIndex() != -1) {
                            // 没有配置type，但配置了index
                            // 向map中存入argument对象中的属性
                            AbstractConfig.appendParameters(map, argument, method.getName() + "." + argument.getIndex());
                        } else {
                            throw new IllegalArgumentException(
                                    "Argument config must set index or type attribute.eg: <dubbo:argument index='0' .../> or <dubbo:argument type=xxx .../>");
                        }

                    }
                }
            } // end of methods for
        }

        if (ProtocolUtils.isGeneric(generic)) {
            // 泛化服务

            map.put(GENERIC_KEY, generic);
            map.put(METHODS_KEY, ANY_VALUE);
        } else {
            // 非泛化服务

            String revision = Version.getVersion(interfaceClass, version);
            if (revision != null && revision.length() > 0) {
                map.put(REVISION_KEY, revision);
            }

            // 通过接口对应的Wrapper，拿到接口中所有的方法名字
            String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();
            if (methods.length == 0) {
                logger.warn("No method found in service interface " + interfaceClass.getName());
                // 接口中没有方法则放入 * 号
                map.put(METHODS_KEY, ANY_VALUE);
            } else {
                // 将接口中方法名放入
                map.put(METHODS_KEY, StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
            }
        }

        /**
         * Here the token value configured by the provider is used to assign the value to ServiceConfig#token
         */
        if (ConfigUtils.isEmpty(token) && provider != null) {
            token = provider.getToken();
        }

        // Token是为了防止服务被消费者直接调用（伪造http请求）
        if (!ConfigUtils.isEmpty(token)) {
            if (ConfigUtils.isDefault(token)) {
                map.put(TOKEN_KEY, UUID.randomUUID().toString());
            } else {
                map.put(TOKEN_KEY, token);
            }
        }
        //init serviceMetadata attachments
        serviceMetadata.getAttachments().putAll(map);

        // export service
        // 通过该host和port访问该服务
        String host = findConfigedHosts(protocolConfig, registryURLs, map);
        Integer port = findConfigedPorts(protocolConfig, name, map, protocolConfigNum);
        // 服务url
        URL url = new URL(name, host, port, getContextPath(protocolConfig).map(p -> p + "/" + path).orElse(path), map);
        // url：dubbo://192.168.56.1:20880/org.apache.dubbo.demo.DemoService?anyhost=true&application=dubbo-demo-annotation-provider&bind.ip=192.168.56.1&bind.port=20880&deprecated=false&dubbo=2.0.2&dynamic=true&generic=false&interface=org.apache.dubbo.demo.DemoService&methods=sayHello,sayHelloAsync&pid=2636&release=&service.name=ServiceBean:/org.apache.dubbo.demo.DemoService&side=provider&timestamp=1690360902105

        // You can customize Configurator to append extra parameters
        // 可以通过ConfiguratorFactory，对服务url再次进行配置
        if (ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class)
                .hasExtension(url.getProtocol())) {
            url = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class)
                    .getExtension(url.getProtocol()).getConfigurator(url).configure(url);
        }

        // scope 的值只能为 null、remote、local、none，没有配置的时候为null，配置其他的值等同于null
        String scope = url.getParameter(SCOPE_KEY);
        // don't export when none is configured
        // 如果scope为none,则不会进行任何的服务暴露，既不会远程，也不会本地
        if (!SCOPE_NONE.equalsIgnoreCase(scope)) {

            // export to local if the config is not remote (export to remote only when config is remote)
            if (!SCOPE_REMOTE.equalsIgnoreCase(scope)) {
                // 如果scope不是remote，scope为null、local，则会进行本地暴露，会根据当前url新建1个url并将新url的protocol改为injvm，然后进行暴露
                exportLocal(url);
            }
            // export to remote if the config is not local (export to local only when config is local)
            if (!SCOPE_LOCAL.equalsIgnoreCase(scope)) {
                // 如果scope不是local，scope为null、remote，则会进行远程暴露
                if (CollectionUtils.isNotEmpty(registryURLs)) {
                    /* 如果有注册中心，则将服务注册到注册中心 */

                    for (URL registryURL : registryURLs) {
                        if (SERVICE_REGISTRY_PROTOCOL.equals(registryURL.getProtocol())) {
                            url = url.addParameterIfAbsent(SERVICE_NAME_MAPPING_KEY, "true");
                        }

                        //if protocol is only injvm ,not register
                        // 如果是injvm，则不需要进行注册中心注册
                        if (LOCAL_PROTOCOL.equalsIgnoreCase(url.getProtocol())) {
                            continue;
                        }
                        // 该服务是否是动态，对应zookeeper上表示是否是临时节点，对应dubbo中的功能就是静态服务
                        // provider、service 的 dynamic 属性默认为true，如果需要禁用，要手动显示配置禁用
                        url = url.addParameterIfAbsent(DYNAMIC_KEY, registryURL.getParameter(DYNAMIC_KEY));

                        // 拿到监控中心地址
                        URL monitorUrl = ConfigValidationUtils.loadMonitor(this, registryURL);
                        if (monitorUrl != null) {
                            // 当前服务连接的是哪个监控中心
                            url = url.addParameterAndEncoded(MONITOR_KEY, monitorUrl.toFullString());
                        }
                        // 服务的register参数，如果为true，则表示要注册到注册中心
                        if (logger.isInfoEnabled()) {
                            if (url.getParameter(REGISTER_KEY, true)) {
                                logger.info("Register dubbo service " + interfaceClass.getName() + " url " + url + " to registry " +
                                        registryURL);
                            } else {
                                logger.info("Export dubbo service " + interfaceClass.getName() + " to url " + url);
                            }
                        }

                        // For providers, this is used to enable custom proxy to generate invoker
                        // 服务使用的动态代理机制，如果为空则使用javassit
                        String proxy = url.getParameter(PROXY_KEY);
                        if (StringUtils.isNotEmpty(proxy)) {
                            registryURL = registryURL.addParameter(PROXY_KEY, proxy);
                        }

                        // 生成一个当前服务接口的代理对象
                        // 使用动态代理生成一个Invoker，Invoker表示服务提供者的代理，可以使用Invoker的invoke方法执行服务
                        // 对应的url为 registry://127.0.0.1:2181/org.apache.dubbo.registry.RegistryService?application=dubbo-demo-annotation-provider&dubbo=2.0.2&export=dubbo%3A%2F%2F192.168.56.1%3A20880%2Forg.apache.dubbo.demo.DemoService%3Fanyhost%3Dtrue%26application%3Ddubbo-demo-annotation-provider%26bind.ip%3D192.168.56.1%26bind.port%3D20880%26deprecated%3Dfalse%26dubbo%3D2.0.2%26dynamic%3Dtrue%26generic%3Dfalse%26interface%3Dorg.apache.dubbo.demo.DemoService%26methods%3DsayHello%2CsayHelloAsync%26pid%3D10984%26release%3D%26service.name%3DServiceBean%3A%2Forg.apache.dubbo.demo.DemoService%26side%3Dprovider%26timestamp%3D1690361577209&id=registryConfig&pid=10984&registry=zookeeper&timestamp=1690361577196
                        // 这个Invoker中包括了服务的实现者、服务接口类、服务的注册地址（针对当前服务的，参数export指定了当前服务）
                        // 此invoker表示一个可执行的服务，调用invoker的invoke()方法即可执行服务,同时此invoker也可用来暴露
                        Invoker<?> invoker = PROXY_FACTORY.getInvoker(ref, (Class) interfaceClass,
                                registryURL.addParameterAndEncoded(EXPORT_KEY, url.toFullString()));

                        // DelegateProviderMetaDataInvoker的作用是保存了Invoker对象和服务的配置ServiceConfig对象
                        DelegateProviderMetaDataInvoker wrapperInvoker = new DelegateProviderMetaDataInvoker(invoker, this);

                        // 暴露服务
                        // 到此为止做了哪些事情？ ServiceBean.export()-->刷新ServiceBean的参数-->得到注册中心URL和协议URL-->遍历每个协议URL-->组成服务URL-->生成可执行服务Invoker-->暴露服务

                        // Protocol接口有3个包装类，一个是ProtocolFilterWrapper、ProtocolListenerWrapper、QosProtocolWrapper，
                        // 所以实际上在调用export方法时，会先经过这3个包装类的export方法，
                        // ProtocolFilterWrapper、ProtocolListenerWrapper 的export方法中都对Registry协议进行了判断，不做处理
                        // QosProtocolWrapper 的export方法在Registry协议下会启动qos服务器，其它协议不做处理

                        // 使用特定的协议来对服务进行暴露，这里的协议为registry，暴露成功后得到一个Exporter
                        // 1. 先使用registry协议对应的InterfaceCompatibleRegistryProtocol进行服务注册
                        //  ，但是InterfaceCompatibleRegistryProtocol没有覆写export方法，所以调的是父类RegistryProtocol的export方法
                        //      registry://   ---> InterfaceCompatibleRegistryProtocol
                        //      zookeeper://  ---> ZookeeperRegistry
                        //      dubbo://      ---> DubboProtocol
                        // 2. 注册完了之后，在RegistryProtocol的export方法中使用DubboProtocol进行真正的服务暴露
                        Exporter<?> exporter = PROTOCOL.export(wrapperInvoker);
                        exporters.add(exporter);
                    }
                } else {
                    /* 没有配置注册中心时，也会暴露服务 */

                    if (logger.isInfoEnabled()) {
                        logger.info("Export dubbo service " + interfaceClass.getName() + " to url " + url);
                    }
                    Invoker<?> invoker = PROXY_FACTORY.getInvoker(ref, (Class) interfaceClass, url);
                    DelegateProviderMetaDataInvoker wrapperInvoker = new DelegateProviderMetaDataInvoker(invoker, this);

                    Exporter<?> exporter = PROTOCOL.export(wrapperInvoker);
                    exporters.add(exporter);
                }

                MetadataUtils.publishServiceDefinition(url);
            }
        }
        this.urls.add(url);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    /**
     * always export injvm
     */
    private void exportLocal(URL url) {
        // 根据传入的url新建1个url
        URL local = URLBuilder.from(url)
                // 把新url的protocol改成injvm
                .setProtocol(LOCAL_PROTOCOL)
                // 把新url的Host改成127.0.0.1
                .setHost(LOCALHOST_VALUE)
                .setPort(0)
                .build();
        // 使用特定的协议来对服务进行暴露，这里的协议为InjvmProtocol，暴露成功后得到一个Exporter
        Exporter<?> exporter = PROTOCOL.export(
                PROXY_FACTORY.getInvoker(ref, (Class) interfaceClass, local));
        exporters.add(exporter);
        logger.info("Export dubbo service " + interfaceClass.getName() + " to local registry url : " + local);
    }

    /**
     * Determine if it is injvm
     *
     * @return
     */
    private boolean isOnlyInJvm() {
        return getProtocols().size() == 1
                && LOCAL_PROTOCOL.equalsIgnoreCase(getProtocols().get(0).getName());
    }


    /**
     * Register & bind IP address for service provider, can be configured separately.
     * Configuration priority: environment variables -> java system properties -> host property in config file ->
     * /etc/hosts -> default network address -> first available network address
     *
     * @param protocolConfig
     * @param registryURLs
     * @param map
     * @return
     */
    private String findConfigedHosts(ProtocolConfig protocolConfig,
                                     List<URL> registryURLs,
                                     Map<String, String> map) {
        /* 为服务提供者绑定ip地址，比如启动Tomcat时可以绑定到特定的ip */

        boolean anyhost = false;

        // 从系统环境变量中获取指定的ip进行绑定
        String hostToBind = getValueFromConfig(protocolConfig, DUBBO_IP_TO_BIND);
        if (hostToBind != null && hostToBind.length() > 0 && isInvalidLocalHost(hostToBind)) {
            throw new IllegalArgumentException("Specified invalid bind ip from property:" + DUBBO_IP_TO_BIND + ", value:" + hostToBind);
        }

        // if bind ip is not found in environment, keep looking up
        if (StringUtils.isEmpty(hostToBind)) {
            // 获取protocolConfig中指定的host
            hostToBind = protocolConfig.getHost();
            if (provider != null && StringUtils.isEmpty(hostToBind)) {
                hostToBind = provider.getHost();
            }
            // 如果是无效的ip，localhost, 0.0.0.0都属于无效，localhost表示只有本机能访问，0.0.0.0表示任何主机都能访问
            if (isInvalidLocalHost(hostToBind)) {
                anyhost = true;
                logger.info("No valid ip found from environment, try to get local host.");
                // 先从网卡中取，如果没有则取127.0.0.1
                hostToBind = getLocalHost();
            }
        }

        map.put(BIND_IP_KEY, hostToBind);

        // registry ip is not used for bind ip by default
        String hostToRegistry = getValueFromConfig(protocolConfig, DUBBO_IP_TO_REGISTRY);
        if (hostToRegistry != null && hostToRegistry.length() > 0 && isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException(
                    "Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        } else if (StringUtils.isEmpty(hostToRegistry)) {
            // bind ip is used as registry ip by default
            hostToRegistry = hostToBind;
        }

        map.put(ANYHOST_KEY, String.valueOf(anyhost));

        return hostToRegistry;
    }


    /**
     * Register port and bind port for the provider, can be configured separately
     * Configuration priority: environment variable -> java system properties -> port property in protocol config file
     * -> protocol default port
     *
     * @param protocolConfig
     * @param name
     * @param protocolConfigNum
     * @return
     */
    private Integer findConfigedPorts(ProtocolConfig protocolConfig,
                                      String name,
                                      Map<String, String> map, int protocolConfigNum) {
        Integer portToBind = null;

        // parse bind port from environment
        String port = getValueFromConfig(protocolConfig, DUBBO_PORT_TO_BIND);
        portToBind = parsePort(port);

        // if there's no bind port found from environment, keep looking up.
        if (portToBind == null) {
            portToBind = protocolConfig.getPort();
            if (provider != null && (portToBind == null || portToBind == 0)) {
                portToBind = provider.getPort();
            }
            final int defaultPort = ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(name).getDefaultPort();
            if (portToBind == null || portToBind == 0) {
                portToBind = defaultPort;
            }
            if (portToBind <= 0) {
                portToBind = getRandomPort(name);
                if (portToBind == null || portToBind < 0) {
                    portToBind = getAvailablePort(defaultPort);
                    putRandomPort(name, portToBind);
                }
            }
        }

        // registry port, not used as bind port by default
        String key = DUBBO_PORT_TO_REGISTRY;
        if (protocolConfigNum > 1) {
            key = getProtocolConfigId(protocolConfig).toUpperCase() + "_" + key;
        }
        String portToRegistryStr = getValueFromConfig(protocolConfig, key);
        Integer portToRegistry = parsePort(portToRegistryStr);
        if (portToRegistry != null) {
            portToBind = portToRegistry;
        }

        // save bind port, used as url's key later
        map.put(BIND_PORT_KEY, String.valueOf(portToBind));

        return portToBind;
    }


    private String getProtocolConfigId(ProtocolConfig config) {
        return Optional.ofNullable(config.getId()).orElse(DUBBO);
    }

    private Integer parsePort(String configPort) {
        Integer port = null;
        if (configPort != null && configPort.length() > 0) {
            try {
                Integer intPort = Integer.parseInt(configPort);
                if (isInvalidPort(intPort)) {
                    throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
                }
                port = intPort;
            } catch (Exception e) {
                throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
            }
        }
        return port;
    }

    private String getValueFromConfig(ProtocolConfig protocolConfig, String key) {
        String protocolPrefix = protocolConfig.getName().toUpperCase() + "_";
        String value = ConfigUtils.getSystemProperty(protocolPrefix + key);
        if (StringUtils.isEmpty(value)) {
            value = ConfigUtils.getSystemProperty(key);
        }
        return value;
    }

    private Integer getRandomPort(String protocol) {
        protocol = protocol.toLowerCase();
        return RANDOM_PORT_MAP.getOrDefault(protocol, Integer.MIN_VALUE);
    }

    private void putRandomPort(String protocol, Integer port) {
        protocol = protocol.toLowerCase();
        if (!RANDOM_PORT_MAP.containsKey(protocol)) {
            RANDOM_PORT_MAP.put(protocol, port);
            logger.warn("Use random available port(" + port + ") for protocol " + protocol);
        }
    }

    private void postProcessConfig() {
        List<ConfigPostProcessor> configPostProcessors = ExtensionLoader.getExtensionLoader(ConfigPostProcessor.class)
                .getActivateExtension(URL.valueOf(CONFIG_POST_PROCESSOR_PROTOCOL), (String[]) null);
        configPostProcessors.forEach(component -> component.postProcessServiceConfig(this));
    }

    /**
     * Dispatch an {@link Event event}
     *
     * @param event an {@link Event event}
     * @since 2.7.5
     */
    private void dispatch(Event event) {
        EventDispatcher.getDefaultExtension().dispatch(event);
    }

    public DubboBootstrap getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(DubboBootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    public String getServiceName() {

        if (!StringUtils.isBlank(serviceName)) {
            return serviceName;
        }
        String generateVersion = version;
        String generateGroup = group;

        if (StringUtils.isBlank(version) && provider != null) {
            generateVersion = provider.getVersion();
        }

        if (StringUtils.isBlank(group) && provider != null) {
            generateGroup = provider.getGroup();
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("ServiceBean:");

        if (!StringUtils.isBlank(generateGroup)) {
            stringBuilder.append(generateGroup);
        }

        stringBuilder.append("/").append(interfaceName);

        if (!StringUtils.isBlank(generateVersion)) {
            stringBuilder.append(":").append(generateVersion);
        }

        serviceName = stringBuilder.toString();
        return serviceName;
    }
}

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
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.constants.RegistryConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.config.event.ReferenceConfigDestroyedEvent;
import org.apache.dubbo.config.event.ReferenceConfigInitializedEvent;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.config.utils.ConfigValidationUtils;
import org.apache.dubbo.event.Event;
import org.apache.dubbo.event.EventDispatcher;
import org.apache.dubbo.registry.client.metadata.MetadataUtils;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.cluster.support.registry.ZoneAwareCluster;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.AsyncMethodInfo;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;
import org.apache.dubbo.rpc.protocol.injvm.InjvmProtocol;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.CLUSTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SEPARATOR_CHAR;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.LOCALHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METHODS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MONITOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROXY_CLASS_REF;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.REVISION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SEMICOLON_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.SUBSCRIBED_SERVICE_NAMES_KEY;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidLocalHost;
import static org.apache.dubbo.common.utils.StringUtils.splitToSet;
import static org.apache.dubbo.config.Constants.DUBBO_IP_TO_REGISTRY;
import static org.apache.dubbo.registry.Constants.CONSUMER_PROTOCOL;
import static org.apache.dubbo.registry.Constants.REGISTER_IP_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_PROTOCOL;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;

/**
 * Please avoid using this class for any new application,
 * use {@link ReferenceConfigBase} instead.
 */
public class ReferenceConfig<T> extends ReferenceConfigBase<T> {

    public static final Logger logger = LoggerFactory.getLogger(ReferenceConfig.class);

    /**
     * The {@link Protocol} implementation with adaptive functionality,it will be different in different scenarios.
     * A particular {@link Protocol} implementation is determined by the protocol attribute in the {@link URL}.
     * For example:
     *
     * <li>when the url is registry://224.5.6.7:1234/org.apache.dubbo.registry.RegistryService?application=dubbo-sample,
     * then the protocol is <b>RegistryProtocol</b></li>
     *
     * <li>when the url is dubbo://224.5.6.7:1234/org.apache.dubbo.config.api.DemoService?application=dubbo-sample, then
     * the protocol is <b>DubboProtocol</b></li>
     * <p>
     * Actually，when the {@link ExtensionLoader} init the {@link Protocol} instants,it will automatically wraps two
     * layers, and eventually will get a <b>ProtocolFilterWrapper</b> or <b>ProtocolListenerWrapper</b>
     */
    private static final Protocol REF_PROTOCOL = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

    /**
     * The {@link Cluster}'s implementation with adaptive functionality, and actually it will get a {@link Cluster}'s
     * specific implementation who is wrapped with <b>MockClusterInvoker</b>
     */
    private static final Cluster CLUSTER = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    /**
     * A {@link ProxyFactory} implementation that will generate a reference service's proxy,the JavassistProxyFactory is
     * its default implementation
     */
    private static final ProxyFactory PROXY_FACTORY = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    /**
     * The interface proxy reference
     */
    private transient volatile T ref;

    /**
     * The invoker of the reference service
     */
    private transient volatile Invoker<?> invoker;

    /**
     * The flag whether the ReferenceConfig has been initialized
     */
    private transient boolean initialized;

    /**
     * whether this ReferenceConfig has been destroyed
     */
    private transient volatile boolean destroyed;

    private final ServiceRepository repository;

    private DubboBootstrap bootstrap;

    /**
     * The service names that the Dubbo interface subscribed.
     *
     * @since 2.7.8
     */
    private String services;

    public ReferenceConfig() {
        super();
        this.repository = ApplicationModel.getServiceRepository();
    }

    public ReferenceConfig(Reference reference) {
        super(reference);
        this.repository = ApplicationModel.getServiceRepository();
    }

    /**
     * Get a string presenting the service names that the Dubbo interface subscribed.
     * If it is a multiple-values, the content will be a comma-delimited String.
     *
     * @return non-null
     * @see RegistryConstants#SUBSCRIBED_SERVICE_NAMES_KEY
     * @since 2.7.8
     */
    @Deprecated
    @Parameter(key = SUBSCRIBED_SERVICE_NAMES_KEY)
    public String getServices() {
        return services;
    }

    /**
     * It's an alias method for {@link #getServices()}, but the more convenient.
     *
     * @return the String {@link List} presenting the Dubbo interface subscribed
     * @since 2.7.8
     */
    @Deprecated
    @Parameter(excluded = true)
    public Set<String> getSubscribedServices() {
        return splitToSet(getServices(), COMMA_SEPARATOR_CHAR);
    }

    /**
     * Set the service names that the Dubbo interface subscribed.
     *
     * @param services If it is a multiple-values, the content will be a comma-delimited String.
     * @since 2.7.8
     */
    public void setServices(String services) {
        this.services = services;
    }

    public synchronized T get() {
        if (destroyed) {
            throw new IllegalStateException("The invoker of ReferenceConfig(" + url + ") has already destroyed!");
        }
        if (ref == null) {
            // 初始化入口
            init();
        }
        // Invoke代理
        return ref;
    }

    @Override
    public synchronized void destroy() {
        if (ref == null) {
            return;
        }
        if (destroyed) {
            return;
        }
        destroyed = true;
        try {
            invoker.destroy();
        } catch (Throwable t) {
            logger.warn("Unexpected error occurred when destroy invoker of ReferenceConfig(" + url + ").", t);
        }
        invoker = null;
        ref = null;

        // dispatch a ReferenceConfigDestroyedEvent since 2.7.4
        dispatch(new ReferenceConfigDestroyedEvent(this));
    }

    public synchronized void init() {
        if (initialized) {
            return;
        }


        if (bootstrap == null) {
            bootstrap = DubboBootstrap.getInstance();
            // compatible with api call.
            if (null != this.getRegistries()) {
                bootstrap.registries(this.getRegistries());
            }
            bootstrap.initialize();
        }

        // 校验和更新配置属性
        checkAndUpdateSubConfigs();

        // Stub配置的合法性检查
        checkStubAndLocal(interfaceClass);

        // Mock配置的合法性检查
        ConfigValidationUtils.checkMock(interfaceClass, this);

        Map<String, String> map = new HashMap<String, String>();
        // 在URL参数里加上消费者的标志
        map.put(SIDE_KEY, CONSUMER_SIDE);

        // 在URL参数里运行时信息
        ReferenceConfigBase.appendRuntimeParameters(map);

        if (!ProtocolUtils.isGeneric(generic)) {
            // 非泛化调用

            // 给URL参数的revision属性赋值
            String revision = Version.getVersion(interfaceClass, version);
            if (revision != null && revision.length() > 0) {
                map.put(REVISION_KEY, revision);
            }

            // 给URL参数的methods属性赋值
            String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();
            if (methods.length == 0) {
                logger.warn("No method found in service interface " + interfaceClass.getName());
                // 接口没有方法赋值为 *
                map.put(METHODS_KEY, ANY_VALUE);
            } else {
                // 赋值为以逗号,连接的方法名字符串
                map.put(METHODS_KEY, StringUtils.join(new HashSet<String>(Arrays.asList(methods)), COMMA_SEPARATOR));
            }
        }
        // 将URL参数的interface属性赋值为远程服务的接口类名称
        map.put(INTERFACE_KEY, interfaceName);

        /* 对map里的值做继承和覆盖操作，获取优先级最高最全的参数值 */
        // 使用指标参数覆盖参数值，如果没有配置则取默认的指标
        AbstractConfig.appendParameters(map, getMetrics());
        // 使用应用相关参数覆盖参数值，如果没有配置则取默认的应用
        AbstractConfig.appendParameters(map, getApplication());
        // 使用模块相关参数覆盖参数值，如果没有配置则取默认的模块
        AbstractConfig.appendParameters(map, getModule());
        // remove 'default.' prefix for configs from ConsumerConfig
        // appendParameters(map, consumer, Constants.DEFAULT_KEY);
        // 使用消费者相关参数覆盖参数值
        AbstractConfig.appendParameters(map, consumer);
        // 使用引用本身相关参数覆盖参数值
        AbstractConfig.appendParameters(map, this);

        // 给URL参数的metadata-type属性赋值
        MetadataReportConfig metadataReportConfig = getMetadataReportConfig();
        if (metadataReportConfig != null && metadataReportConfig.isValid()) {
            map.putIfAbsent(METADATA_KEY, REMOTE_METADATA_STORAGE_TYPE);
        }

        // 在URL参数里加上方法相关信息
        Map<String, AsyncMethodInfo> attributes = null;
        if (CollectionUtils.isNotEmpty(getMethods())) {
            attributes = new HashMap<>();
            for (MethodConfig methodConfig : getMethods()) {
                // 在URL参数里加上方法相关参数值
                AbstractConfig.appendParameters(map, methodConfig, methodConfig.getName());
                // 更正retry属性值
                String retryKey = methodConfig.getName() + ".retry";
                if (map.containsKey(retryKey)) {
                    // 移除方法的retry属性
                    String retryValue = map.remove(retryKey);
                    if ("false".equals(retryValue)) {
                        //如果方法的retry属性配置成false，那么方法retries的属性的值改成0
                        map.put(methodConfig.getName() + ".retries", "0");
                    }
                }
                // 异步方法信息
                AsyncMethodInfo asyncMethodInfo = AbstractConfig.convertMethodConfig2AsyncInfo(methodConfig);
                if (asyncMethodInfo != null) {
//                    consumerModel.getMethodModel(methodConfig.getName()).addAttribute(ASYNC_KEY, asyncMethodInfo);
                    attributes.put(methodConfig.getName(), asyncMethodInfo);
                }
            }
        }

        // 给URL参数的register.ip属性赋值，注册到注册中心的ip
        String hostToRegistry = ConfigUtils.getSystemProperty(DUBBO_IP_TO_REGISTRY);
        if (StringUtils.isEmpty(hostToRegistry)) {
            hostToRegistry = NetUtils.getLocalHost();
        } else if (isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException(
                    "Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        }
        map.put(REGISTER_IP_KEY, hostToRegistry);

        serviceMetadata.getAttachments().putAll(map);

        // 根据参数生成引用对象，执行方法的代理对象
        ref = createProxy(map);

        serviceMetadata.setTarget(ref);
        serviceMetadata.addAttribute(PROXY_CLASS_REF, ref);
        ConsumerModel consumerModel = repository.lookupReferredService(serviceMetadata.getServiceKey());
        consumerModel.setProxyObject(ref);
        consumerModel.init(attributes);

        initialized = true;

        checkInvokerAvailable();

        // dispatch a ReferenceConfigInitializedEvent since 2.7.4
        dispatch(new ReferenceConfigInitializedEvent(this, invoker));
    }

    @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
    private T createProxy(Map<String, String> map) {
        if (shouldJvmRefer(map)) {
            URL url = new URL(LOCAL_PROTOCOL, LOCALHOST_VALUE, 0, interfaceClass.getName()).addParameters(map);
            // injvm://127.0.0.1/org.apache.dubbo.demo.DemoService?application=dubbo-demo-annotation-consumer&dubbo=2.0.2&init=false&interface=org.apache.dubbo.demo.DemoService&methods=sayHello,sayHelloAsync&pid=2284&register.ip=192.168.56.1&release=&side=consumer&sticky=false&timestamp=1690711172957

            invoker = REF_PROTOCOL.refer(interfaceClass, url);
            if (logger.isInfoEnabled()) {
                logger.info("Using injvm service " + interfaceClass.getName());
            }
        } else {
            // 可以在@DubboReference的url属性中配置多个url，可以是点对点的服务地址，也可以是注册中心的地址
            urls.clear();
            if (url != null && url.length() > 0) { // user specified URL, could be peer-to-peer address, or register center's address.
                // @DubboReference中指定了url属性
                // 用户指定的URL，可以是点对点地址，也可以是注册中心的地址。

                // 用;号切割
                String[] us = SEMICOLON_SPLIT_PATTERN.split(url);
                if (us != null && us.length > 0) {
                    for (String u : us) {
                        URL url = URL.valueOf(u);
                        if (StringUtils.isEmpty(url.getPath())) {
                            url = url.setPath(interfaceName);
                        }
                        // 如果是注册中心地址，则在url中添加一个refer参数
                        if (UrlUtils.isRegistry(url)) {
                            // 把消费者端配置的参数序列化并编码放入注册中心URL的refer参数中
                            urls.add(url.addParameterAndEncoded(REFER_KEY, StringUtils.toQueryString(map)));
                        } else {
                            // 如果是服务地址
                            // 有可能url中配置了参数，map中表示的服务消费者消费服务时的参数，所以需要合并
                            urls.add(ClusterUtils.mergeUrl(url, map));
                        }
                    }
                }
            } else { // assemble URL from register center's configuration
                // if protocols not injvm checkRegistry
                // @DubboReference中没有配置url属性

                // @DubboReference中的protocol属性表示使用哪个协议调用服务，如果不是本地调用协议 injvm:// ，则把注册中心地址找出来
                // 对于 injvm:// 协议已经在之前的逻辑中就已经生成invoke了
                if (!LOCAL_PROTOCOL.equalsIgnoreCase(getProtocol())) {
                    // 校验注册中心配置，如果没有会自动补全
                    checkRegistry();
                    // 将注册中心配置转换成注册中心URL
                    List<URL> us = ConfigValidationUtils.loadRegistries(this, false);
                    if (CollectionUtils.isNotEmpty(us)) {
                        for (URL u : us) {
                            // 加载监控中心URL
                            URL monitorUrl = ConfigValidationUtils.loadMonitor(this, u);
                            if (monitorUrl != null) {
                                map.put(MONITOR_KEY, URL.encode(monitorUrl.toFullString()));
                            }
                            // 把消费者端配置的参数序列化并编码放入注册中心地址的refer参数
                            urls.add(u.addParameterAndEncoded(REFER_KEY, StringUtils.toQueryString(map)));
                        }
                    }
                    if (urls.isEmpty()) {
                        throw new IllegalStateException(
                                "No such any registry to reference " + interfaceName + " on the consumer " + NetUtils.getLocalHost() +
                                        " use dubbo version " + Version.getVersion() +
                                        ", please config <dubbo:registry address=\"...\" /> to your spring config.");
                    }
                }
            }

            if (urls.size() == 1) {
                // 只配置了一个url，直接使用特定的协议来引用远程服务，引用成功后得到一个Invoker

                // Protocol接口有3个包装类，一个是ProtocolFilterWrapper、ProtocolListenerWrapper、QosProtocolWrapper，
                // 所以实际上在调用refer方法时，会先经过这3个包装类的refer方法，
                // ProtocolFilterWrapper、ProtocolListenerWrapper 的refer方法中都对Registry协议进行了判断，不做处理
                // QosProtocolWrapper 的refer方法在Registry协议下会启动qos服务器，其它协议不做处理

                // 这里的协议可能为registry（注册中心URL，远端服务地址从注册中心获取），也可能是直连URL的协议（比如：dubbo），
                //      registry://   ---> InterfaceCompatibleRegistryProtocol
                //      dubbo://      ---> DubboProtocol
                // 如果是注册中心URL（registry://）
                // 1. 先使用registry协议对应的InterfaceCompatibleRegistryProtocol进行服务注册，
                //    但是InterfaceCompatibleRegistryProtocol没有覆写refer方法，所以调的是父类RegistryProtocol的refer方法
                // 2. 会在RegistryProtocol的getInvoker方法创建RegistryDirectory，然后通过RegistryDirectory订阅远程服务目录
                // 3. RegistryDirectory会通过ZookeeperRegistry进行真正的zk节点监听
                // 4. RegistryDirectory同时也是一个监听器，ZookeeperRegistry订阅完服务地址之后，
                //    会回调RegistryDirectory的notify方法调用DubboProtocol进行真正的远程服务引用
                invoker = REF_PROTOCOL.refer(interfaceClass, urls.get(0));
                // MigrationInvoker->MockClusterInvoker->chain(AbstractCluster.InterceptorInvokerNode)->FailoverClusterInvoker->FailoverClusterInvoker.directory(RegistryDirectory)
                // RegistryDirectory.invokers->RegistryDirectory.InvokerDelegate->chain(FilterNode)->ListenerInvokerWrapper->AsyncToSyncInvoker->DubboInvoker
                // DubboInvoker.clients-->ReferenceCountExchangeClient->HeaderExchangeClient->NettyClient
                // NettyClient.handler--->MultiMessageHandler->HeartbeatHandler->AllChannelHandler->DecodeHandler->HeaderExchangeHandler->DubboProtocol.requestHandler
            } else {
                // 配置了多个url
                // urls里面，可能是点对点地址，也可能是注册中心的地址。

                List<Invoker<?>> invokers = new ArrayList<Invoker<?>>();
                URL registryURL = null;
                for (URL url : urls) {
                    // For multi-registry scenarios, it is not checked whether each referInvoker is available.
                    // Because this invoker may become available later.

                    // 根据每个url，使用特定的协议来引用远程服务，引用成功后得到一个Invoker
                    invokers.add(REF_PROTOCOL.refer(interfaceClass, url));

                    if (UrlUtils.isRegistry(url)) {
                        // 保存最后一个的注册中心URL
                        registryURL = url; // use last registry url
                    }
                }

                if (registryURL != null) { // registry url is available
                    // 如果这多个urls中存在注册中心url，则把所有invoker整合为ZoneAwareClusterInvoker，
                    // 该Invoker在调用时，提供一种策略来决定如何在它们之间分配流量：
                    // 1. 标记为“preferred=true”的注册中心具有最高优先级。
                    // 2. 检查当前请求所属的区域，首先选择具有相同区域的注册中心。
                    // 3. 根据每个注册中心的权重均衡所有注册中心之间的流量。
                    // 4. 挑选任何有空的注册中心。

                    // for multi-subscription scenario, use 'zone-aware' policy by default
                    // 对于多注册中心订阅场景，注册中心URL集群容错默认使用“zone-aware”策略
                    String cluster = registryURL.getParameter(CLUSTER_KEY, ZoneAwareCluster.NAME);
                    // StaticDirectory表示静态服务目录，里面的invokers是不会变的, 生成一个RegistryAwareCluster
                    // The invoker wrap sequence would be: ZoneAwareClusterInvoker(StaticDirectory) -> FailoverClusterInvoker(RegistryDirectory, routing happens here) -> Invoker
                    invoker = Cluster.getCluster(cluster, false).join(new StaticDirectory(registryURL, invokers));
                    // chain(AbstractCluster.InterceptorInvokerNode)->ZoneAwareClusterInvoker->ZoneAwareClusterInvoker.directory(StaticDirectory)
                    // StaticDirectory.invokers->MigrationInvoker->MockClusterInvoker->chain(AbstractCluster.InterceptorInvokerNode)->FailoverClusterInvoker->FailoverClusterInvoker.directory(RegistryDirectory)
                } else { // not a registry url, must be direct invoke.
                    // 如果urls中不存在注册中心URL，那就只有多个直连URL
                    // 如果invokers中第一个invoker的URL没有配置集群容错，则把所有invoker默认整合为ZoneAwareClusterInvoker
                    String cluster = CollectionUtils.isNotEmpty(invokers)
                            ?
                            (invokers.get(0).getUrl() != null ? invokers.get(0).getUrl().getParameter(CLUSTER_KEY, ZoneAwareCluster.NAME) :
                                    Cluster.DEFAULT)
                            : Cluster.DEFAULT;
                    invoker = Cluster.getCluster(cluster).join(new StaticDirectory(invokers));
                }
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Refer dubbo service " + interfaceClass.getName() + " from url " + invoker.getUrl());
        }

        URL consumerURL = new URL(CONSUMER_PROTOCOL, map.remove(REGISTER_IP_KEY), 0, map.get(INTERFACE_KEY), map);
        MetadataUtils.publishServiceDefinition(consumerURL);

        // create service proxy
        // 创建服务代理
        return (T) PROXY_FACTORY.getProxy(invoker, ProtocolUtils.isGeneric(generic));
    }

    private void checkInvokerAvailable() throws IllegalStateException {
        if (shouldCheck() && !invoker.isAvailable()) {
            invoker.destroy();
            throw new IllegalStateException("Failed to check the status of the service "
                    + interfaceName
                    + ". No provider available for the service "
                    + (group == null ? "" : group + "/")
                    + interfaceName +
                    (version == null ? "" : ":" + version)
                    + " from the url "
                    + invoker.getUrl()
                    + " to the consumer "
                    + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion());
        }
    }

    /**
     * This method should be called right after the creation of this class's instance, before any property in other config modules is used.
     * Check each config modules are created properly and override their properties if necessary.
     */
    public void checkAndUpdateSubConfigs() {
        if (StringUtils.isEmpty(interfaceName)) {
            throw new IllegalStateException("<dubbo:reference interface=\"\" /> interface not allow null!");
        }
        // 填充ReferenceConfig对象中的属性
        completeCompoundConfigs(consumer);
        // get consumer's global configuration
        // 保证consumer不为空
        checkDefault();

        // init some null configuration.
        // 通过扩展点加入一些自定义的属性初始化逻辑
        List<ConfigInitializer> configInitializers = ExtensionLoader.getExtensionLoader(ConfigInitializer.class)
                .getActivateExtension(URL.valueOf("configInitializer://"), (String[]) null);
        configInitializers.forEach(e -> e.initReferConfig(this));

        // 刷新ReferenceConfig对象的属性值
        this.refresh();


        // 更新泛化调用标志
        if (getGeneric() == null && getConsumer() != null) {
            // 给ReferenceConfig的generic属性赋值
            setGeneric(getConsumer().getGeneric());
        }

        // 给ReferenceConfig的interfaceClass属性赋值
        if (ProtocolUtils.isGeneric(generic)) {
            // 泛化调用
            interfaceClass = GenericService.class;
        } else {
            try {
                // 根据配置的interfaceName加载服务接口类
                interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
                        .getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            // 检查@DubboReference注解里配置的方法是否包含在远程服务接口中
            // 设置MethodConfig的所属服务信息
            checkInterfaceAndMethods(interfaceClass, getMethods());
        }

        // 设置服务的元数据信息
        initServiceMetadata(consumer);
        serviceMetadata.setServiceType(getActualInterface());
        // TODO, uncomment this line once service key is unified
        serviceMetadata.setServiceKey(URL.buildKey(interfaceName, group, version));

        ServiceRepository repository = ApplicationModel.getServiceRepository();
        ServiceDescriptor serviceDescriptor = repository.registerService(interfaceClass);
        // 在repository中注册
        repository.registerConsumer(
                serviceMetadata.getServiceKey(),
                serviceDescriptor,
                this,
                null,
                serviceMetadata);

        // 从系统变量或文件中拿接口名对应的配置项，指定点对点调用时的URL
        resolveFile();
        // 校验属性的合法性
        ConfigValidationUtils.validateReferenceConfig(this);
        // 通过扩展点加入一些自定义的属性处理逻辑
        postProcessConfig();
    }


    /**
     * Figure out should refer the service in the same JVM from configurations. The default behavior is true
     * 1. if injvm is specified, then use it
     * 2. then if a url is specified, then assume it's a remote call
     * 3. otherwise, check scope parameter
     * 4. if scope is not specified but the target service is provided in the same JVM, then prefer to make the local
     * call, which is the default behavior
     */
    protected boolean shouldJvmRefer(Map<String, String> map) {
        /*
         * 根据配置计算是否应该引用同一JVM中的服务。默认行为是true。
         * 1. 如果指定了injvm，就使用injvm的值
         * 2. 如果指定了url，那么假设是一个远程调用
         * 3. 否则，校验map中的scope对应值
         * 4. 如果没有map中的scope没有值，但在同一JVM中提供了目标服务，则更倾向于进行本地调用，这是默认行为
         */

        URL tmpUrl = new URL("temp", "localhost", 0, map);
        boolean isJvmRefer;
        if (isInjvm() == null) {
            // injvm 为空
            // if a url is specified, don't do local reference
            if (url != null && url.length() > 0) {
                // 如果url属性被指定了，不走本地
                isJvmRefer = false;
            } else {
                // 否则根据tmpUrl参数（map）判断
                // by default, reference local service if there is
                isJvmRefer = InjvmProtocol.getInjvmProtocol().isInjvmRefer(tmpUrl);
            }
        } else {
            isJvmRefer = isInjvm();
        }
        return isJvmRefer;
    }

    /**
     * Dispatch an {@link Event event}
     *
     * @param event an {@link Event event}
     * @since 2.7.5
     */
    protected void dispatch(Event event) {
        EventDispatcher.getDefaultExtension().dispatch(event);
    }

    public DubboBootstrap getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(DubboBootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    private void postProcessConfig() {
        List<ConfigPostProcessor> configPostProcessors = ExtensionLoader.getExtensionLoader(ConfigPostProcessor.class)
                .getActivateExtension(URL.valueOf("configPostProcessor://"), (String[]) null);
        configPostProcessors.forEach(component -> component.postProcessReferConfig(this));
    }

    // just for test
    Invoker<?> getInvoker() {
        return invoker;
    }
}

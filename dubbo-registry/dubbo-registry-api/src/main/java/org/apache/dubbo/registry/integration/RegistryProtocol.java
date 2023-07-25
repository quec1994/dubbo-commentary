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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.Constants;
import org.apache.dubbo.registry.*;
import org.apache.dubbo.registry.client.ServiceDiscoveryRegistryDirectory;
import org.apache.dubbo.registry.client.migration.ServiceDiscoveryMigrationInvoker;
import org.apache.dubbo.registry.retry.ReExportTask;
import org.apache.dubbo.registry.support.SkipFailbackWrapperException;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.governance.GovernanceRuleRepository;
import org.apache.dubbo.rpc.cluster.support.MergeableCluster;
import org.apache.dubbo.rpc.cluster.support.migration.MigrationClusterInvoker;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.dubbo.common.constants.CommonConstants.*;
import static org.apache.dubbo.common.constants.FilterConstants.VALIDATION_KEY;
import static org.apache.dubbo.common.constants.QosConstants.*;
import static org.apache.dubbo.common.constants.RegistryConstants.*;
import static org.apache.dubbo.common.utils.UrlUtils.classifyUrls;
import static org.apache.dubbo.registry.Constants.REGISTER_KEY;
import static org.apache.dubbo.registry.Constants.*;
import static org.apache.dubbo.remoting.Constants.CHECK_KEY;
import static org.apache.dubbo.remoting.Constants.*;
import static org.apache.dubbo.rpc.Constants.INTERFACES;
import static org.apache.dubbo.rpc.Constants.*;
import static org.apache.dubbo.rpc.cluster.Constants.*;

/**
 * TODO, replace RegistryProtocol completely in the future.
 */
public class RegistryProtocol implements Protocol {
    public static final String[] DEFAULT_REGISTER_PROVIDER_KEYS = {
            APPLICATION_KEY, CODEC_KEY, EXCHANGER_KEY, SERIALIZATION_KEY, CLUSTER_KEY, CONNECTIONS_KEY, DEPRECATED_KEY,
            GROUP_KEY, LOADBALANCE_KEY, MOCK_KEY, PATH_KEY, TIMEOUT_KEY, TOKEN_KEY, VERSION_KEY, WARMUP_KEY,
            WEIGHT_KEY, TIMESTAMP_KEY, DUBBO_VERSION_KEY, RELEASE_KEY, TAG_KEY
    };

    public static final String[] DEFAULT_REGISTER_CONSUMER_KEYS = {
            APPLICATION_KEY, VERSION_KEY, GROUP_KEY, DUBBO_VERSION_KEY, RELEASE_KEY
    };

    private static final String REGISTRY_PROTOCOL_LISTENER_KEY = "registry.protocol.listener";
    private static final int DEFAULT_PORT = 9090;

    private static final Logger LOGGER = LoggerFactory.getLogger(RegistryProtocol.class);
    private final Map<URL, NotifyListener> overrideListeners = new ConcurrentHashMap<>();
    private final Map<String, ServiceConfigurationListener> serviceConfigurationListeners = new ConcurrentHashMap<>();
    private final ProviderConfigurationListener providerConfigurationListener = new ProviderConfigurationListener();
    // To solve the problem of RMI repeated exposure port conflicts, the services that have been exposed are no longer exposed.
    // providerurl <--> exporter
    private final ConcurrentMap<String, ExporterChangeableWrapper<?>> bounds = new ConcurrentHashMap<>();
    protected Protocol protocol;
    protected RegistryFactory registryFactory;
    protected ProxyFactory proxyFactory;

    private ConcurrentMap<URL, ReExportTask> reExportFailedTasks = new ConcurrentHashMap<>();
    private HashedWheelTimer retryTimer =
            new HashedWheelTimer(new NamedThreadFactory("DubboReexportTimer", true), DEFAULT_REGISTRY_RETRY_PERIOD, TimeUnit.MILLISECONDS,
                    128);

    // get the parameters which shouldn't been displayed in url string(Starting with .)
    private static String[] getHiddenKeys(URL url) {
        Map<String, String> params = url.getParameters();
        if (CollectionUtils.isNotEmptyMap(params)) {
            // 过滤url的参数，找到以.号开头的key
            return params.keySet().stream()
                    .filter(k -> k.startsWith(HIDDEN_KEY_PREFIX))
                    .toArray(String[]::new);
        } else {
            return new String[0];
        }
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistryFactory(RegistryFactory registryFactory) {
        this.registryFactory = registryFactory;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    public Map<URL, NotifyListener> getOverrideListeners() {
        return overrideListeners;
    }

    private void registerStatedUrl(URL registryUrl, URL registeredProviderUrl, boolean registered) {
        ProviderModel model = ApplicationModel.getProviderModel(registeredProviderUrl.getServiceKey());
        model.addStatedUrl(new ProviderModel.RegisterStatedURL(
                registeredProviderUrl,
                registryUrl,
                registered));
    }

    @Override
    public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {
        // registry://xxx?xx=xx&registry=zookeeper ---> zookeeper://xxx?xx=xx     表示注册中心
        // 将registry://xxx?xx=xx&registry=zookeeper 转为 zookeeper://xxx?xx=xx
        // 调用的是子类 InterfaceCompatibleRegistryProtocol 的 getRegistryUrl 方法
        URL registryUrl = getRegistryUrl(originInvoker);
        // zookeeper://127.0.0.1:2181/org.apache.dubbo.registry.RegistryService?application=dubbo-demo-annotation-provider&dubbo=2.0.2&export=dubbo%3A%2F%2F192.168.56.1%3A20880%2Forg.apache.dubbo.demo.DemoService%3Fanyhost%3Dtrue%26application%3Ddubbo-demo-annotation-provider%26bind.ip%3D192.168.56.1%26bind.port%3D20880%26deprecated%3Dfalse%26dubbo%3D2.0.2%26dynamic%3Dtrue%26generic%3Dfalse%26interface%3Dorg.apache.dubbo.demo.DemoService%26methods%3DsayHello%2CsayHelloAsync%26pid%3D16360%26release%3D%26service.name%3DServiceBean%3A%2Forg.apache.dubbo.demo.DemoService%26side%3Dprovider%26timestamp%3D1690364598026&id=registryConfig&pid=16360&timestamp=1690364598016

        // url to export locally
        // 得到服务提供者url，表示服务提供者
        URL providerUrl = getProviderUrl(originInvoker);
        // dubbo://192.168.56.1:20880/org.apache.dubbo.demo.DemoService?anyhost=true&application=dubbo-demo-annotation-provider&bind.ip=192.168.56.1&bind.port=20880&deprecated=false&dubbo=2.0.2&dynamic=true&generic=false&interface=org.apache.dubbo.demo.DemoService&methods=sayHello,sayHelloAsync&pid=16360&release=&service.name=ServiceBean:/org.apache.dubbo.demo.DemoService&side=provider&timestamp=1690364598026

        // Subscribe the override data
        // FIXME When the provider subscribes, it will affect the scene : a certain JVM exposes the service and call
        //  the same service. Because the subscribed is cached key with the name of the service, it causes the
        //  subscription information to cover.

        // overrideSubscribeUrl是老版本的动态配置监听url，表示了需要监听的服务以及监听的类型（configurators，这是老版本上的动态配置）
        // 在服务提供者url的基础上，生成一个overrideSubscribeUrl，协议为provider://，增加参数category=configurators&check=false
        final URL overrideSubscribeUrl = getSubscribedOverrideUrl(providerUrl);

        // 一个overrideSubscribeUrl对应一个OverrideListener，用来监听变化事件，监听到overrideSubscribeUrl的变化后，
        // OverrideListener就会根据变化进行相应处理，具体处理逻辑看OverrideListener的实现
        final OverrideListener overrideSubscribeListener = new OverrideListener(overrideSubscribeUrl, originInvoker);

        overrideListeners.put(overrideSubscribeUrl, overrideSubscribeListener);

        // 在这个方法里会利用providerConfigurationListener和serviceConfigurationListener去重写providerUrl
        // providerConfigurationListener表示应用级别的动态配置监听器，providerConfigurationListener是RegistyProtocol的一个属性
        // serviceConfigurationListener表示服务级别的动态配置监听器，serviceConfigurationListener是在每暴露一个服务时就会生成一个
        // 这两个监听器都是新版本中的动态配置监听器
        // 新版本动态配置监听的zk路径是：
        // 服务： /dubbo/config/dubbo/org.apache.dubbo.demo.DemoService::.configurators节点的内容
        // 应用： /dubbo/config/dubbo/dubbo-demo-annotation-provider.configurators节点的内容
        // 注意，要和配置中心的路径区分开来，配置中心的路径是：
        // 全局：/dubbo/config/dubbo/dubbo.properties节点的内容
        // 应用：/dubbo/config/dubbo/dubbo-demo-annotation-provider/dubbo.properties节点的内容
        providerUrl = overrideUrlWithConfig(providerUrl, overrideSubscribeListener);

        // export invoker
        // 根据动态配置重写了providerUrl之后，就会调用DubboProtocol或RestProtocol去进行真的的导出服务了
        final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker, providerUrl);

        // url to registry
        // 得到注册中心-ZookeeperRegistry
        final Registry registry = getRegistry(originInvoker);
        // 得到存入到注册中心去的providerUrl,会对服务提供者url中的参数进行简化
        final URL registeredProviderUrl = getUrlToRegistry(providerUrl, registryUrl);

        // decide if we need to delay publish
        //是否需要注册到注册中心
        boolean register = providerUrl.getParameter(REGISTER_KEY, true);
        if (register) {
            // 注册服务，把简化后的服务提供者url注册到registryUrl中去
            // 调用ZookeeperRegistry的register方法
            registry.register(registeredProviderUrl);
        }

        // register stated url on provider model
        registerStatedUrl(registryUrl, registeredProviderUrl, register);


        exporter.setRegisterUrl(registeredProviderUrl);
        exporter.setSubscribeUrl(overrideSubscribeUrl);

        // Deprecated! Subscribe to override rules in 2.6.x or before.
        // 针对老版本的动态配置，需要把overrideSubscribeListener绑定到overrideSubscribeUrl上去进行监听
        // 兼容老版本的配置修改，利用overrideSubscribeListener去监听旧版本的动态配置变化
        // 监听overrideSubscribeUrl   provider://192.168.56.1:20880/org.apache.dubbo.demo.DemoService?anyhost=true&application=dubbo-demo-annotation-provider&bind.ip=192.168.56.1&bind.port=20880&category=configurators&check=false&deprecated=false&dubbo=2.0.2&dynamic=true&generic=false&interface=org.apache.dubbo.demo.DemoService&methods=sayHello,sayHelloAsync&pid=16360&release=&service.name=ServiceBean:/org.apache.dubbo.demo.DemoService&side=provider&timestamp=1690364598026
        // 那么新版本的providerConfigurationListener和serviceConfigurationListener是在什么时候进行订阅的呢？在这两个类构造的时候
        // 老版本监听的zk路径是：/dubbo/org.apache.dubbo.demo.DemoService/configurators/override://0.0.0.0/org.apache.dubbo.demo.DemoService?category=configurators&compatible_config=true&dynamic=false&enabled=true&timeout=6000
        // 监听的是路径的内容，不是节点的内容
        registry.subscribe(overrideSubscribeUrl, overrideSubscribeListener);

        // 通知注册中心监听器
        notifyExport(exporter);
        //Ensure that a new exporter instance is returned every time export
        // Exporter的最外层包装类，这个类的主要作用是可以用来unexporter对应的服务
        return new DestroyableExporter<>(exporter);
    }

    private <T> void notifyExport(ExporterChangeableWrapper<T> exporter) {
        List<RegistryProtocolListener> listeners = ExtensionLoader.getExtensionLoader(RegistryProtocolListener.class)
                .getActivateExtension(exporter.getOriginInvoker().getUrl(), REGISTRY_PROTOCOL_LISTENER_KEY);
        if (CollectionUtils.isNotEmpty(listeners)) {
            for (RegistryProtocolListener listener : listeners) {
                listener.onExport(this, exporter);
            }
        }
    }

    private URL overrideUrlWithConfig(URL providerUrl, OverrideListener listener) {
        // ProviderConfigurationListener 会监听 应用名+".configurators"节点
        // ProviderConfigurationListener 监听的是配置中心 应用 的动态配置数据修改，
        // 所以它是在RegistryProtocol类中的一个属性，并且是随着RegistryProtocol实例化而实例化好的，一个应用中只有一个
        // 根据应用配置重写 url
        providerUrl = providerConfigurationListener.overrideUrl(providerUrl);

        // ServiceConfigurationListener会监听 服务接口名+":"+group+":"+version+".configurators"节点
        // ServiceConfigurationListener 监听的是配置中心 服务 的动态配置数据修改，
        // 和OverrideListener类似，也是对应一个服务的，所以在每个服务进行导出时都会生成一个
        ServiceConfigurationListener serviceConfigurationListener = new ServiceConfigurationListener(providerUrl, listener);
        serviceConfigurationListeners.put(providerUrl.getServiceKey(), serviceConfigurationListener);
        // 根据服务配置重写 url
        return serviceConfigurationListener.overrideUrl(providerUrl);
    }

    @SuppressWarnings("unchecked")
    private <T> ExporterChangeableWrapper<T> doLocalExport(final Invoker<T> originInvoker, URL providerUrl) {
        String key = getCacheKey(originInvoker);

        return (ExporterChangeableWrapper<T>) bounds.computeIfAbsent(key, s -> {
            // InvokerDelegate类的作用是保存Invoker对象和服务对应的providerUrl，执行时直接调用下层Invoker
            Invoker<?> invokerDelegate = new InvokerDelegate<>(originInvoker, providerUrl);
            // protocol属性的值是哪来的，是在SPI中注入进来的，是一个代理类
            // 这里实际利用的就是DubboProtocol或HttpProtocol去export
            // ExporterChangeableWrapper类主要负责在unexport对应服务之前，把服务URL从注册中心中移除，把该服务对应的动态配置监听器移除
            return new ExporterChangeableWrapper<>((Exporter<T>) protocol.export(invokerDelegate), originInvoker);
        });
    }

    public <T> void reExport(Exporter<T> exporter, URL newInvokerUrl) {
        if (exporter instanceof ExporterChangeableWrapper) {
            ExporterChangeableWrapper<T> exporterWrapper = (ExporterChangeableWrapper<T>) exporter;
            Invoker<T> originInvoker = exporterWrapper.getOriginInvoker();
            reExport(originInvoker, newInvokerUrl);
        }
    }

    /**
     * Reexport the invoker of the modified url
     *
     * @param originInvoker
     * @param newInvokerUrl
     * @param <T>
     */
    @SuppressWarnings("unchecked")
    public <T> void reExport(final Invoker<T> originInvoker, URL newInvokerUrl) {
        String key = getCacheKey(originInvoker);
        ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        // 这个服务之前注册到注册中心的服务提供者url
        URL registeredUrl = exporter.getRegisterUrl();

        // 获取准确的 RegistryUrl
        URL registryUrl = getRegistryUrl(originInvoker);
        // 简化服务提供者URL
        URL newProviderUrl = getUrlToRegistry(newInvokerUrl, registryUrl);

        // update local exporter
        // 到这里才能真正明白，为什么需要InvokerDelegate
        // InvokerDelegate表示一个服务执行者，由下层invoker+url构成，下层invoker不变，url可变
        Invoker<T> invokerDelegate = new InvokerDelegate<T>(originInvoker, newInvokerUrl);
        // 重新导出服务，这里实际利用的就是DubboProtocol或HttpProtocol去export
        exporter.setExporter(protocol.export(invokerDelegate));

        // update registry
        // 如果新的服务提供者url简化后的url和这个服务之前注册到注册中心的服务提供者url不相等，则需要把新的简化后的服务提供者url注册到注册中心去
        if (!newProviderUrl.equals(registeredUrl)) {
            try {
                doReExport(originInvoker, exporter, registryUrl, registeredUrl, newProviderUrl);
            } catch (Exception e) {
                ReExportTask oldTask = reExportFailedTasks.get(registeredUrl);
                if (oldTask != null) {
                    return;
                }
                ReExportTask task = new ReExportTask(
                        () -> doReExport(originInvoker, exporter, registryUrl, registeredUrl, newProviderUrl),
                        registeredUrl,
                        null
                );
                oldTask = reExportFailedTasks.putIfAbsent(registeredUrl, task);
                if (oldTask == null) {
                    // never has a retry task. then start a new task for retry.
                    retryTimer.newTimeout(task, registryUrl.getParameter(REGISTRY_RETRY_PERIOD_KEY, DEFAULT_REGISTRY_RETRY_PERIOD),
                            TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private <T> void doReExport(final Invoker<T> originInvoker, ExporterChangeableWrapper<T> exporter,
                                URL registryUrl, URL oldProviderUrl, URL newProviderUrl) {
        if (getProviderUrl(originInvoker).getParameter(REGISTER_KEY, true)) {
            Registry registry = null;
            try {
                registry = getRegistry(originInvoker);
            } catch (Exception e) {
                throw new SkipFailbackWrapperException(e);
            }

            LOGGER.info("Try to unregister old url: " + oldProviderUrl);
            // 把当前服务之前的服务提供URL从注册中心删掉
            registry.reExportUnregister(oldProviderUrl);

            LOGGER.info("Try to register new url: " + newProviderUrl);
            // 把新的registeredProviderUrl注册到注册中心
            registry.reExportRegister(newProviderUrl);
        }
        try {
            ProviderModel.RegisterStatedURL statedUrl = getStatedUrl(registryUrl, newProviderUrl);
            statedUrl.setProviderUrl(newProviderUrl);
            exporter.setRegisterUrl(newProviderUrl);
        } catch (Exception e) {
            throw new SkipFailbackWrapperException(e);
        }
    }

    private ProviderModel.RegisterStatedURL getStatedUrl(URL registryUrl, URL providerUrl) {
        ProviderModel providerModel = ApplicationModel.getServiceRepository()
                .lookupExportedService(providerUrl.getServiceKey());

        List<ProviderModel.RegisterStatedURL> statedUrls = providerModel.getStatedUrl();
        return statedUrls.stream()
                .filter(u -> u.getRegistryUrl().equals(registryUrl)
                        && u.getProviderUrl().getProtocol().equals(providerUrl.getProtocol()))
                .findFirst().orElseThrow(() -> new IllegalStateException("There should have at least one registered url."));
    }

    /**
     * Get an instance of registry based on the address of invoker
     *
     * @param originInvoker
     * @return
     */
    protected Registry getRegistry(final Invoker<?> originInvoker) {
        URL registryUrl = getRegistryUrl(originInvoker);
        return getRegistry(registryUrl);
    }

    protected Registry getRegistry(URL url) {
        try {
            return registryFactory.getRegistry(url);
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            throw t;
        }
    }

    protected URL
    getRegistryUrl(Invoker<?> originInvoker) {
        return originInvoker.getUrl();
    }

    protected URL getRegistryUrl(URL url) {
        if (SERVICE_REGISTRY_PROTOCOL.equals(url.getProtocol())) {
            return url;
        }
        return url.addParameter(REGISTRY_KEY, url.getProtocol()).setProtocol(SERVICE_REGISTRY_PROTOCOL);
    }

    /**
     * Return the url that is registered to the registry and filter the url parameter once
     *
     * @param providerUrl
     * @return url to registry.
     */
    private URL getUrlToRegistry(final URL providerUrl, final URL registryUrl) {
        // 简化服务提供者URL，把不用存到注册中心去的参数去除

        URL registeredProviderUrl = removeUselessParameters(providerUrl);

        //The address you see at the registry
        if (!registryUrl.getParameter(SIMPLIFIED_KEY, false)) {
            // 为提供者简化注册表。默认都是走这里
            // 移除key以"."开始的参数，以及其他跟服务本身没有关系的参数，比如监控，绑定ip，qos等等
            return registeredProviderUrl.removeParameters(getHiddenKeys(registeredProviderUrl)).removeParameters(
                    MONITOR_KEY, BIND_IP_KEY, BIND_PORT_KEY, QOS_ENABLE, QOS_HOST, QOS_PORT, ACCEPT_FOREIGN_IP, VALIDATION_KEY,
                    INTERFACES);
        } else {
            // 本身就是简化了的 providerUrl
            // 简化注册表后，为 provider 单独添加的一些参数
            String extraKeys = registryUrl.getParameter(EXTRA_KEYS_KEY, "");
            // if path is not the same as interface name then we should keep INTERFACE_KEY,
            // otherwise, the registry structure of zookeeper would be '/dubbo/path/providers',
            // but what we expect is '/dubbo/interface/providers'

            // 如果path不等于interface的值，那么则把 interface 添加到extraKeys中
            // 如果path与接口名称不相同，那么我们应该保留interface，
            // 否则，zookeeper的注册表结构将是“/dubo/path/providers”，
            // 但我们期望的是“/dobo/interface/provider”
            if (!registeredProviderUrl.getPath().equals(registeredProviderUrl.getParameter(INTERFACE_KEY))) {
                if (StringUtils.isNotEmpty(extraKeys)) {
                    extraKeys += ",";
                }
                extraKeys += INTERFACE_KEY;
            }
            // paramsToRegistry包括了DEFAULT_REGISTER_PROVIDER_KEYS和extraKeys
            String[] paramsToRegistry = getParamsToRegistry(DEFAULT_REGISTER_PROVIDER_KEYS
                    , COMMA_SPLIT_PATTERN.split(extraKeys));

            // 生成只含有paramsToRegistry和方法对应的参数
            return URL.valueOf(registeredProviderUrl, paramsToRegistry, registeredProviderUrl.getParameter(METHODS_KEY, (String[]) null));
        }

    }

    /**
     * Remove information that does not require registration
     *
     * @param providerUrl
     * @return
     */
    private URL removeUselessParameters(URL providerUrl) {
        return providerUrl.removeParameters(ON_CONNECT_KEY, ON_DISCONNECT_KEY);
    }

    private URL getSubscribedOverrideUrl(URL registeredProviderUrl) {
        // 注意，setProtocol方法会重新new一个url
        return registeredProviderUrl.setProtocol(PROVIDER_PROTOCOL)
                .addParameters(CATEGORY_KEY, CONFIGURATORS_CATEGORY, CHECK_KEY, String.valueOf(false));
    }

    /**
     * Get the address of the providerUrl through the url of the invoker
     *
     * @param originInvoker
     * @return
     */
    private URL getProviderUrl(final Invoker<?> originInvoker) {
        String export = originInvoker.getUrl().getParameterAndDecoded(EXPORT_KEY);
        if (export == null || export.length() == 0) {
            throw new IllegalArgumentException("The registry export url is null! registry: " + originInvoker.getUrl());
        }
        return URL.valueOf(export);
    }

    /**
     * Get the key cached in bounds by invoker
     *
     * @param originInvoker
     * @return
     */
    private String getCacheKey(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);
        String key = providerUrl.removeParameters("dynamic", "enabled").toFullString();
        return key;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        url = getRegistryUrl(url);
        Registry registry = getRegistry(url);
        if (RegistryService.class.equals(type)) {
            return proxyFactory.getInvoker((T) registry, type, url);
        }

        // group="a,b" or group="*"
        Map<String, String> qs = StringUtils.parseQueryString(url.getParameterAndDecoded(REFER_KEY));
        String group = qs.get(GROUP_KEY);
        if (group != null && group.length() > 0) {
            if ((COMMA_SPLIT_PATTERN.split(group)).length > 1 || "*".equals(group)) {
                return doRefer(Cluster.getCluster(MergeableCluster.NAME), registry, type, url, qs);
            }
        }

        Cluster cluster = Cluster.getCluster(qs.get(CLUSTER_KEY));
        return doRefer(cluster, registry, type, url, qs);
    }

    protected <T> Invoker<T> doRefer(Cluster cluster, Registry registry, Class<T> type, URL url, Map<String, String> parameters) {
        URL consumerUrl = new URL(CONSUMER_PROTOCOL, parameters.remove(REGISTER_IP_KEY), 0, type.getName(), parameters);
        ClusterInvoker<T> migrationInvoker = getMigrationInvoker(this, cluster, registry, type, url, consumerUrl);
        return interceptInvoker(migrationInvoker, url, consumerUrl);
    }

    protected <T> ClusterInvoker<T> getMigrationInvoker(RegistryProtocol registryProtocol, Cluster cluster, Registry registry,
                                                        Class<T> type, URL url, URL consumerUrl) {
        return new ServiceDiscoveryMigrationInvoker<T>(registryProtocol, cluster, registry, type, url, consumerUrl);
    }

    protected <T> Invoker<T> interceptInvoker(ClusterInvoker<T> invoker, URL url, URL consumerUrl) {
        List<RegistryProtocolListener> listeners = findRegistryProtocolListeners(url);
        if (CollectionUtils.isEmpty(listeners)) {
            return invoker;
        }

        for (RegistryProtocolListener listener : listeners) {
            listener.onRefer(this, invoker, consumerUrl);
        }
        return invoker;
    }

    public <T> ClusterInvoker<T> getServiceDiscoveryInvoker(Cluster cluster, Registry registry, Class<T> type, URL url) {
        DynamicDirectory<T> directory = new ServiceDiscoveryRegistryDirectory<>(type, url);
        return doCreateInvoker(directory, cluster, registry, type);
    }

    public <T> ClusterInvoker<T> getInvoker(Cluster cluster, Registry registry, Class<T> type, URL url) {
        // FIXME, this method is currently not used, create the right registry before enable.
        DynamicDirectory<T> directory = new RegistryDirectory<>(type, url);
        return doCreateInvoker(directory, cluster, registry, type);
    }

    protected <T> ClusterInvoker<T> doCreateInvoker(DynamicDirectory<T> directory, Cluster cluster, Registry registry, Class<T> type) {
        directory.setRegistry(registry);
        directory.setProtocol(protocol);
        // all attributes of REFER_KEY
        Map<String, String> parameters = new HashMap<String, String>(directory.getConsumerUrl().getParameters());
        URL urlToRegistry = new URL(CONSUMER_PROTOCOL, parameters.remove(REGISTER_IP_KEY), 0, type.getName(), parameters);
        if (directory.isShouldRegister()) {
            directory.setRegisteredConsumerUrl(urlToRegistry);
            registry.register(directory.getRegisteredConsumerUrl());
        }
        directory.buildRouterChain(urlToRegistry);
        directory.subscribe(toSubscribeUrl(urlToRegistry));

        return (ClusterInvoker<T>) cluster.join(directory);
    }

    public <T> void reRefer(ClusterInvoker<?> invoker, URL newSubscribeUrl) {
        if (!(invoker instanceof MigrationClusterInvoker)) {
            LOGGER.error("Only invoker type of MigrationClusterInvoker supports reRefer, current invoker is " + invoker.getClass());
            return;
        }

        MigrationClusterInvoker<?> migrationClusterInvoker = (MigrationClusterInvoker<?>) invoker;
        migrationClusterInvoker.reRefer(newSubscribeUrl);
    }

    public static URL toSubscribeUrl(URL url) {
        return url.addParameter(CATEGORY_KEY, PROVIDERS_CATEGORY + "," + CONFIGURATORS_CATEGORY + "," + ROUTERS_CATEGORY);
    }

    protected List<RegistryProtocolListener> findRegistryProtocolListeners(URL url) {
        return ExtensionLoader.getExtensionLoader(RegistryProtocolListener.class)
                .getActivateExtension(url, REGISTRY_PROTOCOL_LISTENER_KEY);
    }

    // available to test
    public String[] getParamsToRegistry(String[] defaultKeys, String[] additionalParameterKeys) {
        int additionalLen = additionalParameterKeys.length;
        String[] registryParams = new String[defaultKeys.length + additionalLen];
        // 把defaultKeys复制到registryParams中
        System.arraycopy(defaultKeys, 0, registryParams, 0, defaultKeys.length);
        // 把additionalParameterKeys复制到registryParams中
        System.arraycopy(additionalParameterKeys, 0, registryParams, defaultKeys.length, additionalLen);
        // registryParams包括了defaultKeys和additionalParameterKeys
        return registryParams;
    }

    @Override
    public void destroy() {
        List<RegistryProtocolListener> listeners = ExtensionLoader.getExtensionLoader(RegistryProtocolListener.class)
                .getLoadedExtensionInstances();
        if (CollectionUtils.isNotEmpty(listeners)) {
            for (RegistryProtocolListener listener : listeners) {
                listener.onDestroy();
            }
        }

        List<Exporter<?>> exporters = new ArrayList<Exporter<?>>(bounds.values());
        for (Exporter<?> exporter : exporters) {
            exporter.unexport();
        }
        bounds.clear();

        String application = ApplicationModel.tryGetApplication();
        if (application == null) {
            // already removed
            return;
        }
        ExtensionLoader.getExtensionLoader(GovernanceRuleRepository.class).getDefaultExtension()
                .removeListener(application + CONFIGURATORS_SUFFIX, providerConfigurationListener);
    }

    @Override
    public List<ProtocolServer> getServers() {
        return protocol.getServers();
    }

    // merge the urls of configurators
    private static URL getConfiguredInvokerUrl(List<Configurator> configurators, URL url) {
        if (configurators != null && configurators.size() > 0) {
            for (Configurator configurator : configurators) {
                url = configurator.configure(url);
            }
        }
        return url;
    }

    public static class InvokerDelegate<T> extends InvokerWrapper<T> {
        private final Invoker<T> invoker;

        /**
         * @param invoker
         * @param url     invoker.getUrl return this value
         */
        public InvokerDelegate(Invoker<T> invoker, URL url) {
            super(invoker, url);
            this.invoker = invoker;
        }

        public Invoker<T> getInvoker() {
            if (invoker instanceof InvokerDelegate) {
                return ((InvokerDelegate<T>) invoker).getInvoker();
            } else {
                return invoker;
            }
        }
    }

    private static class DestroyableExporter<T> implements Exporter<T> {

        private Exporter<T> exporter;

        public DestroyableExporter(Exporter<T> exporter) {
            this.exporter = exporter;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        @Override
        public void unexport() {
            exporter.unexport();
        }
    }

    /**
     * Reexport: the exporter destroy problem in protocol
     * 1.Ensure that the exporter returned by registryprotocol can be normal destroyed
     * 2.No need to re-register to the registry after notify
     * 3.The invoker passed by the export method , would better to be the invoker of exporter
     */
    // 当subscribeUrl对应的数据发生了改变，OverrideListener将收到通知
    private class OverrideListener implements NotifyListener {
        private final URL subscribeUrl;
        private final Invoker originInvoker;


        private List<Configurator> configurators;

        public OverrideListener(URL subscribeUrl, Invoker originalInvoker) {
            this.subscribeUrl = subscribeUrl;
            this.originInvoker = originalInvoker;
        }

        /**
         * @param urls The list of registered information, is always not empty, The meaning is the same as the
         *             return value of {@link org.apache.dubbo.registry.RegistryService#lookup(URL)}.
         */
        @Override
        public synchronized void notify(List<URL> urls) {
            LOGGER.debug("original override urls: " + urls);

            // 筛选当前监听器订阅的url
            List<URL> matchedUrls = getMatchedUrls(urls, subscribeUrl.addParameter(CATEGORY_KEY,
                    CONFIGURATORS_CATEGORY));
            LOGGER.debug("subscribe url: " + subscribeUrl + ", override urls: " + matchedUrls);

            // No matching results
            if (matchedUrls.isEmpty()) {
                return;
            }

            // 对发生了变化的url进行过滤，只取url是override协议，或者参数category等于configurators的url
            this.configurators = Configurator.toConfigurators(classifyUrls(matchedUrls, UrlUtils::isConfigurator))
                    .orElse(configurators);

            // 根据Override协议修改
            doOverrideIfNecessary();
        }

        public synchronized void doOverrideIfNecessary() {
            final Invoker<?> invoker;
            if (originInvoker instanceof InvokerDelegate) {
                invoker = ((InvokerDelegate<?>) originInvoker).getInvoker();
            } else {
                invoker = originInvoker;
            }
            //The origin invoker
            // 当前服务的原始服务提供者url
            URL originUrl = RegistryProtocol.this.getProviderUrl(invoker);
            String key = getCacheKey(originInvoker);
            ExporterChangeableWrapper<?> exporter = bounds.get(key);
            if (exporter == null) {
                LOGGER.warn(new IllegalStateException("error state, exporter should not be null"));
                return;
            }
            //The current, may have been merged many times
            Invoker<?> exporterInvoker = exporter.getInvoker();
            // 当前服务被导出的url
            URL currentUrl = exporterInvoker == null ? null : exporterInvoker.getUrl();
            //Merged with this configuration
            // 根据configurators修改url，configurators是全量的，并不是某个新增的或删除的，所以是基于原始的url进行修改，并不是基于currentUrl
            URL newUrl = getConfiguredInvokerUrl(configurators, originUrl);
            // 根据providerConfigurationListener中的Configurators重写服务URL
            newUrl = getConfiguredInvokerUrl(providerConfigurationListener.getConfigurators(), newUrl);
            // 根据serviceConfigurationListeners中对应的服务的Configurators重写服务URL
            newUrl = getConfiguredInvokerUrl(serviceConfigurationListeners.get(originUrl.getServiceKey())
                    .getConfigurators(), newUrl);

            // 修改过的url如果和目前的url不相同，则重新按newUrl导出
            if (!newUrl.equals(currentUrl)) {
                if (newUrl.getParameter(Constants.NEED_REEXPORT, true)) {
                    RegistryProtocol.this.reExport(originInvoker, newUrl);
                }
                LOGGER.info("exported provider url changed, origin url: " + originUrl +
                        ", old export url: " + currentUrl + ", new export url: " + newUrl);
            }
        }

        private List<URL> getMatchedUrls(List<URL> configuratorUrls, URL currentSubscribe) {
            List<URL> result = new ArrayList<URL>();
            for (URL url : configuratorUrls) {
                URL overrideUrl = url;
                // Compatible with the old version
                if (url.getParameter(CATEGORY_KEY) == null && OVERRIDE_PROTOCOL.equals(url.getProtocol())) {
                    overrideUrl = url.addParameter(CATEGORY_KEY, CONFIGURATORS_CATEGORY);
                }

                // Check whether url is to be applied to the current service
                if (UrlUtils.isMatch(currentSubscribe, overrideUrl)) {
                    result.add(url);
                }
            }
            return result;
        }
    }

    private class ServiceConfigurationListener extends AbstractConfiguratorListener {
        private URL providerUrl;
        private OverrideListener notifyListener;

        public ServiceConfigurationListener(URL providerUrl, OverrideListener notifyListener) {
            this.providerUrl = providerUrl;
            this.notifyListener = notifyListener;
            // 订阅 服务接口名+":"+group+":"+version+".configurators"
            // /dubbo/config/dubbo/org.apache.dubbo.demo.DemoService::.configurators
            this.initWith(DynamicConfiguration.getRuleKey(providerUrl) + CONFIGURATORS_SUFFIX);
        }

        private <T> URL overrideUrl(URL providerUrl) {
            return RegistryProtocol.getConfiguredInvokerUrl(configurators, providerUrl);
        }

        @Override
        protected void notifyOverrides() {
            // 当ServiceConfigurationListener监听数据发生了变化时，就会通知OverrideListener。
            notifyListener.doOverrideIfNecessary();
        }
    }

    private class ProviderConfigurationListener extends AbstractConfiguratorListener {

        public ProviderConfigurationListener() {
            // 订阅 应用名+".configurators"
            // /dubbo/config/dubbo/dubbo-demo-annotation-provider.configurators
            this.initWith(ApplicationModel.getApplication() + CONFIGURATORS_SUFFIX);
        }

        /**
         * Get existing configuration rule and override provider url before exporting.
         *
         * @param providerUrl
         * @param <T>
         * @return
         */
        private <T> URL overrideUrl(URL providerUrl) {
            // 通过configurators去修改/装配providerUrl
            return RegistryProtocol.getConfiguredInvokerUrl(configurators, providerUrl);
        }

        @Override
        protected void notifyOverrides() {
            overrideListeners.values().forEach(listener -> ((OverrideListener) listener).doOverrideIfNecessary());
        }
    }

    /**
     * exporter proxy, establish the corresponding relationship between the returned exporter and the exporter
     * exported by the protocol, and can modify the relationship at the time of override.
     *
     * @param <T>
     */
    private class ExporterChangeableWrapper<T> implements Exporter<T> {

        private final ExecutorService executor = newSingleThreadExecutor(new NamedThreadFactory("Exporter-Unexport", true));

        private final Invoker<T> originInvoker;
        private Exporter<T> exporter;
        private URL subscribeUrl;
        private URL registerUrl;

        private AtomicBoolean unexported = new AtomicBoolean(false);

        public ExporterChangeableWrapper(Exporter<T> exporter, Invoker<T> originInvoker) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
        }

        public Invoker<T> getOriginInvoker() {
            return originInvoker;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        public void setExporter(Exporter<T> exporter) {
            this.exporter = exporter;
        }

        @Override
        public void unexport() {
            if (!unexported.compareAndSet(false, true)) {
                return;
            }

            String key = getCacheKey(this.originInvoker);
            bounds.remove(key);

            // 从注册中心删除服务URL
            Registry registry = RegistryProtocol.this.getRegistry(originInvoker);
            try {
                registry.unregister(registerUrl);
            } catch (Throwable t) {
                LOGGER.warn(t.getMessage(), t);
            }
            try {
                // 解绑当前服务的Listener
                NotifyListener listener = RegistryProtocol.this.overrideListeners.remove(subscribeUrl);
                registry.unsubscribe(subscribeUrl, listener);
                ExtensionLoader.getExtensionLoader(GovernanceRuleRepository.class).getDefaultExtension()
                        .removeListener(subscribeUrl.getServiceKey() + CONFIGURATORS_SUFFIX,
                                serviceConfigurationListeners.get(subscribeUrl.getServiceKey()));
            } catch (Throwable t) {
                LOGGER.warn(t.getMessage(), t);
            }

            executor.submit(() -> {
                try {
                    int timeout = ConfigurationUtils.getServerShutdownTimeout();
                    if (timeout > 0) {
                        LOGGER.info("Waiting " + timeout + "ms for registry to notify all consumers before unexport. " +
                                "Usually, this is called when you use dubbo API");
                        Thread.sleep(timeout);
                    }
                    exporter.unexport();
                } catch (Throwable t) {
                    LOGGER.warn(t.getMessage(), t);
                }
            });
        }

        public void setSubscribeUrl(URL subscribeUrl) {
            this.subscribeUrl = subscribeUrl;
        }

        public void setRegisterUrl(URL registerUrl) {
            this.registerUrl = registerUrl;
        }

        public URL getRegisterUrl() {
            return registerUrl;
        }
    }

    // for unit test
    private static RegistryProtocol INSTANCE;

    // for unit test
    public RegistryProtocol() {
        INSTANCE = this;
    }

    // for unit test
    public static RegistryProtocol getRegistryProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(REGISTRY_PROTOCOL); // load
        }
        return INSTANCE;
    }
}

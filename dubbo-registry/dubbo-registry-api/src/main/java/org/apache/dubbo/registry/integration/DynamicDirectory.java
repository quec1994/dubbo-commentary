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
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.registry.client.migration.InvokersChangedListener;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.Constants;
import org.apache.dubbo.rpc.cluster.RouterChain;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.directory.AbstractDirectory;

import java.util.Collections;
import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MONITOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONSUMERS_CATEGORY;
import static org.apache.dubbo.registry.Constants.REGISTER_IP_KEY;
import static org.apache.dubbo.registry.Constants.REGISTER_KEY;
import static org.apache.dubbo.registry.Constants.SIMPLIFIED_KEY;
import static org.apache.dubbo.registry.integration.RegistryProtocol.DEFAULT_REGISTER_CONSUMER_KEYS;
import static org.apache.dubbo.remoting.Constants.CHECK_KEY;


/**
 * RegistryDirectory
 */
public abstract class DynamicDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final Logger logger = LoggerFactory.getLogger(DynamicDirectory.class);

    protected static final Cluster CLUSTER = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    protected static final RouterFactory ROUTER_FACTORY = ExtensionLoader.getExtensionLoader(RouterFactory.class)
            .getAdaptiveExtension();

    protected final String serviceKey; // Initialization at construction time, assertion not null
    protected final Class<T> serviceType; // Initialization at construction time, assertion not null
    // 原始的directoryUrl，即consumerUrl，构造时初始化，断言不为null，并且总是分配非null值
    protected final URL directoryUrl; // Initialization at construction time, assertion not null, and always assign non null value
    protected final boolean multiGroup;
    protected Protocol protocol; // Initialization at the time of injection, the assertion is not null
    protected Registry registry; // Initialization at the time of injection, the assertion is not null
    protected volatile boolean forbidden = false;
    protected boolean shouldRegister;
    protected boolean shouldSimplified;

    // 合并动态配置后的directoryUrl，即consumerUrl，构造时初始化，断言不为null，并且总是分配非null值
    protected volatile URL overrideDirectoryUrl; // Initialization at construction time, assertion not null, and always assign non null value

    protected volatile URL registeredConsumerUrl;

    /**
     * override rules
     * Priority: override>-D>consumer>provider
     * Rule one: for a certain provider <ip:port,timeout=100>
     * Rule two: for all providers <* ,timeout=5000>
     */
    // 旧版动态配置（dubbo 2.6及之前的版本）
    protected volatile List<Configurator> configurators; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    protected volatile List<Invoker<T>> invokers;
    // Set<invokerUrls> cache invokeUrls to invokers mapping.

    protected ServiceInstancesChangedListener serviceListener;

    /**
     * Should continue route if directory is empty
     */
    private final boolean shouldFailFast;

    public DynamicDirectory(Class<T> serviceType, URL url) {
        super(url, true);

        if (serviceType == null) {
            throw new IllegalArgumentException("service type is null.");
        }

        if (url.getServiceKey() == null || url.getServiceKey().length() == 0) {
            throw new IllegalArgumentException("registry serviceKey is null.");
        }

        this.shouldRegister = !ANY_VALUE.equals(url.getServiceInterface()) && url.getParameter(REGISTER_KEY, true);
        this.shouldSimplified = url.getParameter(SIMPLIFIED_KEY, false);

        this.serviceType = serviceType;
        this.serviceKey = super.getConsumerUrl().getServiceKey();

        // 转换registryUrl到consumerUrl
        this.overrideDirectoryUrl = this.directoryUrl = turnRegistryUrlToConsumerUrl(url);
        // zookeeper://127.0.0.1:2181/org.apache.dubbo.registry.RegistryService?application=dubbo-demo-annotation-consumer&dubbo=2.0.2&id=org.apache.dubbo.config.RegistryConfig#0&pid=16256&refer=application%3Ddubbo-demo-annotation-consumer%26dubbo%3D2.0.2%26init%3Dfalse%26interface%3Dorg.apache.dubbo.demo.DemoService%26methods%3DsayHello%2CsayHelloAsync%26pid%3D16256%26register.ip%3D192.168.56.1%26side%3Dconsumer%26sticky%3Dfalse%26timestamp%3D1691050126046&timestamp=1691050126114

        String group = directoryUrl.getParameter(GROUP_KEY, "");
        this.multiGroup = group != null && (ANY_VALUE.equals(group) || group.contains(","));
        // zk上的dubbo.router.should-fail-fast节点内容
        this.shouldFailFast = Boolean.parseBoolean(ConfigurationUtils.getProperty(Constants.SHOULD_FAIL_FAST_KEY, "true"));
    }

    @Override
    public void addServiceListener(ServiceInstancesChangedListener instanceListener) {
        this.serviceListener = instanceListener;
    }

    private URL turnRegistryUrlToConsumerUrl(URL url) {
        return URLBuilder.from(url)
                .setHost(queryMap.get(REGISTER_IP_KEY) == null ? url.getHost() : queryMap.get(REGISTER_IP_KEY))
                .setPort(0)
                .setProtocol(queryMap.get(PROTOCOL_KEY) == null ? DUBBO : queryMap.get(PROTOCOL_KEY))
                .setPath(queryMap.get(INTERFACE_KEY))
                .clearParameters()
                .addParameters(queryMap)
                .removeParameter(MONITOR_KEY)
                .addMethodParameters(URL.toMethodParameters(queryMap)) // reset method parameters
                .build();
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public Registry getRegistry() {
        return registry;
    }

    public boolean isShouldRegister() {
        return shouldRegister;
    }

    public void subscribe(URL url) {
        setConsumerUrl(url);
        registry.subscribe(url, this);
    }

    public void unSubscribe(URL url) {
        setConsumerUrl(null);
        registry.unsubscribe(url, this);
    }

    @Override
    public List<Invoker<T>> doList(Invocation invocation) {
        if (forbidden && shouldFailFast) {
            // 1. No service provider 2. Service providers are disabled
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION, "No provider available from registry " +
                    getUrl().getAddress() + " for service " + getConsumerUrl().getServiceKey() + " on consumer " +
                    NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() +
                    ", please check status of providers(disabled, not registered or in blacklist).");
        }

        if (multiGroup) {
            return this.invokers == null ? Collections.emptyList() : this.invokers;
        }

        List<Invoker<T>> invokers = null;
        try {
            // Get invokers from cache, only runtime routers will be executed.
            // 调用路由链筛选出适合的服务Invoker
            invokers = routerChain.route(getConsumerUrl(), invocation);
        } catch (Throwable t) {
            logger.error("Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(), t);
        }

        return invokers == null ? Collections.emptyList() : invokers;
    }

    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    @Override
    public List<Invoker<T>> getAllInvokers() {
        return invokers;
    }

    @Override
    public URL getConsumerUrl() {
        return this.overrideDirectoryUrl;
    }

    public URL getRegisteredConsumerUrl() {
        return registeredConsumerUrl;
    }

    public void setRegisteredConsumerUrl(URL url) {
        if (!shouldSimplified) {
            this.registeredConsumerUrl = url.addParameters(CATEGORY_KEY, CONSUMERS_CATEGORY, CHECK_KEY,
                    String.valueOf(false));
        } else {
            this.registeredConsumerUrl = URL.valueOf(url, DEFAULT_REGISTER_CONSUMER_KEYS, null).addParameters(
                    CATEGORY_KEY, CONSUMERS_CATEGORY, CHECK_KEY, String.valueOf(false));
        }
    }

    public void buildRouterChain(URL url) {
        this.setRouterChain(RouterChain.buildChain(url));
        // routers
        // 0 = {MockInvokersSelector@3930}  // mock路由
        // 1 = {TagRouter@3931}             // 标签路由
        // 2 = {ServiceRouter@3932}         // 服务条件路由
        // 3 = {AppRouter@3933}             // 应用条件路由
    }

    public List<Invoker<T>> getInvokers() {
        return invokers;
    }

    @Override
    public void destroy() {
        if (isDestroyed()) {
            return;
        }

        // unregister.
        try {
            if (getRegisteredConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unregister(getRegisteredConsumerUrl());
            }
        } catch (Throwable t) {
            logger.warn("unexpected error when unregister service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        // unsubscribe.
        try {
            if (getSubscribeConsumerurl() != null && registry != null && registry.isAvailable()) {
                // overwrite by child, so need call function
                unSubscribe(getSubscribeConsumerurl());
            }
        } catch (Throwable t) {
            logger.warn("unexpected error when unsubscribe service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        super.destroy(); // must be executed after unsubscribing
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            logger.warn("Failed to destroy service " + serviceKey, t);
        }

        invokersChangedListener = null;
    }

    @Override
    public void discordAddresses() {
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            logger.warn("Failed to destroy service " + serviceKey, t);
        }
    }

    private volatile InvokersChangedListener invokersChangedListener;
    private volatile boolean addressChanged;

    public void setInvokersChangedListener(InvokersChangedListener listener) {
        this.invokersChangedListener = listener;
        if (addressChanged) {
            if (invokersChangedListener != null) {
                invokersChangedListener.onChange();
                this.addressChanged = false;
            }
        }
    }

    protected void invokersChanged() {
        if (invokersChangedListener != null) {
            invokersChangedListener.onChange();
            this.addressChanged = false;
        } else {
            this.addressChanged = true;
        }
    }

    protected abstract void destroyAllInvokers();
}

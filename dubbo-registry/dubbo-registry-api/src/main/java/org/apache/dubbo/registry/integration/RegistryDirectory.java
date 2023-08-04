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
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.AddressListener;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.DISABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.APP_DYNAMIC_CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.COMPATIBLE_CONFIG_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTE_PROTOCOL;
import static org.apache.dubbo.registry.Constants.CONFIGURATORS_SUFFIX;
import static org.apache.dubbo.rpc.cluster.Constants.ROUTER_KEY;


/**
 * RegistryDirectory
 */
public class RegistryDirectory<T> extends DynamicDirectory<T> {
    private static final Logger logger = LoggerFactory.getLogger(RegistryDirectory.class);

    private static final ConsumerConfigurationListener CONSUMER_CONFIGURATION_LISTENER = new ConsumerConfigurationListener();
    private ReferenceConfigurationListener referenceConfigurationListener;

    // Map<url, Invoker> cache service url to invoker mapping.
    // The initial value is null and the midway may be assigned to null, please use the local variable reference
    protected volatile Map<URL, Invoker<T>> urlInvokerMap;
    // The initial value is null and the midway may be assigned to null, please use the local variable reference
    protected volatile Set<URL> cachedInvokerUrls;

    public RegistryDirectory(Class<T> serviceType, URL url) {
        super(serviceType, url);
    }

    @Override
    public void subscribe(URL url) {
        setConsumerUrl(url);
        // 监听consumer应用动态配置变更
        // 动态配置目录：/dubbo/config/dubbo/dubbo-demo-annotation-consumer.configurators
        CONSUMER_CONFIGURATION_LISTENER.addNotifyListener(this);
        // 监听所引入的服务的动态配置变更
        // 动态配置目录：/dubbo/config/dubbo/org.apache.dubbo.demo.DemoService::.configurators
        referenceConfigurationListener = new ReferenceConfigurationListener(this, url);
        // ListenerRegistryWrapper 里持有 ZookeeperRegistry
        // ZookeeperRegistry extends FailbackRegistry extends AbstractRegistry
        // 因此这边会先调用 ListenerRegistryWrapper.subscribe，在这个方法里会调用 ZookeeperRegistry.subscribe
        // 但是ZookeeperRegistry没有覆写subscribe方法，因此这边调的是 FailbackRegistry.subscribe
        registry.subscribe(url, this);
    }

    @Override
    public void unSubscribe(URL url) {
        setConsumerUrl(null);
        CONSUMER_CONFIGURATION_LISTENER.removeNotifyListener(this);
        referenceConfigurationListener.stop();
        registry.unsubscribe(url, this);
    }

    @Override
    public synchronized void notify(List<URL> urls) {
        // 收到配置变更通知

        // 将url按类别分组
        Map<String, List<URL>> categoryUrls = urls.stream()
                .filter(Objects::nonNull)
                // 过滤掉不关心的
                .filter(this::isValidCategory)
                .filter(this::isNotCompatibleFor26x)
                // 判断类别
                .collect(Collectors.groupingBy(this::judgeCategory));

        // 动态配置变更（旧版）
        List<URL> configuratorURLs = categoryUrls.getOrDefault(CONFIGURATORS_CATEGORY, Collections.emptyList());
        // 把configurators属性赋值为最新的
        this.configurators = Configurator.toConfigurators(configuratorURLs).orElse(this.configurators);

        // 路由规则变更（旧版）
        List<URL> routerURLs = categoryUrls.getOrDefault(ROUTERS_CATEGORY, Collections.emptyList());
        // 为routerChain属性添加路由规则
        toRouters(routerURLs).ifPresent(this::addRouters);

        // providers
        // 服务提供者信息变更
        List<URL> providerURLs = categoryUrls.getOrDefault(PROVIDERS_CATEGORY, Collections.emptyList());
        /**
         * 3.x added for extend URL address
         */
        // 3.x添加，URL地址监听器扩展点，用于扩展URL地址
        ExtensionLoader<AddressListener> addressListenerExtensionLoader = ExtensionLoader.getExtensionLoader(AddressListener.class);
        List<AddressListener> supportedListeners = addressListenerExtensionLoader.getActivateExtension(getUrl(), (String[]) null);
        if (supportedListeners != null && !supportedListeners.isEmpty()) {
            for (AddressListener addressListener : supportedListeners) {
                providerURLs = addressListener.notify(providerURLs, getConsumerUrl(), this);
            }
        }
        // 更新动态配置和invoker
        refreshOverrideAndInvoker(providerURLs);
    }

    private String judgeCategory(URL url) {
        if (UrlUtils.isConfigurator(url)) {
            return CONFIGURATORS_CATEGORY;
        } else if (UrlUtils.isRoute(url)) {
            return ROUTERS_CATEGORY;
        } else if (UrlUtils.isProvider(url)) {
            return PROVIDERS_CATEGORY;
        }
        return "";
    }

    // RefreshOverrideAndInvoker will be executed by registryCenter and configCenter, so it should be synchronized.
    // refreshOverrideAndInvoker将由registryCenter和configCenter执行，因此应该加锁同步执行
    private synchronized void refreshOverrideAndInvoker(List<URL> urls) {
        // mock zookeeper://xxx?mock=return null

        // 根据最新动态配置更新overrideDirectoryUrl
        overrideDirectoryUrl();
        // 根据最新的服务提供者URL刷新invoker
        // urls —— providerURLs
        refreshInvoker(urls);
    }

    /**
     * Convert the invokerURL list to the Invoker Map. The rules of the conversion are as follows:
     * <ol>
     * <li> If URL has been converted to invoker, it is no longer re-referenced and obtained directly from the cache,
     * and notice that any parameter changes in the URL will be re-referenced.</li>
     * <li>If the incoming invoker list is not empty, it means that it is the latest invoker list.</li>
     * <li>If the list of incoming invokerUrl is empty, It means that the rule is only a override rule or a route
     * rule, which needs to be re-contrasted to decide whether to re-reference.</li>
     * </ol>
     *
     * @param invokerUrls this parameter can't be null
     */
    private void refreshInvoker(List<URL> invokerUrls) {
        // 根据最新的服务提供者URL刷新invoker
        // invokerUrls —— 最新providerURLs（http://xxx:nn/xxx/xx?xx=xx、dubbo://xxx:nn/xxx/xx?xx=xx）

        Assert.notNull(invokerUrls, "invokerUrls should not be null");

        if (invokerUrls.size() == 1
                && invokerUrls.get(0) != null
                && EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {
            // 服务提供者全部下线了

            // 禁止访问
            this.forbidden = true; // Forbid to access
            this.invokers = Collections.emptyList();
            routerChain.setInvokers(this.invokers);
            destroyAllInvokers(); // Close all invokers
        } else {
            // 服务提供者信息变更

            // 保存旧的urlInvokerMap，以便重新生成后销毁invoker
            Map<URL, Invoker<T>> oldUrlInvokerMap = this.urlInvokerMap; // local reference
            if (invokerUrls == Collections.<URL>emptyList()) {
                invokerUrls = new ArrayList<>();
            }
            if (invokerUrls.isEmpty() && this.cachedInvokerUrls != null) {
                invokerUrls.addAll(this.cachedInvokerUrls);
            } else {
                this.cachedInvokerUrls = new HashSet<>();
                // 缓存invokerUrls，便于比较
                this.cachedInvokerUrls.addAll(invokerUrls);//Cached invoker urls, convenient for comparison
            }
            if (invokerUrls.isEmpty()) {
                return;
            }
            // 允许访问
            this.forbidden = false; // Allow to access
            // 将url转换为invoker，如果url已经被引用，则不会重新引用
            // 会先按Protocol进行过滤，并且调用DubboProtocol.refer方法得到DubboInvoker
            Map<URL, Invoker<T>> newUrlInvokerMap = toInvokers(invokerUrls);// Translate url list to Invoker map

            /**
             * If the calculation is wrong, it is not processed.
             * 如果计算错误，则不进行处理。
             *
             * 1. The protocol configured by the client is inconsistent with the protocol of the server.
             *    eg: consumer protocol = dubbo, provider only has other protocol services(rest).
             * 2. The registration center is not robust and pushes illegal specification data.
             * 1. 客户端配置的协议与服务器的协议不一致。
             *    例如：consumer protocol=dubbo，提供者只有其他协议服务（rest）。
             * 2. 注册中心不健全，并推送非法规范数据。
             */
            if (CollectionUtils.isEmptyMap(newUrlInvokerMap)) {
                logger.error(new IllegalStateException("urls to invokers error .invokerUrls.size :" + invokerUrls.size() + ", invoker.size :0. urls :" + invokerUrls
                        .toString()));
                return;
            }

            List<Invoker<T>> newInvokers = Collections.unmodifiableList(new ArrayList<>(newUrlInvokerMap.values()));
            // pre-route and build cache, notice that route cache should build on original Invoker list.
            // toMergeMethodInvokerMap() will wrap some invokers having different groups, those wrapped invokers not should be routed.
            // 得到了所引入的服务Invoker之后，把它们设置到路由链中去，在调用时使用，并且会调用TagRouter的notify方法
            routerChain.setInvokers(newInvokers);
            this.invokers = multiGroup ? toMergeInvokerList(newInvokers) : newInvokers;
            this.urlInvokerMap = newUrlInvokerMap;

            // Close the unused Invoker
            destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap);

        }

        // notify invokers refreshed
        this.invokersChanged();
    }

    private List<Invoker<T>> toMergeInvokerList(List<Invoker<T>> invokers) {
        List<Invoker<T>> mergedInvokers = new ArrayList<>();
        // group by invoker#url#group
        Map<String, List<Invoker<T>>> groupMap =
                invokers.stream().collect(Collectors.groupingBy(x -> x.getUrl().getParameter(GROUP_KEY, "")));

        if (groupMap.size() > 1) {
            for (List<Invoker<T>> groupList : groupMap.values()) {
                StaticDirectory<T> staticDirectory = new StaticDirectory<>(groupList);
                staticDirectory.buildRouterChain();
                mergedInvokers.add(CLUSTER.join(staticDirectory));
            }
        } else {
            mergedInvokers.addAll(invokers);
        }
        return mergedInvokers;
    }

    /**
     * @param urls
     * @return null : no routers ,do nothing
     * else :routers list
     */
    private Optional<List<Router>> toRouters(List<URL> urls) {
        // 将url转换成Router对象，归一化配置

        if (CollectionUtils.isEmpty(urls)) {
            return Optional.empty();
        }

        List<Router> routers = new ArrayList<>();
        for (URL url : urls) {
            if (EMPTY_PROTOCOL.equals(url.getProtocol())) {
                continue;
            }
            String routerType = url.getParameter(ROUTER_KEY);
            if (routerType != null && routerType.length() > 0) {
                // 将url里配置的路由规则类别放到协议
                url = url.setProtocol(routerType);
            }
            try {
                // 根据url的router参数选择不同的转换规则
                Router router = ROUTER_FACTORY.getRouter(url);
                if (!routers.contains(router)) {
                    routers.add(router);
                }
            } catch (Throwable t) {
                logger.error("convert router url to router error, url: " + url, t);
            }
        }

        return Optional.of(routers);
    }

    /**
     * Turn urls into invokers, and if url has been refer, will not re-reference.
     *
     * @param urls
     * @return invokers
     */
    private Map<URL, Invoker<T>> toInvokers(List<URL> urls) {
        // 方法返回值
        Map<URL, Invoker<T>> newUrlInvokerMap = new ConcurrentHashMap<>();
        if (CollectionUtils.isEmpty(urls)) {
            return newUrlInvokerMap;
        }
        Set<URL> keys = new HashSet<>();
        // ReferenceConfig 里的 protocol 属性
        String queryProtocols = this.queryMap.get(PROTOCOL_KEY);
        // 遍历当前服务所有的服务提供者URL过滤出符合的URL
        for (URL providerUrl : urls) {
            // If protocol is configured at the reference side, only the matching protocol is selected
            // 如果在引用端配置了协议，则只选择匹配的协议
            if (queryProtocols != null && queryProtocols.length() > 0) {
                boolean accept = false;
                String[] acceptProtocols = queryProtocols.split(",");
                // 当前消费者如果手动配置了Protocol，那么则进行匹配
                for (String acceptProtocol : acceptProtocols) {
                    if (providerUrl.getProtocol().equals(acceptProtocol)) {
                        accept = true;
                        break;
                    }
                }
                if (!accept) {
                    // 当前的providerUrl协议不符合，直接取下一个providerUrl
                    continue;
                }
            }
            if (EMPTY_PROTOCOL.equals(providerUrl.getProtocol())) {
                // 如果当前的providerUrl的协议是一个特殊值empty协议，直接取下一个providerUrl
                // 应该是providers下没有节点
                continue;
            }
            if (!ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(providerUrl.getProtocol())) {
                logger.error(new IllegalStateException("Unsupported protocol " + providerUrl.getProtocol() +
                        " in notified url: " + providerUrl + " from registry " + getUrl().getAddress() +
                        " to consumer " + NetUtils.getLocalHost() + ", supported protocol: " +
                        ExtensionLoader.getExtensionLoader(Protocol.class).getSupportedExtensions()));
                // 当前providerUrl的协议在应用中没有对应的扩展点
                continue;
            }
            // 合并url参数，取最新优先级最高的配置
            URL url = mergeUrl(providerUrl);

            if (keys.contains(url)) { // Repeated url
                // 重复的providerUrl
                continue;
            }
            // 缓存合并配置后的providerUrl，用于去重
            keys.add(url);
            // Cache key is url that does not merge with consumer side parameters, regardless of how the consumer combines parameters, if the server url changes, then refer again

            Map<URL, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
            Invoker<T> invoker = localUrlInvokerMap == null ? null : localUrlInvokerMap.get(url);
            if (invoker == null) { // Not in the cache, refer again
                // 如果当前服务提供者URL没有生成过Invoker
                try {
                    // url是否含有禁止生成invoker的标志参数，默认为可以生成
                    boolean enabled = true;
                    if (url.hasParameter(DISABLED_KEY)) {
                        enabled = !url.getParameter(DISABLED_KEY, false);
                    } else {
                        enabled = url.getParameter(ENABLED_KEY, true);
                    }
                    if (enabled) {
                        // Protocol接口有3个包装类，一个是ProtocolFilterWrapper、ProtocolListenerWrapper、QosProtocolWrapper，
                        // 所以实际上在调用refer方法时，会先经过这3个包装类的refer方法，
                        // ProtocolFilterWrapper 的refer方法中会添加过滤器链
                        // ProtocolListenerWrapper 的refer方法中会添加监听器
                        // QosProtocolWrapper 的refer方法在Registry协议下会启动qos服务器，其它协议不做处理

                        // 调用Protocol的refer方法得到一个Invoker
                        // 本来应该调用DubboProtocol类的refer方法，但是DubboProtocol类没有覆写refer方法，
                        // 所以这边调用的是AbstractProtocol类的refer方法

                        invoker = new InvokerDelegate<>(protocol.refer(serviceType, url), url, providerUrl);
                        // InvokerDelegate->chain(FilterNode)->ListenerInvokerWrapper->AsyncToSyncInvoker->DubboInvoker
                        // DubboInvoker.clients-->ReferenceCountExchangeClient->HeaderExchangeClient->NettyClient
                        // NettyClient.handler--->MultiMessageHandler->HeartbeatHandler->AllChannelHandler->DecodeHandler->HeaderExchangeHandler->DubboProtocol.requestHandler
                    }
                } catch (Throwable t) {
                    logger.error("Failed to refer invoker for interface:" + serviceType + ",url:(" + url + ")" + t.getMessage(), t);
                }
                if (invoker != null) { // Put new invoker in cache
                    newUrlInvokerMap.put(url, invoker);
                }
            } else {
                // 如果当前服务提供者URL生成过Invoker，使用之前生成的invoker，不重复生成
                newUrlInvokerMap.put(url, invoker);
            }
        }
        keys.clear();
        return newUrlInvokerMap;
    }

    /**
     * Merge url parameters. the order is: override > -D >Consumer > Provider
     *
     * @param providerUrl
     * @return
     */
    private URL mergeUrl(URL providerUrl) {
        // 合并url参数。顺序为：override（动态配置）>Consumer（xml/注解 配置，ReferenceConfig）>Provider（服务提供者端配置）

        // 使用Consumer（xml/注解 配置）覆盖 Provider（服务提供者端配置）
        providerUrl = ClusterUtils.mergeUrl(providerUrl, queryMap); // Merge the consumer side parameters

        // 使用动态配置更新url
        providerUrl = overrideWithConfigurator(providerUrl);

        // 不检查连接是否成功，始终创建Invoker
        providerUrl = providerUrl.addParameter(Constants.CHECK_KEY, String.valueOf(false)); // Do not check whether the connection is successful or not, always create Invoker!

        // fix issue#9922
        // 合并到overrideDirectoryUrl的providerUrl需要去掉tag参数
        Map<String, String> providerSideParameters = new HashMap<>(providerUrl.getParameters());
        providerSideParameters.remove(CommonConstants.TAG_KEY);
        // The combination of directoryUrl and override is at the end of notify, which can't be handled here
        // directoryUrl和override的合并位于notify的末尾，此处无法处理

        // 合并providerUrl的参数
        this.overrideDirectoryUrl = this.overrideDirectoryUrl.addParametersIfAbsent(providerSideParameters); // Merge the provider side parameters

        // 在dubbo协议下，如果providerUrl的path属性为空，将directoryUrl的interface参数值赋值给providerUrl的path属性
        // providerUrl的path属性是服务接口类全限定名
        if ((providerUrl.getPath() == null || providerUrl.getPath()
                .length() == 0) && DUBBO_PROTOCOL.equals(providerUrl.getProtocol())) { // Compatible version 1.0
            //fix by tony.chenl DUBBO-44
            String path = directoryUrl.getParameter(INTERFACE_KEY);
            if (path != null) {
                int i = path.indexOf('/');
                if (i >= 0) {
                    path = path.substring(i + 1);
                }
                i = path.lastIndexOf(':');
                if (i >= 0) {
                    path = path.substring(0, i);
                }
                providerUrl = providerUrl.setPath(path);
            }
        }
        return providerUrl;
    }

    private URL overrideWithConfigurator(URL providerUrl) {
        // override url with configurator from "override://" URL for dubbo 2.6 and before
        // 使用来自override协议的URL(“override://”)的configurator来覆盖url在dubbo 2.6及之前的版本中
        providerUrl = overrideWithConfigurators(this.configurators, providerUrl);

        // override url with configurator from configurator from "app-name.configurators"

        // 使用配置中心应用类型动态配置（APP_NAME.configurators）更新url
        providerUrl = overrideWithConfigurators(CONSUMER_CONFIGURATION_LISTENER.getConfigurators(), providerUrl);

        // override url with configurator from configurators from "service-name.configurators"

        // 使用配置中心服务类型动态配置（SERVICE_NAME.configurators）更新url
        if (referenceConfigurationListener != null) {
            providerUrl = overrideWithConfigurators(referenceConfigurationListener.getConfigurators(), providerUrl);
        }

        return providerUrl;
    }

    private URL overrideWithConfigurators(List<Configurator> configurators, URL url) {
        if (CollectionUtils.isNotEmpty(configurators)) {
            for (Configurator configurator : configurators) {
                url = configurator.configure(url);
            }
        }
        return url;
    }

    /**
     * Close all invokers
     */
    @Override
    protected void destroyAllInvokers() {
        Map<URL, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
        if (localUrlInvokerMap != null) {
            for (Invoker<T> invoker : new ArrayList<>(localUrlInvokerMap.values())) {
                try {
                    // 调用invoker的销毁方法
                    invoker.destroyAll();
                } catch (Throwable t) {
                    logger.warn("Failed to destroy service " + serviceKey + " to provider " + invoker.getUrl(), t);
                }
            }
            localUrlInvokerMap.clear();
        }
        invokers = null;
        cachedInvokerUrls = null;
    }

    /**
     * Check whether the invoker in the cache needs to be destroyed
     * If set attribute of url: refer.autodestroy=false, the invokers will only increase without decreasing,there may be a refer leak
     *
     * @param oldUrlInvokerMap
     * @param newUrlInvokerMap
     */
    private void destroyUnusedInvokers(Map<URL, Invoker<T>> oldUrlInvokerMap, Map<URL, Invoker<T>> newUrlInvokerMap) {
        if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
            destroyAllInvokers();
            return;
        }
        // check deleted invoker
        if (oldUrlInvokerMap != null) {
            for (URL key : oldUrlInvokerMap.keySet()) {
                if (null != key && !newUrlInvokerMap.containsKey(key)) {
                    Invoker<T> invoker = oldUrlInvokerMap.get(key);
                    if (invoker != null) {
                        try {
                            invoker.destroyAll();
                            if (logger.isDebugEnabled()) {
                                logger.debug("destroy invoker[" + invoker.getUrl() + "] success. ");
                            }
                        } catch (Exception e) {
                            logger.warn("destroy invoker[" + invoker.getUrl() + "] failed. " + e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        Map<URL, Invoker<T>> localUrlInvokerMap = urlInvokerMap;
        try {
            if (!forbidden && CollectionUtils.isNotEmptyMap(localUrlInvokerMap)
                    && localUrlInvokerMap.values().stream().anyMatch(Invoker::isAvailable)) {
                return true;
            }
        } catch (Throwable throwable) {
            return true;
        }
        return false;
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<URL, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    private boolean isValidCategory(URL url) {
        // 获取url参数里的类别，默认为providers
        String category = url.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
        if ((ROUTERS_CATEGORY.equals(category) || ROUTE_PROTOCOL.equals(url.getProtocol())) ||
                PROVIDERS_CATEGORY.equals(category) ||
                CONFIGURATORS_CATEGORY.equals(category) || DYNAMIC_CONFIGURATORS_CATEGORY.equals(category) ||
                APP_DYNAMIC_CONFIGURATORS_CATEGORY.equals(category)) {
            // url类别：routers、providers、configurators、dynamicconfigurators、appdynamicconfigurators
            // url协议：route
            return true;
        }
        logger.warn("Unsupported category " + category + " in notified url: " + url + " from registry " +
                getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost());
        return false;
    }

    private boolean isNotCompatibleFor26x(URL url) {
        return StringUtils.isEmpty(url.getParameter(COMPATIBLE_CONFIG_KEY));
    }

    private void overrideDirectoryUrl() {
        // merge override parameters
        // 合并动态配置信息，更新overrideDirectoryUrl

        // directoryUrl是原始的
        this.overrideDirectoryUrl = directoryUrl;
        // 使用动态配置（旧版）更新overrideDirectoryUrl
        List<Configurator> localConfigurators = this.configurators; // local reference
        doOverrideUrl(localConfigurators);
        // 使用全局动态配置（新版）更新overrideDirectoryUrl
        List<Configurator> localAppDynamicConfigurators = CONSUMER_CONFIGURATION_LISTENER.getConfigurators(); // local reference
        doOverrideUrl(localAppDynamicConfigurators);
        if (referenceConfigurationListener != null) {
            // 使用应用动态配置（新版）更新overrideDirectoryUrl
            List<Configurator> localDynamicConfigurators = referenceConfigurationListener.getConfigurators(); // local reference
            doOverrideUrl(localDynamicConfigurators);
        }
    }

    private void doOverrideUrl(List<Configurator> configurators) {
        if (CollectionUtils.isNotEmpty(configurators)) {
            for (Configurator configurator : configurators) {
                this.overrideDirectoryUrl = configurator.configure(overrideDirectoryUrl);
            }
        }
    }

    /**
     * The delegate class, which is mainly used to store the URL address sent by the registry,and can be reassembled on the basis of providerURL queryMap overrideMap for re-refer.
     *
     * @param <T>
     */
    private static class InvokerDelegate<T> extends InvokerWrapper<T> {
        private URL providerUrl;

        public InvokerDelegate(Invoker<T> invoker, URL url, URL providerUrl) {
            super(invoker, url);
            this.providerUrl = providerUrl;
        }

        public URL getProviderUrl() {
            return providerUrl;
        }
    }

    private static class ReferenceConfigurationListener extends AbstractConfiguratorListener {
        private RegistryDirectory directory;
        private URL url;

        ReferenceConfigurationListener(RegistryDirectory directory, URL url) {
            this.directory = directory;
            this.url = url;
            this.initWith(DynamicConfiguration.getRuleKey(url) + CONFIGURATORS_SUFFIX);
        }

        void stop() {
            this.stopListen(DynamicConfiguration.getRuleKey(url) + CONFIGURATORS_SUFFIX);
        }

        @Override
        protected void notifyOverrides() {
            // to notify configurator/router changes
            directory.refreshOverrideAndInvoker(Collections.emptyList());
        }
    }

    private static class ConsumerConfigurationListener extends AbstractConfiguratorListener {
        List<RegistryDirectory> listeners = new ArrayList<>();

        ConsumerConfigurationListener() {
            this.initWith(ApplicationModel.getApplication() + CONFIGURATORS_SUFFIX);
        }

        void addNotifyListener(RegistryDirectory listener) {
            this.listeners.add(listener);
        }

        void removeNotifyListener(RegistryDirectory listener) {
            this.listeners.remove(listener);
        }

        @Override
        protected void notifyOverrides() {
            listeners.forEach(listener -> listener.refreshOverrideAndInvoker(Collections.emptyList()));
        }
    }

}

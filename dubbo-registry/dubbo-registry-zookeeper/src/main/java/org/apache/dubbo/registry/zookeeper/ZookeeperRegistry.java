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
package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.URLStrParser;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.support.FailbackRegistry;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.zookeeper.ChildListener;
import org.apache.dubbo.remoting.zookeeper.StateListener;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.rpc.RpcException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_SEPARATOR_ENCODED;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONSUMERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTERS_CATEGORY;

/**
 * ZookeeperRegistry
 */
public class ZookeeperRegistry extends FailbackRegistry {

    private static final String DEFAULT_ROOT = "dubbo";

    private final String root;

    private final Set<String> anyServices = new ConcurrentHashSet<>();

    private final ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> zkListeners = new ConcurrentHashMap<>();

    private final ZookeeperClient zkClient;

    public ZookeeperRegistry(URL url, ZookeeperTransporter zookeeperTransporter) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        String group = url.getParameter(GROUP_KEY, DEFAULT_ROOT);
        if (!group.startsWith(PATH_SEPARATOR)) {
            group = PATH_SEPARATOR + group;
        }
        this.root = group;
        zkClient = zookeeperTransporter.connect(url);
        zkClient.addStateListener((state) -> {
            if (state == StateListener.RECONNECTED) {
                logger.warn("Trying to fetch the latest urls, in case there're provider changes during connection loss.\n" +
                        " Since ephemeral ZNode will not get deleted for a connection lose, " +
                        "there's no need to re-register url of this instance.");
                ZookeeperRegistry.this.fetchLatestAddresses();
            } else if (state == StateListener.NEW_SESSION_CREATED) {
                logger.warn("Trying to re-register urls and re-subscribe listeners of this instance to registry...");
                try {
                    ZookeeperRegistry.this.recover();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            } else if (state == StateListener.SESSION_LOST) {
                logger.warn("Url of this instance will be deleted from registry soon. " +
                        "Dubbo client will try to re-register once a new session is created.");
            } else if (state == StateListener.SUSPENDED) {

            } else if (state == StateListener.CONNECTED) {

            }
        });
    }

    @Override
    public boolean isAvailable() {
        return zkClient.isConnected();
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            zkClient.close();
        } catch (Exception e) {
            logger.warn("Failed to close zookeeper client " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doRegister(URL url) {
        try {
            // 创建一个zk节点
            // 是否是临时节点 —— url.parameter[dynamic]，默认为 true
            zkClient.create(toUrlPath(url), url.getParameter(DYNAMIC_KEY, true));
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doUnregister(URL url) {
        try {
            zkClient.delete(toUrlPath(url));
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doSubscribe(final URL url, final NotifyListener listener) {
        try {
            if (ANY_VALUE.equals(url.getServiceInterface())) {
                // 订阅所有服务

                // dubbo 根路径
                String root = toRootPath();
                // 根据监听路径去拿listeners，如果没有则生成
                ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.computeIfAbsent(url, k -> new ConcurrentHashMap<>());
                // (parentPath, currentChilds) -> {} 相当于是 new ChildListener(){ void childChanged(String path, List<String> children){} }
                ChildListener zkListener = listeners.computeIfAbsent(listener, k -> (parentPath, currentChilds) -> {
                    // parentPath —— zk上的根路径
                    // currentChilds —— 根路径下的子节点，约定为远程服务节点，currentChilds 是目前所有注册在注册中心的远程服务
                    for (String child : currentChilds) {
                        child = URL.decode(child);
                        if (!anyServices.contains(child)) {
                            anyServices.add(child);
                            // 单独订阅每个服务，有远程服务添加/减少
                            subscribe(url.setPath(child).addParameters(INTERFACE_KEY, child,
                                    Constants.CHECK_KEY, String.valueOf(false)), k);
                        }
                    }
                });
                // 创建zk上的根路径
                zkClient.create(root, false);
                // 添加真正跟zk相关的根路径子节点监听器
                // 根路径下的子节点约定为远程服务节点，services 是目前所有注册在注册中心的远程服务
                List<String> services = zkClient.addChildListener(root, zkListener);
                if (CollectionUtils.isNotEmpty(services)) {
                    for (String service : services) {
                        service = URL.decode(service);
                        anyServices.add(service);
                        // 单独订阅每个服务，初始化订阅
                        subscribe(url.setPath(service).addParameters(INTERFACE_KEY, service,
                                Constants.CHECK_KEY, String.valueOf(false)), listener);
                    }
                }
            } else {
                // 单独订阅某一个服务

                // 控制监听器只有在主线程的同步通知完成后才运行
                CountDownLatch latch = new CountDownLatch(1);
                try {
                    List<URL> urls = new ArrayList<>();
                    // 生成要监听的zk上的路径
                    for (String path : toCategoriesPath(url)) {
                        // CategoriesPath
                        // 0 = "/dubbo/org.apache.dubbo.demo.DemoService/providers"
                        // 1 = "/dubbo/org.apache.dubbo.demo.DemoService/configurators"
                        // 2 = "/dubbo/org.apache.dubbo.demo.DemoService/routers"

                        // 根据监听路径去拿listeners，如果没有则生成
                        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.computeIfAbsent(url, k -> new ConcurrentHashMap<>());
                        // 监听的是path路径下的子节点
                        // ChildListener中的逻辑就是监听到zk上数据发生了变化后会触发的逻辑
                        ChildListener zkListener = listeners.computeIfAbsent(listener, k -> new RegistryChildListenerImpl(url, k, latch));
                        if (zkListener instanceof RegistryChildListenerImpl) {
                            ((RegistryChildListenerImpl) zkListener).setLatch(latch);
                        }
                        // 创建zk上路径
                        zkClient.create(path, false);

                        // 监听当前所引入的服务的提供者目录：/dubbo/org.apache.dubbo.demo.DemoService/providers
                        // 监听当前所引入的服务的老版本动态配置目录：/dubbo/org.apache.dubbo.demo.DemoService/configurators
                        // 监听当前所引入的服务的老版本路由器目录：/dubbo/org.apache.dubbo.demo.DemoService/routers

                        // 添加真正跟zk相关的子节点监听器
                        // children是目前在路径下的子节点
                        List<String> children = zkClient.addChildListener(path, zkListener);
                        if (children != null) {
                            // 这里的urls就是从现在所引入的服务指定路径下查到的url，比如下面这个三个路径下和consumer匹配的节点
                            //    "/dubbo/org.apache.dubbo.demo.DemoService/providers"
                            //    "/dubbo/org.apache.dubbo.demo.DemoService/configurators"
                            //    "/dubbo/org.apache.dubbo.demo.DemoService/routers"
                            urls.addAll(toUrlsWithEmpty(url, path, children));
                        }
                    }
                    notify(url, listener, urls);
                } finally {
                    // tells the listener to run only after the sync notification of main thread finishes.
                    // 告诉监听器主线程的同步通知完成
                    latch.countDown();
                }
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
        if (listeners != null) {
            ChildListener zkListener = listeners.remove(listener);
            if (zkListener != null) {
                if (ANY_VALUE.equals(url.getServiceInterface())) {
                    String root = toRootPath();
                    zkClient.removeChildListener(root, zkListener);
                } else {
                    for (String path : toCategoriesPath(url)) {
                        zkClient.removeChildListener(path, zkListener);
                    }
                }
            }

            if (listeners.isEmpty()) {
                zkListeners.remove(url);
            }
        }
    }

    @Override
    public List<URL> lookup(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("lookup url == null");
        }
        try {
            List<String> providers = new ArrayList<>();
            for (String path : toCategoriesPath(url)) {
                List<String> children = zkClient.getChildren(path);
                if (children != null) {
                    providers.addAll(children);
                }
            }
            return toUrlsWithoutEmpty(url, providers);
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + url + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    private String toRootDir() {
        // root：url.parameter[group]，默认为 dubbo
        if (root.equals(PATH_SEPARATOR)) {
            return root;
        }
        return root + PATH_SEPARATOR;
    }

    private String toRootPath() {
        return root;
    }

    private String toServicePath(URL url) {
        // root + url.parameter[interface]

        String name = url.getServiceInterface();
        if (ANY_VALUE.equals(name)) {
            return toRootPath();
        }
        return toRootDir() + URL.encode(name);
        // /dubbo/org.apache.dubbo.demo.DemoService
    }

    private String[] toCategoriesPath(URL url) {
        // 从url category参数取出categories
        String[] categories;
        if (ANY_VALUE.equals(url.getParameter(CATEGORY_KEY))) {
            categories = new String[]{PROVIDERS_CATEGORY, CONSUMERS_CATEGORY, ROUTERS_CATEGORY, CONFIGURATORS_CATEGORY};
        } else {
            categories = url.getParameter(CATEGORY_KEY, new String[]{DEFAULT_CATEGORY});
        }
        // 根据categories中不同的值生成zk上对应的路径，用于监听
        String[] paths = new String[categories.length];
        for (int i = 0; i < categories.length; i++) {
            // providers，那表示要监听/dubbo/org.apache.dubbo.demo.DemoService/providers路径
            // consumers，那表示要监听/dubbo/org.apache.dubbo.demo.DemoService/consumers路径
            // routers，那表示要监听/dubbo/org.apache.dubbo.demo.DemoService/routers路径
            // configurators，那表示要监听/dubbo/org.apache.dubbo.demo.DemoService/configurators路径
            paths[i] = toServicePath(url) + PATH_SEPARATOR + categories[i];
        }
        return paths;
    }

    private String toCategoryPath(URL url) {
        return toServicePath(url) + PATH_SEPARATOR + url.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
        // /dubbo/org.apache.dubbo.demo.DemoService/providers
    }

    private String toUrlPath(URL url) {
        return toCategoryPath(url) + PATH_SEPARATOR + URL.encode(url.toFullString());
        // /dubbo/org.apache.dubbo.demo.DemoService/providers/dubbo%3A%2F%2F192.168.56.1%3A20880%2Forg.apache.dubbo.demo.DemoService%3Fanyhost%3Dtrue%26application%3Ddubbo-demo-annotation-provider%26deprecated%3Dfalse%26dubbo%3D2.0.2%26dynamic%3Dtrue%26generic%3Dfalse%26interface%3Dorg.apache.dubbo.demo.DemoService%26methods%3DsayHello%2CsayHelloAsync%26pid%3D14092%26release%3D%26service.name%3DServiceBean%3A%2Forg.apache.dubbo.demo.DemoService%26side%3Dprovider%26timeout%3D6000%26timestamp%3D1690963107882
    }

    private List<URL> toUrlsWithoutEmpty(URL consumer, List<String> providers) {
        List<URL> urls = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(providers)) {
            for (String provider : providers) {
                if (provider.contains(PROTOCOL_SEPARATOR_ENCODED)) {
                    // provider是一个url
                    URL url = URLStrParser.parseEncodedStr(provider);
                    // 筛选出与consumer匹配的provider
                    if (UrlUtils.isMatch(consumer, url)) {
                        urls.add(url);
                    }
                }
            }
        }
        return urls;
    }

    private List<URL> toUrlsWithEmpty(URL consumer, String path, List<String> providers) {
        // consumer表示在消费者端配置的某个服务url，
        // path表示某个服务在zk上的配置数据存储路径，
        // providers表示该服务path路径下在的节点，
        // 如果path是/dubbo/org.apache.dubbo.demo.DemoService/providers的话，那providers就是服务提供者注册到注册中心的URL，每个URL代表一个服务提供者

        // 筛选出与consumer匹配的providerURL
        List<URL> urls = toUrlsWithoutEmpty(consumer, providers);
        // 过滤之后为空，则添加一个 empty:// 到urls中，并返回
        if (CollectionUtils.isEmpty(urls)) {
            int i = path.lastIndexOf(PATH_SEPARATOR);
            String category = i < 0 ? path : path.substring(i + 1);
            URL empty = URLBuilder.from(consumer)
                    .setProtocol(EMPTY_PROTOCOL)
                    .addParameter(CATEGORY_KEY, category)
                    .build();
            // empty://192.168.56.1/org.apache.dubbo.demo.DemoService?application=dubbo-demo-annotation-consumer&category=providers&dubbo=2.0.2&init=false&interface=org.apache.dubbo.demo.DemoService&methods=sayHello,sayHelloAsync&pid=16352&side=consumer&sticky=false&timestamp=1690904345587
            urls.add(empty);
        }
        // 所以最后返回的urls，要么只有一个 empty:// 协议，要么就是有一个或多个providerURL（比如：dubbo://）
        return urls;
    }

    /**
     * When zookeeper connection recovered from a connection loss, it need to fetch the latest provider list.
     * re-register watcher is only a side effect and is not mandate.
     */
    private void fetchLatestAddresses() {
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Fetching the latest urls of " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    removeFailedSubscribed(url, listener);
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

    private class RegistryChildListenerImpl implements ChildListener {

        private URL url;

        private NotifyListener listener;

        private volatile CountDownLatch latch;

        RegistryChildListenerImpl(URL url, NotifyListener listener, CountDownLatch latch) {
            this.url = url;
            this.listener = listener;
            // 防止启动的时候出现数据变更
            this.latch = latch;
        }

        void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void childChanged(String path, List<String> children) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.warn("Zookeeper children listener thread was interrupted unexpectedly, may cause race condition with the main thread.");
            }
            ZookeeperRegistry.this.notify(url, listener, toUrlsWithEmpty(url, path, children));
        }
    }

}

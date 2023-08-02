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
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.client.ServiceDiscoveryRegistryDirectory;
import org.apache.dubbo.registry.client.migration.MigrationInvoker;
import org.apache.dubbo.registry.support.AbstractRegistryFactory;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;

import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_PROTOCOL;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY;

/**
 * RegistryProtocol
 */
public class InterfaceCompatibleRegistryProtocol extends RegistryProtocol {

    private static final Logger logger = LoggerFactory.getLogger(InterfaceCompatibleRegistryProtocol.class);

    @Override
    protected URL getRegistryUrl(Invoker<?> originInvoker) {
        // 从registry://的url中的registry参数获取真正的注册中心连接协议，比如 zookeeper，
        // 如果registry参数没有值，则默认为dubbo，dubbo提供了自带的注册中心实现，
        // setProtocol方法会新建一个URL

        URL registryUrl = originInvoker.getUrl();
        if (REGISTRY_PROTOCOL.equals(registryUrl.getProtocol())) {
            String protocol = registryUrl.getParameter(REGISTRY_KEY, DEFAULT_REGISTRY);
            // 将registry://xxx?xx=xx&registry=zookeeper 转为 zookeeper://xxx?xx=xx
            registryUrl = registryUrl.setProtocol(protocol).removeParameter(REGISTRY_KEY);
        }
        return registryUrl;
    }

    @Override
    protected URL getRegistryUrl(URL url) {
        // 从registry://的url中的registry参数获取真正的注册中心连接协议，比如 zookeeper，
        // 如果registry参数没有值，则默认为dubbo，dubbo提供了自带的注册中心实现
        // setProtocol方法会新建一个URL

        // 将registry://xxx?xx=xx&registry=zookeeper 转为 zookeeper://xxx?xx=xx
        return URLBuilder.from(url)
                .setProtocol(url.getParameter(REGISTRY_KEY, DEFAULT_REGISTRY))
                .removeParameter(REGISTRY_KEY)
                .build();
    }

    @Override
    public <T> ClusterInvoker<T> getInvoker(Cluster cluster, Registry registry, Class<T> type, URL url) {
        // RegistryDirectory表示动态服务目录，会和注册中心的数据保持同步
        // type表示一个服务对应一个RegistryDirectory，url表示注册中心地址
        // 在消费端，最核心的就是RegistryDirectory
        DynamicDirectory<T> directory = new RegistryDirectory<>(type, url);
        return doCreateInvoker(directory, cluster, registry, type);
    }

    @Override
    public <T> ClusterInvoker<T> getServiceDiscoveryInvoker(Cluster cluster, Registry registry, Class<T> type, URL url) {
        try {
            registry = registryFactory.getRegistry(super.getRegistryUrl(url));
        } catch (IllegalStateException e) {
            String protocol = url.getProtocol();
            logger.warn(protocol + " do not support service discovery, automatically switch to interface-level service discovery.");
            registry = AbstractRegistryFactory.getDefaultNopRegistryIfNotSupportServiceDiscovery();
        }

        DynamicDirectory<T> directory = new ServiceDiscoveryRegistryDirectory<>(type, url);
        return doCreateInvoker(directory, cluster, registry, type);
    }

    @Override
    protected <T> ClusterInvoker<T> getMigrationInvoker(RegistryProtocol registryProtocol, Cluster cluster, Registry registry,
                                                        Class<T> type, URL url, URL consumerUrl) {
//        ClusterInvoker<T> invoker = getInvoker(cluster, registry, type, url);
        return new MigrationInvoker<T>(registryProtocol, cluster, registry, type, url, consumerUrl);
    }

}

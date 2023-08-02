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
package org.apache.dubbo.rpc.protocol.injvm;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.AbstractProtocol;
import org.apache.dubbo.rpc.protocol.DelegateExporterMap;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import static org.apache.dubbo.common.constants.CommonConstants.BROADCAST_CLUSTER;
import static org.apache.dubbo.common.constants.CommonConstants.CLUSTER_KEY;
import static org.apache.dubbo.rpc.Constants.SCOPE_KEY;
import static org.apache.dubbo.rpc.Constants.SCOPE_LOCAL;
import static org.apache.dubbo.rpc.Constants.SCOPE_REMOTE;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_PROTOCOL;

/**
 * InjvmProtocol
 */
public class InjvmProtocol extends AbstractProtocol implements Protocol{

    public static final String NAME = LOCAL_PROTOCOL;

    public static final int DEFAULT_PORT = 0;
    private static InjvmProtocol INSTANCE;

    public InjvmProtocol() {
        INSTANCE = this;
    }

    public static InjvmProtocol getInjvmProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(InjvmProtocol.NAME); // load
        }
        return INSTANCE;
    }

    static Exporter<?> getExporter(DelegateExporterMap delegateExporterMap, URL key) {
        Exporter<?> result = null;

        if (!key.getServiceKey().contains("*")) {
            result = delegateExporterMap.getExport(key.getServiceKey());
        } else {
            if (!delegateExporterMap.isEmpty()) {
                for (Exporter<?> exporter : delegateExporterMap.getExporters()) {
                    if (UrlUtils.isServiceKeyMatch(key, exporter.getInvoker().getUrl())) {
                        result = exporter;
                        break;
                    }
                }
            }
        }

        if (result == null) {
            return null;
        } else if (ProtocolUtils.isGeneric(
                result.getInvoker().getUrl().getParameter(GENERIC_KEY))) {
            return null;
        } else {
            return result;
        }
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        String serviceKey = invoker.getUrl().getServiceKey();
        InjvmExporter<T> tInjvmExporter = new InjvmExporter<>(invoker, serviceKey, exporterMap);
        exporterMap.addExportMap(serviceKey, tInjvmExporter);
        return tInjvmExporter;
    }

    @Override
    public <T> Invoker<T> protocolBindingRefer(Class<T> serviceType, URL url) throws RpcException {
        return new InjvmInvoker<T>(serviceType, url, url.getServiceKey(), exporterMap);
    }

    public boolean isInjvmRefer(URL url) {
        // 根据 url 参数值判断是否使用在本地JVM中的服务

        String scope = url.getParameter(SCOPE_KEY);
        // Since injvm protocol is configured explicitly, we don't need to set any extra flag, use normal refer process.
        // 由于injvm协议是显式配置的，所以我们不需要设置任何额外的标志，使用正常的引用过程。
        if (SCOPE_LOCAL.equals(scope) || (url.getParameter(LOCAL_PROTOCOL, false))) {
            // if it's declared as local reference
            // 'scope=local' is equivalent to 'injvm=true', injvm will be deprecated in the future release
            // 如果它被声明为本地引用
            // “scope=local”等同于“injvm=true”，injvm将在未来版本中弃用
            return true;
        } else if (SCOPE_REMOTE.equals(scope)) {
            // it's declared as remote reference
            // 它被声明为远程引用
            return false;
        } else if (url.getParameter(GENERIC_KEY, false)) {
            // generic invocation is not local reference
            // 泛型调用不是本地引用
            return false;
        } else if (getExporter(exporterMap, url) != null) {
            // Broadcast cluster means that multiple machines will be called,
            // which is not converted to injvm protocol at this time.
            // 广播集群意味着将调用多个机器，
            // 目前尚未转换为injvm协议。
            if (BROADCAST_CLUSTER.equalsIgnoreCase(url.getParameter(CLUSTER_KEY))) {
                return false;
            }
            // 没有指定url的scope参数值，但在同一JVM中提供了目标服务，则更倾向于进行本地调用，这是默认行为

            // by default, go through local reference if there's the service exposed locally
            // 默认情况下，如果服务在本地有暴露，则通过本地引用
            return true;
        } else {
            return false;
        }
    }
}

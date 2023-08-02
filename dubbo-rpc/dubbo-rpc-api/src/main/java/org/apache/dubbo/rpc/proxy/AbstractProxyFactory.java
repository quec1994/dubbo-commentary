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
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.service.Destroyable;
import org.apache.dubbo.rpc.service.EchoService;
import org.apache.dubbo.rpc.service.GenericService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.rpc.Constants.INTERFACES;

/**
 * AbstractProxyFactory
 */
public abstract class AbstractProxyFactory implements ProxyFactory {
    private static final Class<?>[] INTERNAL_INTERFACES = new Class<?>[]{
            EchoService.class, Destroyable.class
    };

    @Override
    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        return getProxy(invoker, false);
    }

    @Override
    public <T> T getProxy(Invoker<T> invoker, boolean generic) throws RpcException {
        Set<Class<?>> interfaces = new HashSet<>();

        // 获取 invoker.url.parameter[interfaces]，看git提交记录应该是用来兼容2.0.8之前版本RMI。
        String config = invoker.getUrl().getParameter(INTERFACES);
        if (config != null && config.length() > 0) {
            // 使用逗号切分
            String[] types = COMMA_SPLIT_PATTERN.split(config);
            for (String type : types) {
                // TODO can we load successfully for a different classloader?.
                interfaces.add(ReflectUtils.forName(type));
            }
        }

        if (generic) {
            if (GenericService.class.equals(invoker.getInterface()) || !GenericService.class.isAssignableFrom(invoker.getInterface())) {
                // 添加泛化调用的统一接口类 Class
                interfaces.add(com.alibaba.dubbo.rpc.service.GenericService.class);
            }

            try {
                // find the real interface from url
                String realInterface = invoker.getUrl().getParameter(Constants.INTERFACE);
                // 添加泛化调用的服务接口类 Class
                interfaces.add(ReflectUtils.forName(realInterface));
            } catch (Throwable e) {
                // ignore
            }
        }

        // 添加服务接口类 Class
        interfaces.add(invoker.getInterface());
        // 添加 EchoService.class, Destroyable.class
        interfaces.addAll(Arrays.asList(INTERNAL_INTERFACES));

        // interfaces = {HashSet@3931}  size = 3
        // 0 = {Class@3937} "interface org.apache.dubbo.rpc.service.Destroyable"
        // 1 = {Class@2011} "interface org.apache.dubbo.demo.DemoService"
        // 2 = {Class@2377} "interface org.apache.dubbo.rpc.service.EchoService"
        return getProxy(invoker, interfaces.toArray(new Class<?>[0]));
    }

    public abstract <T> T getProxy(Invoker<T> invoker, Class<?>[] types);

}

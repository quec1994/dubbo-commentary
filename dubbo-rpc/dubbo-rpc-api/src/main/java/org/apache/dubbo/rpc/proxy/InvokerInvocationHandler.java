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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ConsumerModel;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * InvokerHandler
 */
public class InvokerInvocationHandler implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(InvokerInvocationHandler.class);
    private final Invoker<?> invoker;
    private ConsumerModel consumerModel;
    private URL url;
    private String protocolServiceKey;

    public static Field stackTraceField;

    static {
        try {
            stackTraceField = Throwable.class.getDeclaredField("stackTrace");
            ReflectUtils.makeAccessible(stackTraceField);
        } catch (NoSuchFieldException e) {
            // ignore
        }
    }

    public InvokerInvocationHandler(Invoker<?> handler) {
        this.invoker = handler;
        this.url = invoker.getUrl();
        String serviceKey = this.url.getServiceKey();
        this.protocolServiceKey = this.url.getProtocolServiceKey();
        if (serviceKey != null) {
            this.consumerModel = ApplicationModel.getConsumerModel(serviceKey);
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        /*
         * proxy：代理类对象实例，在这个方法中没有用上，在这个方法中使用invoker来执行方法
         * method：被代理的方法
         * args：执行代理方法时的传参
         */
        if (method.getDeclaringClass() == Object.class) {
            // 如果是 Object 类中的方法，直接使用invoker执行
            return method.invoke(invoker, args);
        }
        // 特殊方法，特殊处理
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 0) {
            if ("toString".equals(methodName)) {
                return invoker.toString();
            } else if ("$destroy".equals(methodName)) {
                invoker.destroy();
                return null;
            } else if ("hashCode".equals(methodName)) {
                return invoker.hashCode();
            }
        } else if (parameterTypes.length == 1 && "equals".equals(methodName)) {
            return invoker.equals(args[0]);
        }
        // 构建调用远程服务的参数
        RpcInvocation rpcInvocation = new RpcInvocation(method, invoker.getInterface().getName(), protocolServiceKey, args);
        String serviceKey = invoker.getUrl().getServiceKey();
        rpcInvocation.setTargetServiceUniqueName(serviceKey);

        // invoker.getUrl() returns consumer url.
        RpcContext.setRpcContext(invoker.getUrl());

        if (consumerModel != null) {
            rpcInvocation.put(Constants.CONSUMER_MODEL, consumerModel);
            rpcInvocation.put(Constants.METHOD_MODEL, consumerModel.getMethodModel(method));
        }

        // 使用 invoker 执行方法
        // 这里的recreate方法很重要，他会调用AppResponse的recreate方法，
        // 如果AppResponse对象中存在exception信息，则此方法中会throw这个异常
        return invoker.invoke(rpcInvocation).recreate();
    }
}

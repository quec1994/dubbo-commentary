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
import org.apache.dubbo.rpc.*;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * This Invoker works on provider side, delegates RPC to interface implementation.
 */
public abstract class AbstractProxyInvoker<T> implements Invoker<T> {
    Logger logger = LoggerFactory.getLogger(AbstractProxyInvoker.class);

    private final T proxy;

    private final Class<T> type;

    private final URL url;

    public AbstractProxyInvoker(T proxy, Class<T> type, URL url) {
        if (proxy == null) {
            throw new IllegalArgumentException("proxy == null");
        }
        if (type == null) {
            throw new IllegalArgumentException("interface == null");
        }
        if (!type.isInstance(proxy)) {
            throw new IllegalArgumentException(proxy.getClass().getName() + " not implement interface " + type);
        }
        this.proxy = proxy;
        this.type = type;
        this.url = url;
    }

    @Override
    public Class<T> getInterface() {
        return type;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void destroy() {
    }

    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        try {
            // 调用服务接口动态代理类的方法，得到一个返回值对象
            // 可能是一个CompletableFuture(表示异步返回结果)，也可能是一个正常的方法返回值（同步返回结果）
            Object value = doInvoke(proxy, invocation.getMethodName(), invocation.getParameterTypes(), invocation.getArguments());
            // 将同步返回结果的方法返回值封装为CompletableFuture类型
            CompletableFuture<Object> future = wrapWithFuture(value);
            // 设置一个回调
            CompletableFuture<AppResponse> appResponseFuture = future.handle((obj, t) -> {
                // 不管服务的方法是同步返回结果还是异步返回结果，方法执行完成后的结果都将在这个回调方法中封装成AppResponse返回
                AppResponse result = new AppResponse(invocation);
                if (t != null) {
                    // 服务方法的异步执行部分出现了异常
                    if (t instanceof CompletionException) {
                        result.setException(t.getCause());
                    } else {
                        result.setException(t);
                    }
                } else {
                    result.setValue(obj);
                }
                return result;
            });
            // 当服务的方法执行完后，将服务方法返回值或异常设置到AsyncRpcResult中
            return new AsyncRpcResult(appResponseFuture, invocation);
        } catch (InvocationTargetException e) {
            // 执行服务实现类方法时出异常了

            // 不管服务的方法是同步返回结果还是异步返回结果，在代理类的代理方法里面会做catch异常，
            // 因此在执行服务方法时抛出的异常都会被包装成 InvocationTargetException

            if (RpcContext.getContext().isAsyncStarted() && !RpcContext.getContext().stopAsync()) {
                logger.error("Provider async started, but got an exception from the original method, cannot write the exception back to consumer because an async result may have returned the new thread.", e);
            }
            // 将异常信息封装为一个AsyncRpcResult然后返回
            return AsyncRpcResult.newDefaultAsyncResult(null, e.getTargetException(), invocation);
        } catch (Throwable e) {
            // 执行服务过程中的所有非服务实现类方法抛出的异常都会包装为RpcException进行抛出
            throw new RpcException("Failed to invoke remote proxy method " + invocation.getMethodName() + " to " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    private CompletableFuture<Object> wrapWithFuture(Object value) {
        if (RpcContext.getContext().isAsyncStarted()) {
            // 如果设置了方法异步执行，从异步上下文中拿future
            return ((AsyncContextImpl) (RpcContext.getContext().getAsyncContext())).getInternalFuture();
        } else if (value instanceof CompletableFuture) {
            // 如果方法为异步返回结果，直接返回value
            return (CompletableFuture<Object>) value;
        }
        // 如果方法是同步返回结果，那么将方法返回值包装成异步返回结果
        return CompletableFuture.completedFuture(value);
    }

    protected abstract Object doInvoke(T proxy, String methodName, Class<?>[] parameterTypes, Object[] arguments) throws Throwable;

    @Override
    public String toString() {
        return getInterface() + " -> " + (getUrl() == null ? " " : getUrl().toString());
    }


}

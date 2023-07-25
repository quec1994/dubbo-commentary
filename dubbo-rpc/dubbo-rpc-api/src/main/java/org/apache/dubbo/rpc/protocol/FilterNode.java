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
package org.apache.dubbo.rpc.protocol;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ListenableFilter;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

/**
 * @see org.apache.dubbo.rpc.protocol.ProtocolFilterWrapper
 *
 */
class FilterNode<T> implements Invoker<T>{
    private final Invoker<T> invoker;
    private final Invoker<T> next;
    private final Filter filter;
    
    public FilterNode(final Invoker<T> invoker, final Invoker<T> next, final Filter filter) {
        this.invoker = invoker;
        this.next = next;
        this.filter = filter;
    }

    @Override
    public Class<T> getInterface() {
        return invoker.getInterface();
    }

    @Override
    public URL getUrl() {
        return invoker.getUrl();
    }

    @Override
    public boolean isAvailable() {
        return invoker.isAvailable();
    }

    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        Result asyncResult;
        try {
            // 得到一个异步结果
            asyncResult = filter.invoke(next, invocation);
        } catch (Exception e) {
            // 服务调用执行异常时，回调监听过滤器的onError方法
            if (filter instanceof ListenableFilter) {
                // 使用 ListenableFilter 注册的 ListenerFilter
                // 个性化使用，需手动注册，可以针对某个服务某个方法做监听
                ListenableFilter listenableFilter = ((ListenableFilter) filter);
                try {
                    Filter.Listener listener = listenableFilter.listener(invocation);
                    if (listener != null) {
                        listener.onError(e, invoker, invocation);
                    }
                } finally {
                    listenableFilter.removeListener(invocation);
                }
            } else if (filter instanceof Filter.Listener) {
                // 直接在 ProtocolFilterWrapper.buildInvokerChain 时添加的 ListenerFilter
                // 针对所有的服务调用做监听
                Filter.Listener listener = (Filter.Listener) filter;
                listener.onError(e, invoker, invocation);
            }
            throw e;
        } finally {

        }
        return asyncResult.whenCompleteWithContext((r, t) -> {
            // 服务调用执行完了之后，回调监听过滤器的onResponse或onError方法
            if (filter instanceof ListenableFilter) {
                // 使用 ListenableFilter 注册的 ListenerFilter
                // 个性化使用，需手动注册，可以针对某个服务某个方法做监听
                ListenableFilter listenableFilter = ((ListenableFilter) filter);
                Filter.Listener listener = listenableFilter.listener(invocation);
                try {
                    if (listener != null) {
                        if (t == null) {
                            listener.onResponse(r, invoker, invocation);
                        } else {
                            listener.onError(t, invoker, invocation);
                        }
                    }
                } finally {
                    listenableFilter.removeListener(invocation);
                }
            } else if (filter instanceof Filter.Listener) {
                // 直接在 ProtocolFilterWrapper.buildInvokerChain 时添加的 ListenerFilter
                // 针对所有的服务调用做监听
                Filter.Listener listener = (Filter.Listener) filter;
                if (t == null) {
                    listener.onResponse(r, invoker, invocation);
                } else {
                    listener.onError(t, invoker, invocation);
                }
            }
        });
    }

    @Override
    public void destroy() {
        invoker.destroy();
    }

    @Override
    public String toString() {
        return invoker.toString();
    }

}

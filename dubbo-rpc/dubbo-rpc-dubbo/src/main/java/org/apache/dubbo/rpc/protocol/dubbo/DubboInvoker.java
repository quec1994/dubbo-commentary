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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.utils.AtomicPositiveInteger;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.FutureContext;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.TimeoutCountDown;
import org.apache.dubbo.rpc.protocol.AbstractInvoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_VERSION;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLE_TIMEOUT_COUNTDOWN_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_ATTACHMENT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIME_COUNTDOWN_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;

/**
 * DubboInvoker
 */
public class DubboInvoker<T> extends AbstractInvoker<T> {

    private final ExchangeClient[] clients;

    private final AtomicPositiveInteger index = new AtomicPositiveInteger();

    private final String version;

    private final ReentrantLock destroyLock = new ReentrantLock();

    private final Set<Invoker<?>> invokers;

    public DubboInvoker(Class<T> serviceType, URL url, ExchangeClient[] clients) {
        this(serviceType, url, clients, null);
    }

    public DubboInvoker(Class<T> serviceType, URL url, ExchangeClient[] clients, Set<Invoker<?>> invokers) {
        super(serviceType, url, new String[]{INTERFACE_KEY, GROUP_KEY, TOKEN_KEY});
        this.clients = clients;
        // get version.
        this.version = url.getParameter(VERSION_KEY, DEFAULT_VERSION);
        this.invokers = invokers;
    }

    @Override
    protected Result doInvoke(final Invocation invocation) throws Throwable {
        RpcInvocation inv = (RpcInvocation) invocation;
        final String methodName = RpcUtils.getMethodName(invocation);
        inv.setAttachment(PATH_KEY, getUrl().getPath());
        inv.setAttachment(VERSION_KEY, version);

        // 选择本次调用该使用哪个client
        ExchangeClient currentClient;
        if (clients.length == 1) {
            currentClient = clients[0];
        } else {
            // 轮询使用clients
            currentClient = clients[index.getAndIncrement() % clients.length];
        }
        try {
            // isOneway 表示是否单向请求，默认为false
            boolean isOneway = RpcUtils.isOneway(getUrl(), invocation);
            // 计算超时时间
            int timeout = calculateTimeout(invocation, methodName);
            invocation.put(TIMEOUT_KEY, timeout);
            if (isOneway) {
                boolean isSent = getUrl().getMethodParameter(methodName, Constants.SENT_KEY, false);
                // 发送远端方法调用数据
                currentClient.send(inv, isSent);
                // 只发送数据，不等待远端方法返回结果，直接生成一个默认的结果，value=null
                return AsyncRpcResult.newDefaultAsyncResult(invocation);
            } else {
                // 获取收到响应数据时的回调执行器
                ExecutorService executor = getCallbackExecutor(getUrl(), inv);

                // 异步去请求，得到一个CompletableFuture，这里并不会阻塞
                CompletableFuture<AppResponse> appResponseFuture =
                        currentClient.request(inv, timeout, executor).thenApply(obj -> (AppResponse) obj);

                // save for 2.6.x compatibility, for example, TraceFilter in Zipkin uses com.alibaba.xxx.FutureAdapter
                FutureContext.getContext().setCompatibleFuture(appResponseFuture);

                // AsyncRpcResult只是一个包装类，appResponseFuture才是真正的远端方法返回值容器
                // 如果要达到阻塞的效果在外层使用result去控制
                AsyncRpcResult result = new AsyncRpcResult(appResponseFuture, inv);
                result.setExecutor(executor);
                return result;
            }
        } catch (TimeoutException e) {
            throw new RpcException(RpcException.TIMEOUT_EXCEPTION, "Invoke remote method timeout. method: " + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(), e);
        } catch (RemotingException e) {
            throw new RpcException(RpcException.NETWORK_EXCEPTION, "Failed to invoke remote method: " + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isAvailable() {
        if (!super.isAvailable()) {
            return false;
        }
        for (ExchangeClient client : clients) {
            if (client.isConnected() && !client.hasAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY)) {
                //cannot write == not Available ?
                return true;
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        destroyInternal(false);
    }

    @Override
    public void destroyAll() {
        destroyInternal(true);
    }

    /**
     * when destroy unused invoker, closeAll should be true
     *
     * @param closeAll
     */
    private void destroyInternal(boolean closeAll) {
        // in order to avoid closing a client multiple times, a counter is used in case of connection per jvm, every
        // time when client.close() is called, counter counts down once, and when counter reaches zero, client will be
        // closed.
        if (super.isDestroyed()) {
            return;
        } else {
            // double check to avoid dup close
            destroyLock.lock();
            try {
                if (super.isDestroyed()) {
                    return;
                }
                super.destroy();
                if (invokers != null) {
                    invokers.remove(this);
                }
                for (ExchangeClient client : clients) {
                    try {
                        if (closeAll) {
                            client.closeAll(ConfigurationUtils.getServerShutdownTimeout());
                        } else {
                            client.close(ConfigurationUtils.getServerShutdownTimeout());
                        }
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }

            } finally {
                destroyLock.unlock();
            }
        }
    }

    private int calculateTimeout(Invocation invocation, String methodName) {
        Object countdown = RpcContext.getContext().get(TIME_COUNTDOWN_KEY);
        int timeout = DEFAULT_TIMEOUT;
        if (countdown == null) {
            // 获取超时时间
            timeout = (int) RpcUtils.getTimeout(getUrl(), methodName, RpcContext.getContext(), DEFAULT_TIMEOUT);
            if (getUrl().getParameter(ENABLE_TIMEOUT_COUNTDOWN_KEY, false)) {
                // 将超时时间传递给远程服务器
                invocation.setObjectAttachment(TIMEOUT_ATTACHMENT_KEY, timeout); // pass timeout to remote server
            }
        } else {
            // 调用参数的附件中带了倒计时对象
            TimeoutCountDown timeoutCountDown = (TimeoutCountDown) countdown;
            // 获取倒计时剩余时间
            timeout = (int) timeoutCountDown.timeRemaining(TimeUnit.MILLISECONDS);
            // 将超时时间传递给远程服务器，可用于动态覆盖远端方法超时时间
            invocation.setObjectAttachment(TIMEOUT_ATTACHMENT_KEY, timeout);// pass timeout to remote server
        }
        return timeout;
    }
}

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
package org.apache.dubbo.rpc.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvokeMode;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.service.GenericService;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.$INVOKE;
import static org.apache.dubbo.common.constants.CommonConstants.$INVOKE_ASYNC;
import static org.apache.dubbo.common.constants.CommonConstants.DOT_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_PARAMETER_DESC;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_ATTACHMENT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.rpc.Constants.$ECHO;
import static org.apache.dubbo.rpc.Constants.$ECHO_PARAMETER_DESC;
import static org.apache.dubbo.rpc.Constants.ASYNC_KEY;
import static org.apache.dubbo.rpc.Constants.AUTO_ATTACH_INVOCATIONID_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.ID_KEY;
import static org.apache.dubbo.rpc.Constants.RETURN_KEY;

/**
 * RpcUtils
 */
public class RpcUtils {

    private static final Logger logger = LoggerFactory.getLogger(RpcUtils.class);
    private static final AtomicLong INVOKE_ID = new AtomicLong(0);

    public static Class<?> getReturnType(Invocation invocation) {
        try {
            // 获取泛化调用方法的返回值
            if (invocation != null && invocation.getInvoker() != null
                    && invocation.getInvoker().getUrl() != null
                    && invocation.getInvoker().getInterface() != GenericService.class
                    && !invocation.getMethodName().startsWith("$")) {
                // 泛化调用
                String service = invocation.getInvoker().getUrl().getServiceInterface();
                if (StringUtils.isNotEmpty(service)) {
                    Method method = getMethodByService(invocation, service);
                    return method == null ? null : method.getReturnType();
                }
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        return null;
    }

    public static Type[] getReturnTypes(Invocation invocation) {
        try {
            if (invocation != null && invocation.getInvoker() != null
                    && invocation.getInvoker().getUrl() != null
                    && invocation.getInvoker().getInterface() != GenericService.class
                    && !invocation.getMethodName().startsWith("$")) {
                /**
                 * if is generic, must use readObject()
                 * the serialization like hession2, avro when use readObject(class), when the class is Template like class A<T>
                 *  it use the A class to create and T is jsonobject, so it muse be error
                 *  so it can depend to the data describe or GenericImplFilter to PojoUtils.realize it use Type
                 *  of course we can change the serialization to use Type to create, but this work is to large....
                 */
                String generic = invocation.getInvoker().getUrl().getParameter(GENERIC_KEY);
                if (!ProtocolUtils.isGeneric(generic)) {
                    String service = invocation.getInvoker().getUrl().getServiceInterface();
                    if (StringUtils.isNotEmpty(service)) {
                        Method method = getMethodByService(invocation, service);
                        return ReflectUtils.getReturnTypes(method);
                    }
                }
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        return null;
    }

    public static Long getInvocationId(Invocation inv) {
        String id = inv.getAttachment(ID_KEY);
        return id == null ? null : new Long(id);
    }

    /**
     * Idempotent operation: invocation id will be added in async operation by default
     *
     * @param url
     * @param inv
     */
    public static void attachInvocationIdIfAsync(URL url, Invocation inv) {
        // 幂等操作：默认情况下会在异步操作中添加调用id
        if (isAttachInvocationId(url, inv) && getInvocationId(inv) == null && inv instanceof RpcInvocation) {
            inv.setAttachment(ID_KEY, String.valueOf(INVOKE_ID.getAndIncrement()));
        }
    }

    private static boolean isAttachInvocationId(URL url, Invocation invocation) {
        // 方法参数里设置的是否要自动附上调用id
        String value = url.getMethodParameter(invocation.getMethodName(), AUTO_ATTACH_INVOCATIONID_KEY);
        if (value == null) {
            // 方法参数里没有设置是否要自动附上调用id
            // 查看是否异步操作，异步操作默认设置invocationid
            // add invocationid in async operation by default
            return isAsync(url, invocation);
        } else if (Boolean.TRUE.toString().equalsIgnoreCase(value)) {
            // 方法参数里设置了要自动附上调用id
            return true;
        } else {
            // 否则不自动附上调用id
            return false;
        }
    }

    public static String getMethodName(Invocation invocation) {
        if ($INVOKE.equals(invocation.getMethodName())
                && invocation.getArguments() != null
                && invocation.getArguments().length > 0
                && invocation.getArguments()[0] instanceof String) {
            // 泛化调用时第一个参数是方法名
            return (String) invocation.getArguments()[0];
        }
        // 非泛化调用直接返回invocation里的方法名称
        return invocation.getMethodName();
    }

    public static Object[] getArguments(Invocation invocation) {
        if ($INVOKE.equals(invocation.getMethodName())
                && invocation.getArguments() != null
                && invocation.getArguments().length > 2
                && invocation.getArguments()[2] instanceof Object[]) {
            return (Object[]) invocation.getArguments()[2];
        }
        return invocation.getArguments();
    }

    public static Class<?>[] getParameterTypes(Invocation invocation) {
        if ($INVOKE.equals(invocation.getMethodName())
                && invocation.getArguments() != null
                && invocation.getArguments().length > 1
                && invocation.getArguments()[1] instanceof String[]) {
            String[] types = (String[]) invocation.getArguments()[1];
            if (types == null) {
                return new Class<?>[0];
            }
            Class<?>[] parameterTypes = new Class<?>[types.length];
            for (int i = 0; i < types.length; i++) {
                parameterTypes[i] = ReflectUtils.forName(types[0]);
            }
            return parameterTypes;
        }
        return invocation.getParameterTypes();
    }

    public static boolean isAsync(URL url, Invocation inv) {
        // 是否异步操作
        boolean isAsync;

        if (inv instanceof RpcInvocation) {
            RpcInvocation rpcInvocation = (RpcInvocation) inv;
            if (rpcInvocation.getInvokeMode() != null) {
                // 调用参数模式
                return rpcInvocation.getInvokeMode() == InvokeMode.ASYNC;
            }
        }

        String config;
        if ((config = inv.getAttachment(getMethodName(inv) + DOT_SEPARATOR + ASYNC_KEY)) != null) {
            // 附加参数里的方法设置
            isAsync = Boolean.valueOf(config);
        } else if ((config = inv.getAttachment(ASYNC_KEY)) != null) {
            // 附加参数里的服务设置
            isAsync = Boolean.valueOf(config);
        } else if ((config = url.getMethodParameter(getMethodName(inv), ASYNC_KEY)) != null) {
            // 方法或服务参数
            isAsync = Boolean.valueOf(config);
        } else {
            // 服务参数
            isAsync = url.getParameter(ASYNC_KEY, false);
        }
        return isAsync;
    }

    public static boolean isReturnTypeFuture(Invocation inv) {
        // 判断是不是异步方法返回值

        Class<?> clazz;
        if (inv instanceof RpcInvocation) {
            clazz = ((RpcInvocation) inv).getReturnType();
        } else {
            // 获取泛化调用的方法返回值
            clazz = getReturnType(inv);
        }
        // 判断返回值
        return (clazz != null && CompletableFuture.class.isAssignableFrom(clazz))
                // 根据泛化调用使用的方法判断
                || isGenericAsync(inv);
    }

    public static boolean isGenericAsync(Invocation inv) {
        return $INVOKE_ASYNC.equals(inv.getMethodName());
    }

    // check parameterTypesDesc to fix CVE-2020-1948
    public static boolean isGenericCall(String parameterTypesDesc, String method) {
        return ($INVOKE.equals(method) || $INVOKE_ASYNC.equals(method)) && GENERIC_PARAMETER_DESC.equals(parameterTypesDesc);
    }

    // check parameterTypesDesc to fix CVE-2020-1948
    public static boolean isEcho(String parameterTypesDesc, String method) {
        return $ECHO.equals(method) && $ECHO_PARAMETER_DESC.equals(parameterTypesDesc);
    }

    public static InvokeMode getInvokeMode(URL url, Invocation inv) {
        if (inv instanceof RpcInvocation) {
            // 第一次执行的时候一般不会走这个逻辑
            RpcInvocation rpcInvocation = (RpcInvocation) inv;
            if (rpcInvocation.getInvokeMode() != null) {
                return rpcInvocation.getInvokeMode();
            }
        }

        if (isReturnTypeFuture(inv)) {
            // 异步方法返回值
            return InvokeMode.FUTURE;
        } else if (isAsync(url, inv)) {
            // 异步操作
            return InvokeMode.ASYNC;
        } else {
            return InvokeMode.SYNC;
        }
    }

    public static boolean isOneway(URL url, Invocation inv) {
        // 判断是否指定了单向请求，单向请求不需要的远端的返回值，只调用不等待远端方法的执行结果

        boolean isOneway;
        String config;
        if ((config = inv.getAttachment(getMethodName(inv) + DOT_SEPARATOR + RETURN_KEY)) != null) {
            // 调用参数附件里的方法设置
            isOneway = !Boolean.valueOf(config);
        } else if ((config = inv.getAttachment(RETURN_KEY)) != null) {
            // 调用参数附件里的直接设置
            isOneway = !Boolean.valueOf(config);
        } else {
            // url上的方法和服务参数
            isOneway = !url.getMethodParameter(getMethodName(inv), RETURN_KEY, true);
        }
        return isOneway;
    }

    private static Method getMethodByService(Invocation invocation, String service) throws NoSuchMethodException {
        Class<?> invokerInterface = invocation.getInvoker().getInterface();
        Class<?> cls = invokerInterface != null ? ReflectUtils.forName(invokerInterface.getClassLoader(), service)
                : ReflectUtils.forName(service);
        Method method = cls.getMethod(invocation.getMethodName(), invocation.getParameterTypes());
        if (method.getReturnType() == void.class) {
            return null;
        }
        return method;
    }

    public static long getTimeout(Invocation invocation, long defaultTimeout) {
        long timeout = defaultTimeout;
        Object genericTimeout = invocation.getObjectAttachment(TIMEOUT_ATTACHMENT_KEY);
        if (genericTimeout != null) {
            timeout = convertToNumber(genericTimeout, defaultTimeout);
        }
        return timeout;
    }

    public static long getTimeout(URL url, String methodName, RpcContext context, long defaultTimeout) {
        long timeout = defaultTimeout;
        // 调用参数附件里的
        Object genericTimeout = context.getObjectAttachment(TIMEOUT_KEY);
        if (genericTimeout != null) {
            timeout = convertToNumber(genericTimeout, defaultTimeout);
        } else if (url != null) {
            // 拿当前方法和服务参数上所配置的超时时间，默认为1000，1秒
            timeout = url.getMethodPositiveParameter(methodName, TIMEOUT_KEY, defaultTimeout);
        }
        return timeout;
    }

    private static long convertToNumber(Object obj, long defaultTimeout) {
        long timeout = 0;
        try {
            if (obj instanceof String) {
                timeout = Long.parseLong((String) obj);
            } else if (obj instanceof Number) {
                timeout = ((Number) obj).longValue();
            } else {
                timeout = Long.parseLong(obj.toString());
            }
        } catch (Exception e) {
            // ignore
        }
        return timeout;
    }
}

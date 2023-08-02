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
package org.apache.dubbo.common.bytecode;

import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.ReflectUtils;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.MAX_PROXY_COUNT;

/**
 * Proxy.
 */

public abstract class Proxy {
    public static final InvocationHandler RETURN_NULL_INVOKER = (proxy, method, args) -> null;
    public static final InvocationHandler THROW_UNSUPPORTED_INVOKER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            throw new UnsupportedOperationException("Method [" + ReflectUtils.getName(method) + "] unimplemented.");
        }
    };
    private static final AtomicLong PROXY_CLASS_COUNTER = new AtomicLong(0);
    private static final String PACKAGE_NAME = Proxy.class.getPackage().getName();
    private static final Map<ClassLoader, Map<String, Object>> PROXY_CACHE_MAP = new WeakHashMap<ClassLoader, Map<String, Object>>();
    // cache class, avoid PermGen OOM.
    private static final Map<ClassLoader, Map<String, Object>> PROXY_CLASS_MAP = new WeakHashMap<ClassLoader, Map<String, Object>>();
    // 挂起的生成标记
    private static final Object PENDING_GENERATION_MARKER = new Object();

    protected Proxy() {
    }

    /**
     * Get proxy.
     *
     * @param ics interface class array.
     * @return Proxy instance.
     */
    public static Proxy getProxy(Class<?>... ics) {
        return getProxy(ClassUtils.getClassLoader(Proxy.class), ics);
    }

    /**
     * Get proxy.
     *
     * @param cl  class loader.
     * @param ics interface class array.
     * @return Proxy instance.
     */
    public static Proxy getProxy(ClassLoader cl, Class<?>... ics) {
        if (ics.length > MAX_PROXY_COUNT) {
            throw new IllegalArgumentException("interface limit exceeded");
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ics.length; i++) {
            String itf = ics[i].getName();
            // 校验传进来的Class是不是一个接口
            if (!ics[i].isInterface()) {
                throw new RuntimeException(itf + " is not a interface.");
            }

            // 校验传进来的Class是不是在cl里加载的
            Class<?> tmp = null;
            try {
                tmp = Class.forName(itf, false, cl);
            } catch (ClassNotFoundException e) {
            }

            if (tmp != ics[i]) {
                throw new IllegalArgumentException(ics[i] + " is not visible from class loader");
            }

            sb.append(itf).append(';');
        }

        // use interface class name list as key.
        // 使用传进来的接口类的名字组成Key
        String key = sb.toString();

        // get cache by class loader.
        // 代理类对象实例缓存，key是传进来的接口类的名字组成的
        final Map<String, Object> cache;
        // cache class
        // 代理类缓存，key是传进来的接口类的名字组成的
        final Map<String, Object> classCache;
        synchronized (PROXY_CACHE_MAP) {
            // 还有一层 class loader 做key的缓存
            cache = PROXY_CACHE_MAP.computeIfAbsent(cl, k -> new HashMap<>());
            classCache = PROXY_CLASS_MAP.computeIfAbsent(cl, k -> new HashMap<>());
        }

        Proxy proxy = null;
        // 加锁控制同一个 class loader 下按照线性同步顺序打代理对象生成标记
        synchronized (cache) {
            do {
                // 这是个死循环，成功在 cache 里放入了 挂起的生成标记 的线程可以走出死循环
                // cache.put(key, PENDING_GENERATION_MARKER);

                Object value = cache.get(key);
                if (value instanceof Reference<?>) {
                    // 之前生成过了
                    // 为了防止OOM，缓存的是SoftReference对象，SoftReference对象里面保存了代理类生成类对象实例
                    // 代理类生成类 —— 代理类对象实例需要调用 Proxy 类的 newInstance 方法去生成
                    proxy = (Proxy) ((Reference<?>) value).get();
                    if (proxy != null) {
                        // 如果软引用里的代理类生成类对象实例没有被清除掉，直接返回之前生成的代理类生成类对象实例
                        return proxy;
                    }
                }

                // get Class by key.
                Object clazzObj = classCache.get(key);
                if (null == clazzObj || clazzObj instanceof Reference<?>) {
                    Class<?> clazz = null;
                    if (clazzObj instanceof Reference<?>) {
                        // 之前生成过代理类生成类，直接使用之前生成的代理类生成类
                        // 为了防止OOM，缓存的是SoftReference对象，SoftReference对象里面保存了代理类生成类
                        clazz = (Class<?>) ((Reference<?>) clazzObj).get();
                    }

                    if (null == clazz) {
                        if (value == PENDING_GENERATION_MARKER) {
                            try {
                                // 如果缓存里的代理对象是生成标记则挂起，相当于一个锁防止多线程下重复生成代理对象
                                cache.wait();
                            } catch (InterruptedException e) {
                            }
                        } else {
                            // 在缓存Map中放入上代理对象生成的标记
                            cache.put(key, PENDING_GENERATION_MARKER);
                            // 成功在 cache 里放入了代理对象生成标记，离开死循环
                            break;
                        }
                    } else {
                        try {
                            // 如果软引用里的代理类没有被清除掉，直接使用之前生成的代理类进行实例化
                            proxy = (Proxy) clazz.newInstance();
                            return proxy;
                        } catch (InstantiationException | IllegalAccessException e) {
                            throw new RuntimeException(e);
                        } finally {
                            if (null == proxy) {
                                cache.remove(key);
                            } else {
                                // 将生成的代理对象保存进缓存中
                                cache.put(key, new SoftReference<Proxy>(proxy));
                            }
                        }
                    }
                }
            }
            while (true);
        }

        long id = PROXY_CLASS_COUNTER.getAndIncrement();
        String pkg = null;
        ClassGenerator ccp = null, ccm = null;
        try {
            ccp = ClassGenerator.newInstance(cl);

            Set<String> worked = new HashSet<>();
            List<Method> methods = new ArrayList<>();

            for (int i = 0; i < ics.length; i++) {
                // 校验传进来的非public修饰的接口是否是同一个package下
                if (!Modifier.isPublic(ics[i].getModifiers())) {
                    String npkg = ics[i].getPackage().getName();
                    if (pkg == null) {
                        // 如果传进来的接口是非public修饰的，则使用接口的package
                        pkg = npkg;
                    } else {
                        if (!pkg.equals(npkg)) {
                            // 存在不同的package下的非public修饰的接口直接报错
                            throw new IllegalArgumentException("non-public interfaces from different packages");
                        }
                    }
                }
                // 给代理类添加实现的接口
                ccp.addInterface(ics[i]);
                // 生成代理方法
                for (Method method : ics[i].getMethods()) {
                    String desc = ReflectUtils.getDesc(method);
                    if (worked.contains(desc) || Modifier.isStatic(method.getModifiers())) {
                        // 方法相同不重复生成代理方法
                        // 方法被static修饰不做代理
                        continue;
                    }
                    // 添加已生成代理方法的接口方法
                    worked.add(desc);
                    // sayHello(Ljava/lang/String;)Ljava/lang/String;

                    int ix = methods.size();
                    Class<?> rt = method.getReturnType();
                    Class<?>[] pts = method.getParameterTypes();

                    StringBuilder code = new StringBuilder("Object[] args = new Object[").append(pts.length).append("];");
                    // Object[] args = new Object[1];
                    for (int j = 0; j < pts.length; j++) {
                        code.append(" args[").append(j).append("] = ($w)$").append(j + 1).append(";");
                    }
                    // Object[] args = new Object[1]; args[0] = ($w)$1;
                    code.append(" Object ret = handler.invoke(this, methods[").append(ix).append("], args);");
                    // Object[] args = new Object[1]; args[0] = ($w)$1; Object ret = handler.invoke(this, methods[1], args);
                    if (!Void.TYPE.equals(rt)) {
                        code.append(" return ").append(asArgument(rt, "ret")).append(";");
                        // Object[] args = new Object[1]; args[0] = ($w)$1; Object ret = handler.invoke(this, methods[1], args); return (java.lang.String)ret;
                    }

                    // 保存被代理的方法
                    methods.add(method);
                    // Object[] args = new Object[1]; args[0] = ($w)$1; Object ret = handler.invoke(this, methods[1], args); return (java.lang.String)ret;
                    // 在代理类中添加代理方法
                    ccp.addMethod(method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(), code.toString());
                }
            }

            if (pkg == null) {
                // 如果传进来的接口没有非public修饰的接口，则使用固定的包名
                pkg = PACKAGE_NAME;
                // org.apache.dubbo.common.bytecode
            }

            // create ProxyInstance class.
            // 代理类类名
            String pcn = pkg + ".proxy" + id;
            ccp.setClassName(pcn);
            // 属性-被代理的方法集合，代理方法中会用到
            ccp.addField("public static java.lang.reflect.Method[] methods;");
            // 属性-执行方法的handler，类型是InvocationHandler，代理方法中会用到
            ccp.addField("private " + InvocationHandler.class.getName() + " handler;");
            // 构造方法 - 给handler属性赋值
            ccp.addConstructor(Modifier.PUBLIC, new Class<?>[]{InvocationHandler.class}, new Class<?>[0], "handler=$1;");
            // 添加空参构造函数
            ccp.addDefaultConstructor();
            // 代理类
            Class<?> clazz = ccp.toClass();
            // 给 methods 属性赋值
            clazz.getField("methods").set(null, methods.toArray(new Method[0]));

            // create Proxy class.
            // 外层代理类类名，用于生成代理类
            String fcn = Proxy.class.getName() + id;
            ccm = ClassGenerator.newInstance(cl);
            ccm.setClassName(fcn);
            ccm.addDefaultConstructor();
            ccm.setSuperClass(Proxy.class);
            // 实现抽象类 Proxy 的 newInstance(java.lang.reflect.InvocationHandler) 方法，实例化代理类
            ccm.addMethod("public Object newInstance(" + InvocationHandler.class.getName() + " h){ return new " + pcn + "($1); }");
            // public Object newInstance(java.lang.reflect.InvocationHandler h){ return new org.apache.dubbo.common.bytecode.proxy0($1); }
            Class<?> pc = ccm.toClass();
            // 实例化代理类生成类
            proxy = (Proxy) pc.newInstance();

            synchronized (classCache) {
                // 缓存代理类生成类对象实例
                classCache.put(key, new SoftReference<Class<?>>(pc));
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            // release ClassGenerator
            if (ccp != null) {
                ccp.release();
            }
            if (ccm != null) {
                ccm.release();
            }
            synchronized (cache) {
                if (proxy == null) {
                    cache.remove(key);
                } else {
                    // 缓存代理类生成类
                    cache.put(key, new SoftReference<>(proxy));
                }
                cache.notifyAll();
            }
        }
        return proxy;
    }

    private static String asArgument(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (Boolean.TYPE == cl) {
                return name + "==null?false:((Boolean)" + name + ").booleanValue()";
            }
            if (Byte.TYPE == cl) {
                return name + "==null?(byte)0:((Byte)" + name + ").byteValue()";
            }
            if (Character.TYPE == cl) {
                return name + "==null?(char)0:((Character)" + name + ").charValue()";
            }
            if (Double.TYPE == cl) {
                return name + "==null?(double)0:((Double)" + name + ").doubleValue()";
            }
            if (Float.TYPE == cl) {
                return name + "==null?(float)0:((Float)" + name + ").floatValue()";
            }
            if (Integer.TYPE == cl) {
                return name + "==null?(int)0:((Integer)" + name + ").intValue()";
            }
            if (Long.TYPE == cl) {
                return name + "==null?(long)0:((Long)" + name + ").longValue()";
            }
            if (Short.TYPE == cl) {
                return name + "==null?(short)0:((Short)" + name + ").shortValue()";
            }
            throw new RuntimeException(name + " is unknown primitive type.");
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }

    /**
     * get instance with default handler.
     *
     * @return instance.
     */
    public Object newInstance() {
        return newInstance(THROW_UNSUPPORTED_INVOKER);
    }

    /**
     * get instance with special handler.
     *
     * @return instance.
     */
    abstract public Object newInstance(InvocationHandler handler);
}

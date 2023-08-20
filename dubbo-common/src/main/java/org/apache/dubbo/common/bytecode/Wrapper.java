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

import java.util.Arrays;
import java.util.stream.Collectors;
import javassist.ClassPool;
import javassist.CtMethod;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.ReflectUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;

/**
 * Wrapper.
 */
public abstract class Wrapper {
    private static final Map<Class<?>, Wrapper> WRAPPER_MAP = new ConcurrentHashMap<Class<?>, Wrapper>(); //class wrapper map
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static final String[] OBJECT_METHODS = new String[]{"getClass", "hashCode", "toString", "equals"};
    private static final Wrapper OBJECT_WRAPPER = new Wrapper() {
        @Override
        public String[] getMethodNames() {
            return OBJECT_METHODS;
        }

        @Override
        public String[] getDeclaredMethodNames() {
            return OBJECT_METHODS;
        }

        @Override
        public String[] getPropertyNames() {
            return EMPTY_STRING_ARRAY;
        }

        @Override
        public Class<?> getPropertyType(String pn) {
            return null;
        }

        @Override
        public Object getPropertyValue(Object instance, String pn) throws NoSuchPropertyException {
            throw new NoSuchPropertyException("Property [" + pn + "] not found.");
        }

        @Override
        public void setPropertyValue(Object instance, String pn, Object pv) throws NoSuchPropertyException {
            throw new NoSuchPropertyException("Property [" + pn + "] not found.");
        }

        @Override
        public boolean hasProperty(String name) {
            return false;
        }

        @Override
        public Object invokeMethod(Object instance, String mn, Class<?>[] types, Object[] args) throws NoSuchMethodException {
            if ("getClass".equals(mn)) {
                return instance.getClass();
            }
            if ("hashCode".equals(mn)) {
                return instance.hashCode();
            }
            if ("toString".equals(mn)) {
                return instance.toString();
            }
            if ("equals".equals(mn)) {
                if (args.length == 1) {
                    return instance.equals(args[0]);
                }
                throw new IllegalArgumentException("Invoke method [" + mn + "] argument number error.");
            }
            throw new NoSuchMethodException("Method [" + mn + "] not found.");
        }
    };
    private static AtomicLong WRAPPER_CLASS_COUNTER = new AtomicLong(0);

    /**
     * get wrapper.
     *
     * @param c Class instance.
     * @return Wrapper instance(not null).
     */
    public static Wrapper getWrapper(Class<?> c) {
        while (ClassGenerator.isDynamicClass(c)) // can not wrapper on dynamic class.
        {
            c = c.getSuperclass();
        }

        if (c == Object.class) {
            return OBJECT_WRAPPER;
        }

        // 动态生成一个Wrapper子类并实例化
        return WRAPPER_MAP.computeIfAbsent(c, Wrapper::makeWrapper);
    }

    private static Wrapper makeWrapper(Class<?> c) {
        if (c.isPrimitive()) {
            throw new IllegalArgumentException("Can not create wrapper for primitive type: " + c);
        }

        String name = c.getName();
        ClassLoader cl = ClassUtils.getClassLoader(c);

        // 属性设置动态代理代码
        StringBuilder c1 = new StringBuilder("public void setPropertyValue(Object o, String n, Object v){ ");
        // 属性获取动态代理代码
        StringBuilder c2 = new StringBuilder("public Object getPropertyValue(Object o, String n){ ");
        // 方法动态代理代码
        StringBuilder c3 = new StringBuilder("public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws " + InvocationTargetException.class.getName() + "{ ");

        c1.append(name).append(" w; try{ w = ((").append(name).append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
        // public void setPropertyValue(Object o, String n, Object v){ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }
        c2.append(name).append(" w; try{ w = ((").append(name).append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
        // public Object getPropertyValue(Object o, String n){ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }
        c3.append(name).append(" w; try{ w = ((").append(name).append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
        // public Object getPropertyValue(Object o, String n){ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }

        Map<String, Class<?>> pts = new HashMap<>(); // <property name, property types>
        Map<String, Method> ms = new LinkedHashMap<>(); // <method desc, Method instance>
        List<String> mns = new ArrayList<>(); // method names.
        List<String> dmns = new ArrayList<>(); // declaring method names.

        // get all public field.
        // public 修饰的 属性设置/属性获取 的主要动态代理代码
        for (Field f : c.getFields()) {
            String fn = f.getName();
            Class<?> ft = f.getType();
            if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) {
                // static、Transient 修饰的属性不代理
                continue;
            }

            c1.append(" if( $2.equals(\"").append(fn).append("\") ){ w.").append(fn).append("=").append(arg(ft, "$3")).append("; return; }");
            c2.append(" if( $2.equals(\"").append(fn).append("\") ){ return ($w)w.").append(fn).append("; }");
            pts.put(fn, ft);
        }

        final ClassPool classPool = new ClassPool(ClassPool.getDefault());
        classPool.appendClassPath(new CustomizedLoaderClassPath(cl));

        List<String> allMethod = new ArrayList<>();
        try {
            final CtMethod[] ctMethods = classPool.get(c.getName()).getMethods();
            for (CtMethod method : ctMethods) {
                allMethod.add(ReflectUtils.getDesc(method));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Method[] methods = Arrays.stream(c.getMethods())
                                 .filter(method -> allMethod.contains(ReflectUtils.getDesc(method)))
                                 .collect(Collectors.toList())
                                 .toArray(new Method[] {});
        // get all public method.
        // 有非Object.class里的方法
        boolean hasMethod = hasMethods(methods);
        if (hasMethod) {
            // 给方法名计数，用于判断是否有重载方法
            Map<String, Integer> sameNameMethodCount = new HashMap<>((int) (methods.length / 0.75f) + 1);
            for (Method m : methods) {
                sameNameMethodCount.compute(m.getName(),
                        (key, oldValue) -> oldValue == null ? 1 : oldValue + 1);
            }

            c3.append(" try{");
            // public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{
            for (Method m : methods) {
                //ignore Object's method.
                // 忽略Object.class里的方法
                if (m.getDeclaringClass() == Object.class) {
                    continue;
                }

                String mn = m.getName();
                // 动态代理代码作用：判断是否是要调用的被代理方法
                // if ("CLASS_METHOD_NAME".equals($2) && $3.length == 2
                c3.append(" if( \"").append(mn).append("\".equals( $2 ) ");
                // 0: public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )
                // 1: public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHello((java.lang.String)$4[0]); } if( "sayHelloAsync".equals( $2 )
                int len = m.getParameterTypes().length;
                c3.append(" && ").append(" $3.length == ").append(len);
                // 0: public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1
                // 1: public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHello((java.lang.String)$4[0]); } if( "sayHelloAsync".equals( $2 )  &&  $3.length == 1

                boolean overload = sameNameMethodCount.get(m.getName()) > 1;
                if (overload) {
                    // 有方法重载的
                    if (len > 0) {
                        // 动态代理代码作用：加上方法类型的匹配条件
                        // && $3[1].getName().equals("CLASS_METHOD_PARAMETER_1_TYPE_NAME")
                        //     && $3[2].getName().equals("CLASS_METHOD_PARAMETER_2_TYPE_NAME")
                        for (int l = 0; l < len; l++) {
                            c3.append(" && ").append(" $3[").append(l).append("].getName().equals(\"")
                                    .append(m.getParameterTypes()[l].getName()).append("\")");
                        }
                    }
                }

                c3.append(" ) { ");
                // 0: public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {
                // 1: public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHello((java.lang.String)$4[0]); } if( "sayHelloAsync".equals( $2 )  &&  $3.length == 1 ) {

                // 动态代理代码作用：调用被代理方法
                if (m.getReturnType() == Void.TYPE) {
                    // ($w) w.CLASS_METHOD_NAME((CLASS_METHOD_PARAMETER_1_TYPE_NAME) $4[0]
                    //     ,(CLASS_METHOD_PARAMETER_2_TYPE_NAME) $4[1]); return null;
                    c3.append(" w.").append(mn).append('(').append(args(m.getParameterTypes(), "$4")).append(");").append(" return null;");
                } else {
                    // return ($w) w.CLASS_METHOD_NAME((CLASS_METHOD_PARAMETER_1_TYPE_NAME) $4[0]
                    //     ,(CLASS_METHOD_PARAMETER_2_TYPE_NAME) $4[1]);
                    c3.append(" return ($w)w.").append(mn).append('(').append(args(m.getParameterTypes(), "$4")).append(");");
                    // 0: public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHello((java.lang.String)$4[0]);
                    // 1: public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHello((java.lang.String)$4[0]); } if( "sayHelloAsync".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHelloAsync((java.lang.String)$4[0]);
                }

                c3.append(" }");
                // 0: public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHello((java.lang.String)$4[0]); }
                // 1: public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHello((java.lang.String)$4[0]); } if( "sayHelloAsync".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHelloAsync((java.lang.String)$4[0]); }

                mns.add(mn);
                if (m.getDeclaringClass() == c) {
                    dmns.add(mn);
                }
                ms.put(ReflectUtils.getDesc(m), m);
            }
            // 动态代理代码作用：在调用被代理方法的时候如果抛出异常，将异常包装成 InvocationTargetException 后抛出
            c3.append(" } catch(Throwable e) { ");
            c3.append("     throw new java.lang.reflect.InvocationTargetException(e); ");
            c3.append(" }");
            // public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHello((java.lang.String)$4[0]); } if( "sayHelloAsync".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHelloAsync((java.lang.String)$4[0]); } } catch(Throwable e) {      throw new java.lang.reflect.InvocationTargetException(e);  }
        }

        // 动态代理代码作用：没有匹配到被代理方法时抛出异常
        c3.append(" throw new " + NoSuchMethodException.class.getName() + "(\"Not found method \\\"\"+$2+\"\\\" in class " + c.getName() + ".\"); }");
        // public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHello((java.lang.String)$4[0]); } if( "sayHelloAsync".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHelloAsync((java.lang.String)$4[0]); } } catch(Throwable e) {      throw new java.lang.reflect.InvocationTargetException(e);  } throw new org.apache.dubbo.common.bytecode.NoSuchMethodException("Not found method \""+$2+"\" in class org.apache.dubbo.demo.DemoService."); }

        // deal with get/set method.
        // 通过方法设置和获取属性的主要动态代理代码
        Matcher matcher;
        for (Map.Entry<String, Method> entry : ms.entrySet()) {
            String md = entry.getKey();
            Method method = entry.getValue();
            if ((matcher = ReflectUtils.GETTER_METHOD_DESC_PATTERN.matcher(md)).matches()) {
                // get 方法

                // 方法里的属性名
                String pn = propertyName(matcher.group(1));
                c2.append(" if( $2.equals(\"").append(pn).append("\") ){ return ($w)w.").append(method.getName()).append("(); }");
                pts.put(pn, method.getReturnType());
            } else if ((matcher = ReflectUtils.IS_HAS_CAN_METHOD_DESC_PATTERN.matcher(md)).matches()) {
                // is 方法

                // 方法里的属性名
                String pn = propertyName(matcher.group(1));
                c2.append(" if( $2.equals(\"").append(pn).append("\") ){ return ($w)w.").append(method.getName()).append("(); }");
                pts.put(pn, method.getReturnType());
            } else if ((matcher = ReflectUtils.SETTER_METHOD_DESC_PATTERN.matcher(md)).matches()) {
                // set 方法

                Class<?> pt = method.getParameterTypes()[0];
                // 方法里的属性名
                String pn = propertyName(matcher.group(1));
                c1.append(" if( $2.equals(\"").append(pn).append("\") ){ w.").append(method.getName()).append("(").append(arg(pt, "$3")).append("); return; }");
                pts.put(pn, pt);
            }
        }
        // 动态代理代码作用：没有匹配到属性赋值方法时抛出异常
        c1.append(" throw new " + NoSuchPropertyException.class.getName() + "(\"Not found property \\\"\"+$2+\"\\\" field or setter method in class " + c.getName() + ".\"); }");
        // public void setPropertyValue(Object o, String n, Object v){ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } throw new org.apache.dubbo.common.bytecode.NoSuchPropertyException("Not found property \""+$2+"\" field or setter method in class org.apache.dubbo.demo.DemoService."); }

        // 动态代理代码作用：没有匹配到属性获取方法时抛出异常
        c2.append(" throw new " + NoSuchPropertyException.class.getName() + "(\"Not found property \\\"\"+$2+\"\\\" field or getter method in class " + c.getName() + ".\"); }");
        // public Object getPropertyValue(Object o, String n){ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } throw new org.apache.dubbo.common.bytecode.NoSuchPropertyException("Not found property \""+$2+"\" field or getter method in class org.apache.dubbo.demo.DemoService."); }

        // make class
        // 构造代理类的字节码生成器
        long id = WRAPPER_CLASS_COUNTER.getAndIncrement();
        ClassGenerator cc = ClassGenerator.newInstance(cl);
        // 代理类名称
        cc.setClassName((Modifier.isPublic(c.getModifiers()) ? Wrapper.class.getName() : c.getName() + "$sw") + id);
        // 代理类父类
        cc.setSuperClass(Wrapper.class);

        cc.addDefaultConstructor();
        // 代理类属性：被代理类属性名称数组
        cc.addField("public static String[] pns;"); // property name array.
        // 代理类属性：被代理类属性类型映射
        cc.addField("public static " + Map.class.getName() + " pts;"); // property type map.
        // 代理类属性：被代理类所有方法名称数组
        cc.addField("public static String[] mns;"); // all method name array.
        // 代理类属性：被代理类公开声明的方法名称数组
        cc.addField("public static String[] dmns;"); // declared method name array.
        // 被代理类所有方法参数类型数组
        for (int i = 0, len = ms.size(); i < len; i++) {
            // 代理类属性：被代理类方法参数类型数组
            cc.addField("public static Class[] mts" + i + ";");
        }

        cc.addMethod("public String[] getPropertyNames(){ return pns; }");
        cc.addMethod("public boolean hasProperty(String n){ return pts.containsKey($1); }");
        cc.addMethod("public Class getPropertyType(String n){ return (Class)pts.get($1); }");
        cc.addMethod("public String[] getMethodNames(){ return mns; }");
        cc.addMethod("public String[] getDeclaredMethodNames(){ return dmns; }");
        cc.addMethod(c1.toString());
        // public void setPropertyValue(Object o, String n, Object v){ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } throw new org.apache.dubbo.common.bytecode.NoSuchPropertyException("Not found property \""+$2+"\" field or setter method in class org.apache.dubbo.demo.DemoService."); }
        cc.addMethod(c2.toString());
        // public Object getPropertyValue(Object o, String n){ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } throw new org.apache.dubbo.common.bytecode.NoSuchPropertyException("Not found property \""+$2+"\" field or getter method in class org.apache.dubbo.demo.DemoService."); }
        cc.addMethod(c3.toString());
        // public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ org.apache.dubbo.demo.DemoService w; try{ w = ((org.apache.dubbo.demo.DemoService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); } try{ if( "sayHello".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHello((java.lang.String)$4[0]); } if( "sayHelloAsync".equals( $2 )  &&  $3.length == 1 ) {  return ($w)w.sayHelloAsync((java.lang.String)$4[0]); } } catch(Throwable e) {      throw new java.lang.reflect.InvocationTargetException(e);  } throw new org.apache.dubbo.common.bytecode.NoSuchMethodException("Not found method \""+$2+"\" in class org.apache.dubbo.demo.DemoService."); }

        try {
            // 生成代理类字节码
            Class<?> wc = cc.toClass();

            // 给代理类属性赋值
            // setup static field.
            wc.getField("pts").set(null, pts);
            wc.getField("pns").set(null, pts.keySet().toArray(new String[0]));
            wc.getField("mns").set(null, mns.toArray(new String[0]));
            wc.getField("dmns").set(null, dmns.toArray(new String[0]));
            int ix = 0;
            for (Method m : ms.values()) {
                wc.getField("mts" + ix++).set(null, m.getParameterTypes());
            }

            // 实例化代理类
            return (Wrapper) wc.getDeclaredConstructor().newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            cc.release();
            ms.clear();
            mns.clear();
            dmns.clear();
        }
    }

    private static String arg(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (cl == Boolean.TYPE) {
                return "((Boolean)" + name + ").booleanValue()";
            }
            if (cl == Byte.TYPE) {
                return "((Byte)" + name + ").byteValue()";
            }
            if (cl == Character.TYPE) {
                return "((Character)" + name + ").charValue()";
            }
            if (cl == Double.TYPE) {
                return "((Number)" + name + ").doubleValue()";
            }
            if (cl == Float.TYPE) {
                return "((Number)" + name + ").floatValue()";
            }
            if (cl == Integer.TYPE) {
                return "((Number)" + name + ").intValue()";
            }
            if (cl == Long.TYPE) {
                return "((Number)" + name + ").longValue()";
            }
            if (cl == Short.TYPE) {
                return "((Number)" + name + ").shortValue()";
            }
            throw new RuntimeException("Unknown primitive type: " + cl.getName());
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }

    private static String args(Class<?>[] cs, String name) {
        int len = cs.length;
        if (len == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(arg(cs[i], name + "[" + i + "]"));
        }
        return sb.toString();
    }

    private static String propertyName(String pn) {
        return pn.length() == 1 || Character.isLowerCase(pn.charAt(1)) ? Character.toLowerCase(pn.charAt(0)) + pn.substring(1) : pn;
    }

    private static boolean hasMethods(Method[] methods) {
        if (methods == null || methods.length == 0) {
            return false;
        }
        for (Method m : methods) {
            if (m.getDeclaringClass() != Object.class) {
                return true;
            }
        }
        return false;
    }

    /**
     * get property name array.
     *
     * @return property name array.
     */
    abstract public String[] getPropertyNames();

    /**
     * get property type.
     *
     * @param pn property name.
     * @return Property type or nul.
     */
    abstract public Class<?> getPropertyType(String pn);

    /**
     * has property.
     *
     * @param name property name.
     * @return has or has not.
     */
    abstract public boolean hasProperty(String name);

    /**
     * get property value.
     *
     * @param instance instance.
     * @param pn       property name.
     * @return value.
     */
    abstract public Object getPropertyValue(Object instance, String pn) throws NoSuchPropertyException, IllegalArgumentException;

    /**
     * set property value.
     *
     * @param instance instance.
     * @param pn       property name.
     * @param pv       property value.
     */
    abstract public void setPropertyValue(Object instance, String pn, Object pv) throws NoSuchPropertyException, IllegalArgumentException;

    /**
     * get property value.
     *
     * @param instance instance.
     * @param pns      property name array.
     * @return value array.
     */
    public Object[] getPropertyValues(Object instance, String[] pns) throws NoSuchPropertyException, IllegalArgumentException {
        Object[] ret = new Object[pns.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = getPropertyValue(instance, pns[i]);
        }
        return ret;
    }

    /**
     * set property value.
     *
     * @param instance instance.
     * @param pns      property name array.
     * @param pvs      property value array.
     */
    public void setPropertyValues(Object instance, String[] pns, Object[] pvs) throws NoSuchPropertyException, IllegalArgumentException {
        if (pns.length != pvs.length) {
            throw new IllegalArgumentException("pns.length != pvs.length");
        }

        for (int i = 0; i < pns.length; i++) {
            setPropertyValue(instance, pns[i], pvs[i]);
        }
    }

    /**
     * get method name array.
     *
     * @return method name array.
     */
    abstract public String[] getMethodNames();

    /**
     * get method name array.
     *
     * @return method name array.
     */
    abstract public String[] getDeclaredMethodNames();

    /**
     * has method.
     *
     * @param name method name.
     * @return has or has not.
     */
    public boolean hasMethod(String name) {
        for (String mn : getMethodNames()) {
            if (mn.equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * invoke method.
     *
     * @param instance instance.
     * @param mn       method name.
     * @param types
     * @param args     argument array.
     * @return return value.
     */
    abstract public Object invokeMethod(Object instance, String mn, Class<?>[] types, Object[] args) throws NoSuchMethodException, InvocationTargetException;
}

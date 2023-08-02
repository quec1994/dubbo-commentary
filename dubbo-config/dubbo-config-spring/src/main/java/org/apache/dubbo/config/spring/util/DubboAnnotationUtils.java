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
package org.apache.dubbo.config.spring.util;

import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.annotation.Service;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.spring.util.AnnotationUtils.getAttribute;
import static org.springframework.util.ClassUtils.getAllInterfacesForClass;
import static org.springframework.util.ClassUtils.resolveClassName;
import static org.springframework.util.StringUtils.hasText;

/**
 * Dubbo Annotation Utilities Class
 *
 * @see org.springframework.core.annotation.AnnotationUtils
 * @since 2.5.11
 */
public class DubboAnnotationUtils {


    @Deprecated
    public static String resolveInterfaceName(Service service, Class<?> defaultInterfaceClass)
            throws IllegalStateException {

        String interfaceName;
        if (hasText(service.interfaceName())) {
            interfaceName = service.interfaceName();
        } else if (!void.class.equals(service.interfaceClass())) {
            interfaceName = service.interfaceClass().getName();
        } else if (defaultInterfaceClass.isInterface()) {
            interfaceName = defaultInterfaceClass.getName();
        } else {
            throw new IllegalStateException(
                    "The @Service undefined interfaceClass or interfaceName, and the type "
                            + defaultInterfaceClass.getName() + " is not a interface.");
        }

        return interfaceName;

    }

    /**
     * Resolve the interface name from {@link AnnotationAttributes}
     *
     * @param attributes            {@link AnnotationAttributes} instance, may be {@link Service @Service} or {@link Reference @Reference}
     * @param defaultInterfaceClass the default {@link Class class} of interface
     * @return the interface name if found
     * @throws IllegalStateException if interface name was not found
     */
    public static String resolveInterfaceName(AnnotationAttributes attributes, Class<?> defaultInterfaceClass) {
        Boolean generic = getAttribute(attributes, "generic");
        if (generic != null && generic) {
            // it's a generic reference
            String interfaceClassName = getAttribute(attributes, "interfaceName");
            Assert.hasText(interfaceClassName,
                    "@Reference interfaceName() must be present when reference a generic service!");
            return interfaceClassName;
        }
        return resolveServiceInterfaceClass(attributes, defaultInterfaceClass).getName();
    }

    /**
     * Resolve the {@link Class class} of Dubbo Service interface from the specified
     * {@link AnnotationAttributes annotation attributes} and annotated {@link Class class}.
     *
     * @param attributes            {@link AnnotationAttributes annotation attributes}
     * @param defaultInterfaceClass the annotated {@link Class class}.
     * @return the {@link Class class} of Dubbo Service interface
     * @throws IllegalArgumentException if can't resolved
     */
    public static Class<?> resolveServiceInterfaceClass(AnnotationAttributes attributes, Class<?> defaultInterfaceClass)
            throws IllegalArgumentException {

        ClassLoader classLoader = defaultInterfaceClass != null ? defaultInterfaceClass.getClassLoader() : Thread.currentThread().getContextClassLoader();
        // 从注解配置里获取服务的接口类
        Class<?> interfaceClass = getAttribute(attributes, "interfaceClass");

        // 注解的interfaceClass属性的默认配置是void.class
        if (void.class.equals(interfaceClass)) { // default or set void.class for purpose.

            interfaceClass = null;
            // 从注解配置里获取服务的接口全限定名
            String interfaceClassName = getAttribute(attributes, "interfaceName");

            if (hasText(interfaceClassName)) {
                if (ClassUtils.isPresent(interfaceClassName, classLoader)) {
                    // 加载接口类
                    interfaceClass = resolveClassName(interfaceClassName, classLoader);
                }
            }

        }

        // 没有通过注解配置服务对应的接口，或者配置的接口加载不到
        if (interfaceClass == null && defaultInterfaceClass != null) {
            // Find all interfaces from the annotated class
            // To resolve an issue : https://github.com/apache/dubbo/issues/3251
            // 服务提供者——拿出实现类上实现的所有接口
            // 服务消费者——defaultInterfaceClass
            Class<?>[] allInterfaces = getAllInterfacesForClass(defaultInterfaceClass);

            if (allInterfaces.length > 0) {
                // 取类实现的第1个接口
                // 服务提供者——这里有坑，如果服务的接口不是实现类implements的第一个接口，那注册到注册中心的接口就是错的，消费者就调不通
                interfaceClass = allInterfaces[0];
            }

        }

        Assert.notNull(interfaceClass,
                "@Service interfaceClass() or interfaceName() or interface class must be present!");

        Assert.isTrue(interfaceClass.isInterface(),
                "The annotated type must be an interface!");

        return interfaceClass;
    }

    @Deprecated
    public static String resolveInterfaceName(Reference reference, Class<?> defaultInterfaceClass)
            throws IllegalStateException {

        String interfaceName;
        if (!"".equals(reference.interfaceName())) {
            interfaceName = reference.interfaceName();
        } else if (!void.class.equals(reference.interfaceClass())) {
            interfaceName = reference.interfaceClass().getName();
        } else if (defaultInterfaceClass.isInterface()) {
            interfaceName = defaultInterfaceClass.getName();
        } else {
            throw new IllegalStateException(
                    "The @Reference undefined interfaceClass or interfaceName, and the type "
                            + defaultInterfaceClass.getName() + " is not a interface.");
        }

        return interfaceName;
    }

    /**
     * Resolve the parameters of {@link org.apache.dubbo.config.annotation.DubboService}
     * and {@link org.apache.dubbo.config.annotation.DubboReference} from the specified.
     * It iterate elements in order.The former element plays as key or key&value role, it would be
     * spilt if it contain specific string, for instance, ":" and "=". As for later element can't
     * be split in anytime.It will throw IllegalArgumentException If converted array length isn't
     * even number.
     * The convert cases below work in right way,which are best practice.
     * <p>
     * (array->map)
     * ["a","b"] ==> {a=b}
     * [" a "," b "] ==> {a=b}
     * ["a=b"] ==>{a=b}
     * ["a:b"] ==>{a=b}
     * ["a=b","c","d"] ==>{a=b,c=d}
     * ["a","a:b"] ==>{a=a:b}
     * </p>
     *
     * @param parameters
     * @return
     */
    public static Map<String, String> convertParameters(String[] parameters) {
        /*
            将字符串数组转换成Map，样例(以JSON结构表示)
            String[] ==> Map<String, String>
            ["a","b"] ==> {"a":"b"}
            [" a "," b "] ==> {"a":"b"}
            ["a=b"] ==> {"a":"b"}
            ["a:b"] ==> {"a":"b"}
            ["a=b","c","d"] ==> {"a":"b","c":"d"}
            ["a","a:b"] ==> {"a":"a:b"}
         */

        if (ArrayUtils.isEmpty(parameters)) {
            return null;
        }

        List<String> compatibleParameterArray = Arrays.stream(parameters)
                .map(String::trim)
                .reduce(new ArrayList<>(parameters.length), (list, parameter) ->
                        {
                            if (list.size() % 2 == 1) {
                                //value doesn't split
                                list.add(parameter);
                                return list;
                            }

                            String[] sp1 = parameter.split(":");
                            if (sp1.length > 0 && sp1.length % 2 == 0) {
                                //key split
                                list.addAll(Arrays.stream(sp1).map(String::trim).collect(Collectors.toList()));
                                return list;
                            }
                            sp1 = parameter.split("=");
                            if (sp1.length > 0 && sp1.length % 2 == 0) {
                                list.addAll(Arrays.stream(sp1).map(String::trim).collect(Collectors.toList()));
                                return list;
                            }
                            list.add(parameter);
                            return list;
                        }
                        , (a, b) -> a);

        return CollectionUtils.toStringMap(compatibleParameterArray.toArray(new String[0]));
    }
}

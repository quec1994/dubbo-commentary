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
package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.spring.beans.factory.annotation.ReferenceAnnotationBeanPostProcessor;
import org.apache.dubbo.config.spring.beans.factory.annotation.ServiceAnnotationPostProcessor;
import org.apache.dubbo.config.spring.context.DubboSpringInitializer;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;

/**
 * Dubbo {@link DubboComponentScan} Bean Registrar
 *
 * @see Service
 * @see DubboComponentScan
 * @see ImportBeanDefinitionRegistrar
 * @see ServiceAnnotationPostProcessor
 * @see ReferenceAnnotationBeanPostProcessor
 * @since 2.5.7
 */
public class DubboComponentScanRegistrar implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {

        // 注册DubboDeployApplicationListener，它实现了ApplicationListener接口，一旦Spring启动完后，就会进行服务暴露与服务引入
        // initialize dubbo beans
        // 初始化 dubbo bean
        DubboSpringInitializer.initialize(registry);

        // 拿到@DubboComponentScan注解所定义的包路径
        Set<String> packagesToScan = getPackagesToScan(importingClassMetadata);

        // 注册ServiceAnnotationPostProcessor，
        // 它实现了BeanDefinitionRegistryPostProcessor接口，所以在Spring启动时会调用postProcessBeanDefinitionRegistry方法
        // 该方法会注册DubboBootstrapApplicationListener监听器，一旦Spring启动完后，就会进行服务暴露
        // 然后会进行扫描，扫描@DubboService注解了的类，
        // 然后生成BeanDefinition（会生成两个，一个普通的bean，一个ServiceBean），后续的Spring周期中会生成Bean
        registerServiceAnnotationPostProcessor(packagesToScan, registry);
    }

    /**
     * Registers {@link ServiceAnnotationPostProcessor}
     *
     * @param packagesToScan packages to scan without resolving placeholders
     * @param registry       {@link BeanDefinitionRegistry}
     * @since 2.5.8
     */
    private void registerServiceAnnotationPostProcessor(Set<String> packagesToScan, BeanDefinitionRegistry registry) {

        // 构造BeanDefinitionBuilder
        BeanDefinitionBuilder builder = rootBeanDefinition(ServiceAnnotationPostProcessor.class);
        builder.addConstructorArgValue(packagesToScan);
        builder.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
        // 生成BeanDefinition
        AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
        // 注册BeanDefinition
        BeanDefinitionReaderUtils.registerWithGeneratedName(beanDefinition, registry);

    }

    private Set<String> getPackagesToScan(AnnotationMetadata metadata) {
        // get from @DubboComponentScan 
        Set<String> packagesToScan = getPackagesToScan0(metadata, DubboComponentScan.class, "basePackages", "basePackageClasses");

        // get from @EnableDubbo, compatible with spring 3.x
        if (packagesToScan.isEmpty()) {
            packagesToScan = getPackagesToScan0(metadata, EnableDubbo.class, "scanBasePackages", "scanBasePackageClasses");
        }

        if (packagesToScan.isEmpty()) {
            return Collections.singleton(ClassUtils.getPackageName(metadata.getClassName()));
        }
        return packagesToScan;
    }

    private Set<String> getPackagesToScan0(AnnotationMetadata metadata, Class annotationClass, String basePackagesName, String basePackageClassesName) {

        AnnotationAttributes attributes = AnnotationAttributes.fromMap(
                metadata.getAnnotationAttributes(annotationClass.getName()));
        if (attributes == null) {
            return Collections.emptySet();
        }

        Set<String> packagesToScan = new LinkedHashSet<>();
        // basePackages
        String[] basePackages = attributes.getStringArray(basePackagesName);
        packagesToScan.addAll(Arrays.asList(basePackages));
        // basePackageClasses
        Class<?>[] basePackageClasses = attributes.getClassArray(basePackageClassesName);
        for (Class<?> basePackageClass : basePackageClasses) {
            packagesToScan.add(ClassUtils.getPackageName(basePackageClass));
        }
        // value
        if (attributes.containsKey("value")) {
            String[] value = attributes.getStringArray("value");
            packagesToScan.addAll(Arrays.asList(value));
        }
        return packagesToScan;
    }

}

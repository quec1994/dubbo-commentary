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

import org.apache.dubbo.config.spring.beans.factory.annotation.ServiceClassPostProcessor;
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

import static org.apache.dubbo.config.spring.util.DubboBeanUtils.registerCommonBeans;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;

/**
 * Dubbo {@link DubboComponentScan} Bean Registrar
 *
 * @see Service
 * @see DubboComponentScan
 * @see ImportBeanDefinitionRegistrar
 * @see ServiceClassPostProcessor
 * @see ReferenceAnnotationBeanPostProcessor
 * @since 2.5.7
 */
public class DubboComponentScanRegistrar implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {

        // 拿到@DubboComponentScan注解所定义的包路径
        Set<String> packagesToScan = getPackagesToScan(importingClassMetadata);

        // 注册ServiceClassPostProcessor一个Bean
        // 实现了BeanDefinitionRegistryPostProcessor接口，所以在Spring启动时会调用postProcessBeanDefinitionRegistry方法
        // 该方法会注册DubboBootstrapApplicationListener监听器，一旦Spring启动完后，就会进行服务导出
        // 然后会进行扫描，扫描@DubboService注解了的类，然后生成BeanDefinition（会生成两个，一个普通的bean，一个ServiceBean），后续的Spring周期中会生成Bean
        registerServiceClassPostProcessor(packagesToScan, registry);

        // @since 2.7.6 Register the common beans
        // 注册注册其他通用的Bean
        registerCommonBeans(registry);
    }

    /**
     * Registers {@link ServiceClassPostProcessor}
     *
     * @param packagesToScan packages to scan without resolving placeholders
     * @param registry       {@link BeanDefinitionRegistry}
     * @since 2.5.8
     */
    private void registerServiceClassPostProcessor(Set<String> packagesToScan, BeanDefinitionRegistry registry) {
        // 生成一个RootBeanDefinition，对应的beanClass为ServiceClassPostProcessor.class
        BeanDefinitionBuilder builder = rootBeanDefinition(ServiceClassPostProcessor.class);
        // 将包路径作为在构造ServiceClassPostProcessor时调用构造方法时的传入参数
        builder.addConstructorArgValue(packagesToScan);
        builder.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
        AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
        BeanDefinitionReaderUtils.registerWithGeneratedName(beanDefinition, registry);

    }

    private Set<String> getPackagesToScan(AnnotationMetadata metadata) {
        AnnotationAttributes attributes = AnnotationAttributes.fromMap(
                metadata.getAnnotationAttributes(DubboComponentScan.class.getName()));
        String[] basePackages = attributes.getStringArray("basePackages");
        Class<?>[] basePackageClasses = attributes.getClassArray("basePackageClasses");
        String[] value = attributes.getStringArray("value");
        // Appends value array attributes
        // 添加@DubboComponentScan注解中直接定义的包路径
        Set<String> packagesToScan = new LinkedHashSet<String>(Arrays.asList(value));
        // 添加@DubboComponentScan注解中直接定义的包路径
        packagesToScan.addAll(Arrays.asList(basePackages));
        for (Class<?> basePackageClass : basePackageClasses) {
            // 添加@DubboComponentScan注解中通过基础类间接定义的包路径
            packagesToScan.add(ClassUtils.getPackageName(basePackageClass));
        }
        if (packagesToScan.isEmpty()) {
            // 如果@DubboComponentScan注解中没有定义包路径，则使用被@DubboComponentScan注解的类的包路径
            return Collections.singleton(ClassUtils.getPackageName(metadata.getClassName()));
        }
        return packagesToScan;
    }

}

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
package org.apache.dubbo.config.spring.beans.factory.config;

import com.alibaba.spring.beans.factory.config.GenericBeanPostProcessorAdapter;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.AbstractConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.MergedBeanDefinitionPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.CommonAnnotationBeanPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;

import javax.annotation.PostConstruct;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Arrays;

import static com.alibaba.spring.util.ObjectUtils.of;
import static org.springframework.aop.support.AopUtils.getTargetClass;
import static org.springframework.beans.BeanUtils.getPropertyDescriptor;
import static org.springframework.util.ReflectionUtils.invokeMethod;

/**
 * The {@link BeanPostProcessor} class for the default property value of {@link AbstractConfig Dubbo's Config Beans}
 *
 * @since 2.7.6
 */
public class DubboConfigDefaultPropertyValueBeanPostProcessor extends GenericBeanPostProcessorAdapter<AbstractConfig>
        implements MergedBeanDefinitionPostProcessor, PriorityOrdered {

    /**
     * The bean name of {@link DubboConfigDefaultPropertyValueBeanPostProcessor}
     */
    public static final String BEAN_NAME = "dubboConfigDefaultPropertyValueBeanPostProcessor";

    protected void processBeforeInitialization(AbstractConfig dubboConfigBean, String beanName) throws BeansException {
        // [Feature] https://github.com/apache/dubbo/issues/5721
        // 如果ConfigBean没有配置id，则把beanName赋值给id
        setBeanNameAsDefaultValue(dubboConfigBean, "id", beanName);
        if (dubboConfigBean instanceof ProtocolConfig) {
            // 如果ProtocolConfig Bean没有配置name，则把name属性赋值为 dubbo
            ProtocolConfig config = (ProtocolConfig) dubboConfigBean;
            if (StringUtils.isEmpty(config.getName())) {
                config.setName("dubbo");
            }
        } else {
            // 如果ConfigBean没有配置name，则把beanName赋值给name
            setBeanNameAsDefaultValue(dubboConfigBean, "name", beanName);
        }
    }

    @Override
    public void postProcessMergedBeanDefinition(RootBeanDefinition beanDefinition, Class<?> beanType, String beanName) {
        // DO NOTHING
    }

    protected void setBeanNameAsDefaultValue(Object bean, String propertyName, String beanName) {

        Class<?> beanClass = getTargetClass(bean);

        PropertyDescriptor propertyDescriptor = getPropertyDescriptor(beanClass, propertyName);

        if (propertyDescriptor != null) { // the property is present
            // Bean的属性中存在propertyName属性

            Method getterMethod = propertyDescriptor.getReadMethod();

            if (getterMethod == null) { // if The getter method is absent

                return;
            }
            // Bean的方法中存在获取propertyName方法

            Object propertyValue = invokeMethod(getterMethod, bean);

            if (propertyValue != null) { // If The return value of "getName" method is not null
                return;
            }
            // Bean的propertyName属性没有值

            Method setterMethod = propertyDescriptor.getWriteMethod();
            if (setterMethod != null) { // the getter and setter methods are present
                // Bean的方法中存在给propertyName赋值方法
                if (Arrays.equals(of(String.class), setterMethod.getParameterTypes())) { // the param type is String
                    // Bean的propertyName属性是String类型的

                    // set bean name to the value of the the property
                    // 将beanName赋值给Bean的propertyName属性
                    invokeMethod(setterMethod, bean, beanName);
                }
            }
        }

    }

    /**
     * @return Higher than {@link InitDestroyAnnotationBeanPostProcessor#getOrder()}
     * @see InitDestroyAnnotationBeanPostProcessor
     * @see CommonAnnotationBeanPostProcessor
     * @see PostConstruct
     */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}

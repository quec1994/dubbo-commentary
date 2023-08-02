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
package org.apache.dubbo.config.spring.beans.factory.annotation;

import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.annotation.Method;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.spring.ReferenceBean;
import org.apache.dubbo.config.spring.util.DubboAnnotationUtils;
import org.springframework.beans.propertyeditors.StringTrimmerEditor;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.validation.DataBinder;

import java.beans.PropertyEditorSupport;
import java.util.List;
import java.util.Map;

import static com.alibaba.spring.util.AnnotationUtils.getAttribute;
import static com.alibaba.spring.util.AnnotationUtils.getAttributes;
import static com.alibaba.spring.util.ObjectUtils.of;
import static org.apache.dubbo.config.spring.util.DubboAnnotationUtils.resolveServiceInterfaceClass;
import static org.apache.dubbo.config.spring.util.DubboBeanUtils.getOptionalBean;
import static org.springframework.core.annotation.AnnotationAttributes.fromMap;

/**
 * {@link ReferenceBean} Builder
 *
 * @since 2.5.7
 */
class ReferenceBeanBuilder extends AnnotatedInterfaceConfigBeanBuilder<ReferenceBean> {

    // Ignore those fields
    static final String[] IGNORE_FIELD_NAMES = of("application", "module", "consumer", "monitor", "registry");

    private ReferenceBeanBuilder(AnnotationAttributes attributes, ApplicationContext applicationContext) {
        super(attributes, applicationContext);
    }

    private void configureInterface(AnnotationAttributes attributes, ReferenceBean referenceBean) {
        // 解析@DubboRefrence注解中配置的generic属性
        Boolean generic = getAttribute(attributes, "generic");
        if (generic != null && generic) {
            // 泛化调用
            // it's a generic reference
            // 解析@DubboRefrence注解中配置的interfaceName属性
            String interfaceClassName = getAttribute(attributes, "interfaceName");
            Assert.hasText(interfaceClassName,
                    "@Reference interfaceName() must be present when reference a generic service!");
            // 赋值
            referenceBean.setInterface(interfaceClassName);
            return;
        }
        // 不是泛化调用

        // 取出@DubboRefrence注解中配置的服务接口的Class对象
        Class<?> serviceInterfaceClass = resolveServiceInterfaceClass(attributes, interfaceClass);

        Assert.isTrue(serviceInterfaceClass.isInterface(),
                "The class of field or method that was annotated @Reference is not an interface!");

        // 赋值
        referenceBean.setInterface(serviceInterfaceClass);

    }


    private void configureConsumerConfig(AnnotationAttributes attributes, ReferenceBean<?> referenceBean) {

        // 解析@DubboRefrence注解中配置的consumer属性
        String consumerBeanName = getAttribute(attributes, "consumer");

        // 从Spring容器获取ConsumerConfig的bean对象，如果没有配置那么就按照ConsumerConfig类型取
        ConsumerConfig consumerConfig = getOptionalBean(applicationContext, consumerBeanName, ConsumerConfig.class);

        // 赋值
        referenceBean.setConsumer(consumerConfig);

    }

    void configureMethodConfig(AnnotationAttributes attributes, ReferenceBean<?> referenceBean) {
        // 解析@DubboRefrence注解中配置的consumer属性
        Method[] methods = (Method[]) attributes.get("methods");
        // 将Method注解配置转换成MethodConfig对象
        List<MethodConfig> methodConfigs = MethodConfig.constructMethodConfig(methods);

        // 异步方法相关配置
        for (MethodConfig methodConfig : methodConfigs) {
            // 将方法执行器BeanName转换成Bean对象
            if (!StringUtils.isEmpty(methodConfig.getOninvoke())) {
                Object invokeRef = this.applicationContext.getBean((String) methodConfig.getOninvoke());
                methodConfig.setOninvoke(invokeRef);
            }
            // 方法正常返回处理器BeanName转换成Bean对象
            if (!StringUtils.isEmpty(methodConfig.getOnreturn())) {
                Object returnRef = this.applicationContext.getBean((String) methodConfig.getOnreturn());
                methodConfig.setOnreturn(returnRef);
            }
            // 方法抛异常处理器BeanName转换成Bean对象
            if (!StringUtils.isEmpty(methodConfig.getOnthrow())) {
                Object throwRef = this.applicationContext.getBean((String) methodConfig.getOnthrow());
                methodConfig.setOnthrow(throwRef);
            }
        }

        if (!methodConfigs.isEmpty()) {
            // 赋值
            referenceBean.setMethods(methodConfigs);
        }
    }

    @Override
    protected ReferenceBean doBuild() {
        return new ReferenceBean<Object>();
    }

    @Override
    protected void preConfigureBean(AnnotationAttributes attributes, ReferenceBean referenceBean) {
        Assert.notNull(interfaceClass, "The interface class must set first!");
        DataBinder dataBinder = new DataBinder(referenceBean);
        // Register CustomEditors for special fields
        // 为filter、listener 属性注册一个去空格的编辑器
        dataBinder.registerCustomEditor(String.class, "filter", new StringTrimmerEditor(true));
        dataBinder.registerCustomEditor(String.class, "listener", new StringTrimmerEditor(true));
        // 为parameters 属性注册一个字符串数组转Map的编辑器
        dataBinder.registerCustomEditor(Map.class, "parameters", new PropertyEditorSupport() {
            @Override
            public void setValue(Object value) {
                if (value instanceof String[]) {
                    // 将字符串数组转换成Map
                    value = DubboAnnotationUtils.convertParameters((String[]) value);
                }
                super.setValue(value);
            }
        });

        // Bind annotation attributes
        // 将 @DubboReference 配置信息和 referenceBean 属性绑定
        dataBinder.bind(new AnnotationPropertyValuesAdapter(attributes, applicationContext.getEnvironment(), IGNORE_FIELD_NAMES));

    }


    @Override
    protected String resolveModuleConfigBeanName(AnnotationAttributes attributes) {
        return getAttribute(attributes, "module");
    }

    @Override
    protected String resolveApplicationConfigBeanName(AnnotationAttributes attributes) {
        return getAttribute(attributes, "application");
    }

    @Override
    protected String[] resolveRegistryConfigBeanNames(AnnotationAttributes attributes) {
        return getAttribute(attributes, "registry");
    }

    @Override
    protected String resolveMonitorConfigBeanName(AnnotationAttributes attributes) {
        return getAttribute(attributes, "monitor");
    }

    @Override
    protected void postConfigureBean(AnnotationAttributes attributes, ReferenceBean bean) throws Exception {

        // 将spring上下文赋值给bean
        bean.setApplicationContext(applicationContext);

        // 给configBean的interfaceName属性赋值
        // 也就是给configBean配置服务接口类的名字
        configureInterface(attributes, bean);

        // 给configBean的consumer属性赋值，如果没有配置就使用公共的ConsumerConfig
        configureConsumerConfig(attributes, bean);

        // 给configBean的methods属性赋值
        configureMethodConfig(attributes, bean);

        // 初始化
        bean.afterPropertiesSet();

        applicationContext.getAutowireCapableBeanFactory().applyBeanPostProcessorsAfterInitialization(bean, beanName);

    }

    @Deprecated
    public static ReferenceBeanBuilder create(Reference reference, ClassLoader classLoader,
                                              ApplicationContext applicationContext) {
        return create(fromMap(getAttributes(reference, applicationContext.getEnvironment(), true)), applicationContext);
    }

    public static ReferenceBeanBuilder create(AnnotationAttributes attributes, ApplicationContext applicationContext) {
        return new ReferenceBeanBuilder(attributes, applicationContext);
    }
}

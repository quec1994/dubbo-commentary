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
package org.apache.dubbo.config.spring;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.MetricsConfig;
import org.apache.dubbo.config.ModuleConfig;
import org.apache.dubbo.config.MonitorConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.SslConfig;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.spring.extension.SpringExtensionFactory;
import org.apache.dubbo.config.support.Parameter;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import static org.springframework.beans.factory.BeanFactoryUtils.beansOfTypeIncludingAncestors;

/**
 * ReferenceFactoryBean
 */
public class ReferenceBean<T> extends ReferenceConfig<T> implements FactoryBean,
        ApplicationContextAware, InitializingBean, DisposableBean {

    private static final long serialVersionUID = 213195494150089726L;

    private transient ApplicationContext applicationContext;

    public ReferenceBean() {
        super();
    }

    public ReferenceBean(Reference reference) {
        super(reference);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        SpringExtensionFactory.addApplicationContext(applicationContext);
    }

    @Override
    public Object getObject() {
        // FactoryBean 接口的方法，spring执行自动注入的时候会调用这个方法
        return get();
    }

    @Override
    public Class<?> getObjectType() {
        return getInterfaceClass();
    }

    @Override
    @Parameter(excluded = true)
    public boolean isSingleton() {
        return true;
    }

    /**
     * Initializes there Dubbo's Config Beans before @Reference bean autowiring
     */
    private void prepareDubboConfigBeans() {
        // Refactor 2.7.9
        final boolean includeNonSingletons = true;
        final boolean allowEagerInit = false;
        beansOfTypeIncludingAncestors(applicationContext, ApplicationConfig.class, includeNonSingletons, allowEagerInit);
        beansOfTypeIncludingAncestors(applicationContext, ModuleConfig.class, includeNonSingletons, allowEagerInit);
        beansOfTypeIncludingAncestors(applicationContext, RegistryConfig.class, includeNonSingletons, allowEagerInit);
        beansOfTypeIncludingAncestors(applicationContext, ProtocolConfig.class, includeNonSingletons, allowEagerInit);
        beansOfTypeIncludingAncestors(applicationContext, MonitorConfig.class, includeNonSingletons, allowEagerInit);
        beansOfTypeIncludingAncestors(applicationContext, ProviderConfig.class, includeNonSingletons, allowEagerInit);
        beansOfTypeIncludingAncestors(applicationContext, ConsumerConfig.class, includeNonSingletons, allowEagerInit);
        beansOfTypeIncludingAncestors(applicationContext, ConfigCenterBean.class, includeNonSingletons, allowEagerInit);
        beansOfTypeIncludingAncestors(applicationContext, MetadataReportConfig.class, includeNonSingletons, allowEagerInit);
        beansOfTypeIncludingAncestors(applicationContext, MetricsConfig.class, includeNonSingletons, allowEagerInit);
        beansOfTypeIncludingAncestors(applicationContext, SslConfig.class, includeNonSingletons, allowEagerInit);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public void afterPropertiesSet() throws Exception {

        // Initializes Dubbo's Config Beans before @Reference bean autowiring
        // 在自动装配@Reference的bean之前触发spring初始化Dubbo的配置Bean
        prepareDubboConfigBeans();

        // lazy init by default.
        if (init == null) {
            // 没有配置的话，设置init默认值为false，进行延迟初始化
            init = false;
        }

        // eager init if necessary.
        // 如果没有主动配置init的话，在前面逻辑的作用下这边会返回false
        // 默认延迟初始化，后面会在 ReferenceAnnotationBeanPostProcessor.doGetInjectedBean 方法中
        // 调用 referenceBean.get() 去完成初始化
        if (shouldInit()) {
            // 初始化Bean，配置def生成invoke代理
            getObject();
        }
    }

    @Override
    public void destroy() {
        // do nothing
    }
}

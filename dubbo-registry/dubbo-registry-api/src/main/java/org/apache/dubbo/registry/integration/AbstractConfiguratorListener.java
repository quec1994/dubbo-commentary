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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.config.configcenter.ConfigChangeType;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.configurator.parser.ConfigParser;
import org.apache.dubbo.rpc.cluster.governance.GovernanceRuleRepository;

import java.util.Collections;
import java.util.List;

/**
 * AbstractConfiguratorListener
 */
public abstract class AbstractConfiguratorListener implements ConfigurationListener {
    private static final Logger logger = LoggerFactory.getLogger(AbstractConfiguratorListener.class);

    protected List<Configurator> configurators = Collections.emptyList();
    protected GovernanceRuleRepository ruleRepository = ExtensionLoader.getExtensionLoader(
            GovernanceRuleRepository.class).getDefaultExtension();

    // 在构造ProviderConfigurationListener和ServiceConfigurationListener都会调用到这个方法
    // 完成Listener自身订阅到对应的应用和服务
    // 订阅关系绑定完了之后，主动从动态配置中心获取一下对应的配置数据生成configurators，后面需要重写providerUrl
    protected final void initWith(String key) {
        // 添加Listener,进行了订阅
        ruleRepository.addListener(key, this);
        // 从配置中心ConfigCenter获取属于当前应用的动态配置数据，从zk中拿到原始数据(主动从配置中心获取数据)
        String rawConfig = ruleRepository.getRule(key, DynamicConfiguration.DEFAULT_GROUP);
        // 如果存在应用配置信息则根据配置信息生成Configurator
        if (!StringUtils.isEmpty(rawConfig)) {
            genConfiguratorsFromRawRule(rawConfig);
        }
    }

    protected final void stopListen(String key) {
        ruleRepository.removeListener(key, this);
    }

    @Override
    public void process(ConfigChangedEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info("Notification of overriding rule, change type is: " + event.getChangeType() +
                    ", raw config content is:\n " + event.getContent());
        }

        if (event.getChangeType().equals(ConfigChangeType.DELETED)) {
            configurators.clear();
        } else {
            // ADDED or MODIFIED
            if (!genConfiguratorsFromRawRule(event.getContent())) {
                return;
            }
        }

        notifyOverrides();
    }

    private boolean genConfiguratorsFromRawRule(String rawConfig) {
        try {
            // parseConfigurators will recognize app/service config automatically.
            // 根据节点内容去生成override://协议的URL，然后根据URL去生成Configurator， Configurator对象很重要，表示一个配置器，根据配置器可以去重写URL
            configurators = Configurator.toConfigurators(ConfigParser.parseConfigurators(rawConfig))
                    .orElse(configurators);
        } catch (Exception e) {
            logger.warn("Failed to parse raw dynamic config and it will not take effect, the raw config is: "
                    + rawConfig + ", cause: " + e.getMessage());
            return false;
        }
        return true;
    }

    protected abstract void notifyOverrides();

    public List<Configurator> getConfigurators() {
        return configurators;
    }

    public void setConfigurators(List<Configurator> configurators) {
        this.configurators = configurators;
    }
}

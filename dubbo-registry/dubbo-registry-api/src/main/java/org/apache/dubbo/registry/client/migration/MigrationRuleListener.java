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
package org.apache.dubbo.registry.client.migration;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.integration.RegistryProtocol;
import org.apache.dubbo.registry.integration.RegistryProtocolListener;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.support.migration.MigrationRule;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.Optional;
import java.util.Set;

import static org.apache.dubbo.common.constants.RegistryConstants.INIT;

@Activate
public class MigrationRuleListener implements RegistryProtocolListener, ConfigurationListener {
    private static final Logger logger = LoggerFactory.getLogger(MigrationRuleListener.class);

    private Set<MigrationRuleHandler> listeners = new ConcurrentHashSet<>();
    private DynamicConfiguration configuration;

    private volatile String rawRule;

    public MigrationRuleListener() {
        // 动态配置中心
        Optional<DynamicConfiguration> optional = ApplicationModel.getEnvironment().getDynamicConfiguration();

        if (optional.isPresent()) {
            this.configuration = optional.get();

            logger.info("Listening for migration rules on dataId-" + MigrationRule.RULE_KEY + " group-" + MigrationRule.DUBBO_SERVICEDISCOVERY_MIGRATION_GROUP);
            // 注册一个配置监听器
            // zookeeper 协议的配置中心调的是 TreePathDynamicConfiguration 类的 addListener 方法
            configuration.addListener(MigrationRule.RULE_KEY, MigrationRule.DUBBO_SERVICEDISCOVERY_MIGRATION_GROUP, this);
            // 获取配置中心上的配置信息
            // 获取的就是zookeeper上 /dubbo/config/MIGRATION/dubbo-demo-annotation-consumer.migration 节点中的内容
            rawRule = configuration.getConfig(MigrationRule.RULE_KEY, MigrationRule.DUBBO_SERVICEDISCOVERY_MIGRATION_GROUP);
            if (StringUtils.isEmpty(rawRule)) {
                // 没有动态配置，打个标
                rawRule = INIT;
            }

        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("config center is not configured!");
            }

            // 没有动态配置，打个标
            rawRule = INIT;
        }

        process(new ConfigChangedEvent(MigrationRule.RULE_KEY, MigrationRule.DUBBO_SERVICEDISCOVERY_MIGRATION_GROUP, rawRule));
    }

    @Override
    public synchronized void process(ConfigChangedEvent event) {
        // 动态配置变更的时候会调用这个方法
        // zookeeper协议连接的配置中心是通过 CacheListener.dataChanged 走到这个方法

        rawRule = event.getContent();
        if (StringUtils.isEmpty(rawRule)) {
            logger.warn("Received empty migration rule, will ignore.");
            return;
        }

        logger.info("Using the following migration rule to migrate:");
        logger.info(rawRule);

        if (CollectionUtils.isNotEmpty(listeners)) {
            // 监听到配置变更，重新做迁移
            listeners.forEach(listener -> listener.doMigrate(rawRule));
        }
    }

    @Override
    public synchronized void onExport(RegistryProtocol registryProtocol, Exporter<?> exporter) {

    }

    @Override
    public synchronized void onRefer(RegistryProtocol registryProtocol, ClusterInvoker<?> invoker, URL url) {
        MigrationInvoker<?> migrationInvoker = (MigrationInvoker<?>) invoker;

        // 创建一个迁移规则执行器
        MigrationRuleHandler<?> migrationListener = new MigrationRuleHandler<>(migrationInvoker);
        // 加到动态配置变更监听器集合中
        // 对象实例化的时候会监听zookeeper上 /dubbo/config/MIGRATION/dubbo-demo-annotation-consumer.migration 节点中的内容
        listeners.add(migrationListener);
        // 根据配置的规则执行迁移逻辑
        migrationListener.doMigrate(rawRule);
    }

    @Override
    public void onDestroy() {
        if (null != configuration) {
            configuration.removeListener(MigrationRule.RULE_KEY, MigrationRule.DUBBO_SERVICEDISCOVERY_MIGRATION_GROUP, this);
        }
    }
}

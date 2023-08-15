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
package org.apache.dubbo.rpc.cluster.router.condition.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigChangeType;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.router.AbstractRouter;
import org.apache.dubbo.rpc.cluster.router.condition.ConditionRouter;
import org.apache.dubbo.rpc.cluster.router.condition.config.model.ConditionRouterRule;
import org.apache.dubbo.rpc.cluster.router.condition.config.model.ConditionRuleParser;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract router which listens to dynamic configuration
 */
public abstract class ListenableRouter extends AbstractRouter implements ConfigurationListener {
    public static final String NAME = "LISTENABLE_ROUTER";
    private static final String RULE_SUFFIX = ".condition-router";

    private static final Logger logger = LoggerFactory.getLogger(ListenableRouter.class);
    private ConditionRouterRule routerRule;
    private List<ConditionRouter> conditionRouters = Collections.emptyList();

    public ListenableRouter(URL url, String ruleKey) {
        // 应用条件路由 和 服务条件路由 都会走这边进行初始化

        super(url);
        this.force = false;
        // ruleKey为服务名或应用名
        // 初始化，会绑定一个监听器，负责监听配置中心条件路由的修改，并且会主动从配置中心获取一下当前条件路由的数据并做解析
        this.init(ruleKey);
    }

    @Override
    public synchronized void process(ConfigChangedEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info("Notification of condition rule, change type is: " + event.getChangeType() +
                    ", raw rule is:\n " + event.getContent());
        }

        if (event.getChangeType().equals(ConfigChangeType.DELETED)) {
            // 如果是一个删除事件，则清空当前Router中的conditionRouters属性，表示当前Router对象中没有路由规则
            routerRule = null;
            conditionRouters = Collections.emptyList();
        } else {
            try {
                // 解析路由规则
                routerRule = ConditionRuleParser.parse(event.getContent());
                // 根据路由规则，生成ConditionRouter-条件路由对象，并赋值给当前Router对象的conditionRouters属性
                generateConditions(routerRule);
            } catch (Exception e) {
                logger.error("Failed to parse the raw condition rule and it will not take effect, please check " +
                        "if the condition rule matches with the template, the raw rule is:\n " + event.getContent(), e);
            }
        }
    }

    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        if (CollectionUtils.isEmpty(invokers) || conditionRouters.size() == 0) {
            return invokers;
        }

        // We will check enabled status inside each router.
        // 根据路由规则进行过滤
        for (Router router : conditionRouters) {
            invokers = router.route(invokers, url, invocation);
        }

        return invokers;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public boolean isForce() {
        return (routerRule != null && routerRule.isForce());
    }

    private boolean isRuleRuntime() {
        return routerRule != null && routerRule.isValid() && routerRule.isRuntime();
    }

    private void generateConditions(ConditionRouterRule rule) {
        if (rule != null && rule.isValid()) {
            this.conditionRouters = rule.getConditions()
                    .stream()
                    .map(condition -> new ConditionRouter(condition, rule.isForce(), rule.isEnabled()))
                    .collect(Collectors.toList());
        }
    }

    private synchronized void init(String ruleKey) {
        if (StringUtils.isEmpty(ruleKey)) {
            return;
        }
        // 应用名 + ".condition-router"，或 服务名 + ".condition-router"
        String routerKey = ruleKey + RULE_SUFFIX;
        // dubbo-demo-annotation-consumer.condition-router
        // org.apache.dubbo.demo.DemoService::.condition-router

        // 绑定一个监听器去监听配置中心routerKey对应的节点，当前类ListenableRouter就是一个监听器
        // 应用路由规则监听地址：/dubbo/config/dubbo/org.apache.dubbo.demo.DemoService::.condition-router
        // 服务路由规则监听地址：/dubbo/config/dubbo/dubbo-demo-annotation-consumer.condition-router

        ruleRepository.addListener(routerKey, this);
        // 绑定完监听器后，主动的从配置中心获取一下当前应用或服务的对应的路由规则配置
        String rule = ruleRepository.getRule(routerKey, DynamicConfiguration.DEFAULT_GROUP);
        if (StringUtils.isNotEmpty(rule)) {
            // 手动调用监听器执行配置变更事件的方法
            this.process(new ConfigChangedEvent(routerKey, DynamicConfiguration.DEFAULT_GROUP, rule));
        }
    }
}

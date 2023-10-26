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
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.status.reporter.FrameworkStatusReportService;
import org.apache.dubbo.registry.client.migration.model.MigrationRule;
import org.apache.dubbo.registry.client.migration.model.MigrationStep;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_NO_PARAMETERS_URL;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;

public class MigrationRuleHandler<T> {
    public static final String DUBBO_SERVICEDISCOVERY_MIGRATION = "dubbo.application.migration.step";
    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(MigrationRuleHandler.class);

    private final MigrationClusterInvoker<T> migrationInvoker;
    private volatile MigrationStep currentStep;
    private volatile Float currentThreshold = 0f;
    private final URL consumerURL;

    public MigrationRuleHandler(MigrationClusterInvoker<T> invoker, URL url) {
        this.migrationInvoker = invoker;
        this.consumerURL = url;
    }

    public synchronized void doMigrate(MigrationRule rule) {
        if (migrationInvoker instanceof ServiceDiscoveryMigrationInvoker) {
            refreshInvoker(MigrationStep.FORCE_APPLICATION, 1.0f, rule);
            return;
        }

        // initial step : APPLICATION_FIRST
        MigrationStep step = MigrationStep.APPLICATION_FIRST;
        float threshold = -1f;

        try {
            step = rule.getStep(consumerURL);
            threshold = rule.getThreshold(consumerURL);
        } catch (Exception e) {
            logger.error(REGISTRY_NO_PARAMETERS_URL, "", "", "Failed to get step and threshold info from rule: " + rule, e);
        }

        if (refreshInvoker(step, threshold, rule)) {
            // refresh success, update rule
            setMigrationRule(rule);
        }
    }

    private boolean refreshInvoker(MigrationStep step, Float threshold, MigrationRule newRule) {
        if (step == null || threshold == null) {
            throw new IllegalStateException("Step or threshold of migration rule cannot be null");
        }
        MigrationStep originStep = currentStep;

        // 如果step发生了改变，或者step没有变化但是threshold发生了变化，则判断到底使用什么级别的服务提供者（接口还是应用）
        // 比如step本来是APPLICATION_FIRST，现在改为了FORCE_APPLICATION，就表示当前消费者应用在使用当前服务时，只使用应用级地址了，那就需要销毁之前的接口级Invoker，从而释放一些所占用的资源
        if ((currentStep == null || currentStep != step) || !currentThreshold.equals(threshold)) {
            boolean success = true;
            switch (step) {
                case APPLICATION_FIRST:
                    // 先进行接口级服务引入得到对应的ClusterInvoker并赋值给invoker属性
                    // 再进行应用级服务引入得到对应的ClusterInvoker并赋值给serviceDiscoveryInvoker属性
                    // 最后再根据两者的数量判断到底用哪个，并且把确定的ClusterInvoker赋值给currentAvailableInvoker属性
                    migrationInvoker.migrateToApplicationFirstInvoker(newRule);
                    break;
                case FORCE_APPLICATION:
                    // 只进行应用级服务引入得到对应的ClusterInvoker并赋值给serviceDiscoveryInvoker和currentAvailableInvoker属性
                    success = migrationInvoker.migrateToForceApplicationInvoker(newRule);
                    break;
                case FORCE_INTERFACE:
                default:
                    // 只进行接口级服务引入得到对应的ClusterInvoker并赋值给调用程序和currentAvailableInvoker属性
                    success = migrationInvoker.migrateToForceInterfaceInvoker(newRule);
            }

            if (success) {
                setCurrentStepAndThreshold(step, threshold);
                logger.info("Succeed Migrated to " + step + " mode. Service Name: " + consumerURL.getDisplayServiceKey());
                report(step, originStep, "true");
            } else {
                // migrate failed, do not save new step and rule
                logger.warn(INTERNAL_ERROR, "unknown error in registry module", "", "Migrate to " + step + " mode failed. Probably not satisfy the threshold you set "
                    + threshold + ". Please try re-publish configuration if you still after check.");
                report(step, originStep, "false");
            }

            return success;
        }
        // ignore if step is same with previous, will continue override rule for MigrationInvoker
        return true;
    }

    private void report(MigrationStep step, MigrationStep originStep, String success) {
        FrameworkStatusReportService reportService =
            consumerURL.getOrDefaultApplicationModel().getBeanFactory().getBean(FrameworkStatusReportService.class);

        if (reportService.hasReporter()) {
            reportService.reportMigrationStepStatus(
                reportService.createMigrationStepReport(consumerURL.getServiceInterface(), consumerURL.getVersion(),
                    consumerURL.getGroup(), String.valueOf(originStep), String.valueOf(step), success));
        }
    }

    private void setMigrationRule(MigrationRule rule) {
        this.migrationInvoker.setMigrationRule(rule);
    }

    private void setCurrentStepAndThreshold(MigrationStep currentStep, Float currentThreshold) {
        if (currentThreshold != null) {
            this.currentThreshold = currentThreshold;
        }
        if (currentStep != null) {
            this.currentStep = currentStep;
            this.migrationInvoker.setMigrationStep(currentStep);
        }
    }

    // for test purpose
    public MigrationStep getMigrationStep() {
        return currentStep;
    }
}

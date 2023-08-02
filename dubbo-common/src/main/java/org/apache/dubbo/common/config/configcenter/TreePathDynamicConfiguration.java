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
package org.apache.dubbo.common.config.configcenter;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.file.FileSystemDynamicConfiguration;
import org.apache.dubbo.common.utils.StringUtils;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.util.Collections.emptySortedSet;
import static java.util.Collections.unmodifiableSortedSet;
import static org.apache.dubbo.common.constants.CommonConstants.CONFIG_NAMESPACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;
import static org.apache.dubbo.common.utils.CollectionUtils.isEmpty;
import static org.apache.dubbo.common.utils.PathUtils.buildPath;
import static org.apache.dubbo.common.utils.PathUtils.normalize;

/**
 * An abstract implementation of {@link DynamicConfiguration} is like "tree-structure" path :
 * <ul>
 *     <li>{@link FileSystemDynamicConfiguration "file"}</li>
 *     <li>{@link org.apache.dubbo.configcenter.support.zookeeper.ZookeeperDynamicConfiguration "zookeeper"}</li>
 *     <li>{@link org.apache.dubbo.configcenter.consul.ConsulDynamicConfiguration "consul"}</li>
 * </ul>
 *
 * @see DynamicConfiguration
 * @see AbstractDynamicConfiguration
 * @since 2.7.8
 */
public abstract class TreePathDynamicConfiguration extends AbstractDynamicConfiguration {

    /**
     * The parameter name of URL for the config root path
     */
    public static final String CONFIG_ROOT_PATH_PARAM_NAME = PARAM_NAME_PREFIX + "root-path";

    /**
     * The parameter name of URL for the config base path
     */
    public static final String CONFIG_BASE_PATH_PARAM_NAME = PARAM_NAME_PREFIX + "base-path";

    /**
     * The default value of parameter of URL for the config base path
     */
    public static final String DEFAULT_CONFIG_BASE_PATH = "/config";

    private final String rootPath;

    public TreePathDynamicConfiguration(URL url) {
        super(url);
        // 默认 /dubbo/config
        this.rootPath = getRootPath(url);
    }

    public TreePathDynamicConfiguration(String rootPath,
                                        String threadPoolPrefixName,
                                        int threadPoolSize,
                                        long keepAliveTime,
                                        String group,
                                        long timeout) {
        super(threadPoolPrefixName, threadPoolSize, keepAliveTime, group, timeout);
        this.rootPath = rootPath;
    }

    @Override
    protected final String doGetConfig(String key, String group) throws Exception {
        // /dubbo/config + / + group + / + key
        String pathKey = buildPathKey(group, key);
        // /dubbo/config/dubbo/dubbo.properties
        return doGetConfig(pathKey);
    }

    @Override
    public final boolean publishConfig(String key, String group, String content) {
        String pathKey = buildPathKey(group, key);
        return Boolean.TRUE.equals(execute(() -> doPublishConfig(pathKey, content), getDefaultTimeout()));
    }

    @Override
    protected final boolean doRemoveConfig(String key, String group) throws Exception {
        String pathKey = buildPathKey(group, key);
        return doRemoveConfig(pathKey);
    }

    @Override
    public final void addListener(String key, String group, ConfigurationListener listener) {
        String pathKey = buildPathKey(group, key);
        doAddListener(pathKey, listener);
    }

    @Override
    public final void removeListener(String key, String group, ConfigurationListener listener) {
        String pathKey = buildPathKey(group, key);
        doRemoveListener(pathKey, listener);
    }

    @Override
    public final SortedSet<String> getConfigKeys(String group) throws UnsupportedOperationException {
        String groupPath = buildGroupPath(group);
        Collection<String> configKeys = doGetConfigKeys(groupPath);
        return isEmpty(configKeys) ? emptySortedSet() : unmodifiableSortedSet(new TreeSet<>(configKeys));
    }

    protected abstract boolean doPublishConfig(String pathKey, String content) throws Exception;

    protected abstract String doGetConfig(String pathKey) throws Exception;

    protected abstract boolean doRemoveConfig(String pathKey) throws Exception;

    protected abstract Collection<String> doGetConfigKeys(String groupPath);

    protected abstract void doAddListener(String pathKey, ConfigurationListener listener);

    protected abstract void doRemoveListener(String pathKey, ConfigurationListener listener);

    protected String buildGroupPath(String group) {
        // rootPath + / + group
        return buildPath(rootPath, group);
        // /dubbo/config/dubbo
    }

    protected String buildPathKey(String group, String key) {
        // rootPath + / + group + / + key
        return buildPath(buildGroupPath(group), key);
        // /dubbo/config/dubbo/dubbo.properties
    }

    /**
     * Get the root path from the specified {@link URL connection URl}
     *
     * @param url the specified {@link URL connection URl}
     * @return non-null
     */
    protected String getRootPath(URL url) {
        // URL参数 dubbo.config-center.root-path 配置根路径
        // 否则通过 buildRootPath 构造一个默认的
        String rootPath = url.getParameter(CONFIG_ROOT_PATH_PARAM_NAME, buildRootPath(url));

        // 如果是 //xxx/yyy?kkk=vvv 归一成 /xxx/yyy
        rootPath = normalize(rootPath);

        int rootPathLength = rootPath.length();

        // 去掉末尾的 /
        if (rootPathLength > 1 && rootPath.endsWith(PATH_SEPARATOR)) {
            rootPath = rootPath.substring(0, rootPathLength - 1);
        }

        return rootPath;
    }

    private String buildRootPath(URL url) {
        // 通过URL参数配置：/ + URL.parameter[namespace] + URL.parameter[dubbo.config-center.base-path]
        // 默认 / + dubbo + /config ，即 /dubbo/config
        return PATH_SEPARATOR + getConfigNamespace(url) + getConfigBasePath(url);
    }

    /**
     * Get the namespace from the specified {@link URL connection URl}
     *
     * @param url the specified {@link URL connection URl}
     * @return non-null
     */
    protected String getConfigNamespace(URL url) {
        // URL参数 namespace 配置命名空间
        // 默认 dubbo
        return url.getParameter(CONFIG_NAMESPACE_KEY, DEFAULT_GROUP);
    }

    /**
     * Get the config base path from the specified {@link URL connection URl}
     *
     * @param url the specified {@link URL connection URl}
     * @return non-null
     */
    protected String getConfigBasePath(URL url) {
        // URL参数 dubbo.config-center.base-path 配置基础路径
        // 没有配置的话默认为 /config
        String configBasePath = url.getParameter(CONFIG_BASE_PATH_PARAM_NAME, DEFAULT_CONFIG_BASE_PATH);
        if (StringUtils.isNotEmpty(configBasePath) && !configBasePath.startsWith(PATH_SEPARATOR)) {
            configBasePath = PATH_SEPARATOR + configBasePath;
        }
        return configBasePath;
    }
}

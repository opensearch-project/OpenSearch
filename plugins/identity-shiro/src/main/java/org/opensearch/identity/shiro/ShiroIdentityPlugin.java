/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.SecurityManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.identity.PluginSubject;
import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

/**
 * Identity implementation with Shiro
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ShiroIdentityPlugin extends Plugin implements IdentityPlugin {
    private Logger log = LogManager.getLogger(this.getClass());

    private final Settings settings;
    private final ShiroTokenManager authTokenHandler;

    private ThreadPool threadPool;

    /**
     * Create a new instance of the Shiro Identity Plugin
     *
     * @param settings settings being used in the configuration
     */
    public ShiroIdentityPlugin(final Settings settings) {
        this.settings = settings;
        authTokenHandler = new ShiroTokenManager();

        SecurityManager securityManager = new ShiroSecurityManager();
        SecurityUtils.setSecurityManager(securityManager);
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.threadPool = threadPool;
        return Collections.emptyList();
    }

    /**
     * Return a Shiro Subject based on the provided authTokenHandler and current subject
     *
     * @return The current subject
     */
    @Override
    public Subject getCurrentSubject() {
        return new ShiroSubject(authTokenHandler, SecurityUtils.getSubject());
    }

    /**
     * Return the Shiro Token Handler
     *
     * @return the Shiro Token Handler
     */
    @Override
    public TokenManager getTokenManager() {
        return this.authTokenHandler;
    }

    @Override
    public PluginSubject getPluginSubject(PluginInfo pluginInfo) {
        return new ShiroPluginSubject(threadPool);
    }
}

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
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.Subject;
import org.opensearch.identity.noop.NoopPluginSubject;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.plugins.IdentityAwarePlugin;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

/**
 * Identity implementation with Shiro
 *
 * @opensearch.experimental
 */
public final class ShiroIdentityPlugin extends Plugin implements IdentityPlugin {
    private Logger log = LogManager.getLogger(this.getClass());

    private final Settings settings;
    private final ShiroTokenManager authTokenHandler;

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

    /**
     * Return a Shiro Subject based on the provided authTokenHandler and current subject
     *
     * @return The current subject
     */
    @Override
    public Subject getSubject() {
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
    public void initializeIdentityAwarePlugins(List<IdentityAwarePlugin> identityAwarePlugins, ThreadPool threadPool) {
        if (identityAwarePlugins != null) {
            for (IdentityAwarePlugin plugin : identityAwarePlugins) {
                Subject subject = new NoopPluginSubject(threadPool);
                plugin.initializePlugin(subject);
            }
        }
    }
}

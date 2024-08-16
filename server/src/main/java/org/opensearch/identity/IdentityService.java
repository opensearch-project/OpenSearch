/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.noop.NoopIdentityPlugin;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.plugins.IdentityAwarePlugin;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Identity and access control for OpenSearch
 *
 * @opensearch.experimental
 * */
public class IdentityService {
    private static final Logger log = LogManager.getLogger(IdentityService.class);

    private final Settings settings;
    private final IdentityPlugin identityPlugin;

    public IdentityService(final Settings settings, final ThreadPool threadPool, final List<IdentityPlugin> identityPlugins) {
        this.settings = settings;

        if (identityPlugins.size() == 0) {
            log.debug("Identity plugins size is 0");
            identityPlugin = new NoopIdentityPlugin(threadPool);
        } else if (identityPlugins.size() == 1) {
            log.debug("Identity plugins size is 1");
            identityPlugin = identityPlugins.get(0);
        } else {
            throw new OpenSearchException(
                "Multiple identity plugins are not supported, found: "
                    + identityPlugins.stream().map(Object::getClass).map(Class::getName).collect(Collectors.joining(","))
            );
        }
    }

    /**
     * Gets the current UserSubject
     */
    public UserSubject getUserSubject() {
        return identityPlugin.getUserSubject();
    }

    /**
     * Gets the token manager
     */
    public TokenManager getTokenManager() {
        return identityPlugin.getTokenManager();
    }

    public void initializeIdentityAwarePlugins(final List<IdentityAwarePlugin> identityAwarePlugins) {
        if (identityAwarePlugins != null) {
            for (IdentityAwarePlugin plugin : identityAwarePlugins) {
                PluginSubject pluginSubject = identityPlugin.getPluginSubject((Plugin) plugin);
                plugin.assignSubject(pluginSubject);
            }
        }
    }
}

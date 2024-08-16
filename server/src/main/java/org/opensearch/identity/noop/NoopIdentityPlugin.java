/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.plugins.IdentityAwarePlugin;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

/**
 * Implementation of identity plugin that does not enforce authentication or authorization
 * <p>
 * This class and related classes in this package will not return nulls or fail access checks
 *
 * @opensearch.internal
 */
public class NoopIdentityPlugin implements IdentityPlugin {

    /**
     * Get the current subject
     * @return Must never return null
     */
    @Override
    public Subject getSubject() {
        return new NoopSubject();
    }

    /**
     * Get a new NoopTokenManager
     * @return Must never return null
     */
    @Override
    public TokenManager getTokenManager() {
        return new NoopTokenManager();
    }

    @Override
    public void initializeIdentityAwarePlugins(List<IdentityAwarePlugin> identityAwarePlugins, ThreadPool threadPool) {
        if (identityAwarePlugins != null) {
            for (IdentityAwarePlugin plugin : identityAwarePlugins) {
                Subject subject = new NoopPluginSubject(threadPool);
                plugin.assignSubject(subject);
            }
        }
    }
}

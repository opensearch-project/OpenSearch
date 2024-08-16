/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

/**
 * Plugin that provides identity and access control for OpenSearch
 *
 * @opensearch.experimental
 */
public interface IdentityPlugin {

    /**
     * Get the current subject.
     * @return Should never return null
     * */
    Subject getSubject();

    /**
     * Get the Identity Plugin's token manager implementation
     * @return Should never return null.
     */
    TokenManager getTokenManager();

    void initializeIdentityAwarePlugins(final List<IdentityAwarePlugin> systemIndexPlugins, ThreadPool threadPool);
}

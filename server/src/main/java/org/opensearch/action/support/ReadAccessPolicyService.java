/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.plugins.AccessPolicyProviderPlugin;

import java.util.List;
import java.util.Objects;

/** Node-level access to the installed {@link ReadAccessPolicyProvider}. */
public final class ReadAccessPolicyService {

    private final AccessPolicyProviderPlugin plugin;
    private volatile ReadAccessPolicyProvider provider;
    private volatile boolean initialized;

    public ReadAccessPolicyService(List<AccessPolicyProviderPlugin> plugins) {
        if (plugins.size() > 1) {
            throw new IllegalStateException("Only one AccessPolicyProviderPlugin may be installed, found [" + plugins.size() + "]");
        }
        this.plugin = plugins.isEmpty() ? null : plugins.getFirst();
    }

    /**
     * Resolves the provider after plugins have created their components. The service can be registered
     * before that point so plugins can receive it during their own component creation.
     */
    public synchronized void initialize() {
        if (initialized) {
            throw new IllegalStateException("ReadAccessPolicyService is already initialized");
        }
        provider = plugin == null
            ? null
            : Objects.requireNonNull(plugin.getReadAccessPolicyProvider(), "AccessPolicyProviderPlugin returned null");
        initialized = true;
    }

    /** Returns the effective policy, or an unrestricted policy when no provider is installed. */
    public DocumentLevelReadAccessPolicy getDocumentLevelReadAccessPolicy(ReadAccessContext context) {
        if (initialized == false) {
            throw new IllegalStateException("ReadAccessPolicyService is not initialized");
        }
        if (provider == null) {
            return DocumentLevelReadAccessPolicy.unrestricted();
        }
        return Objects.requireNonNull(provider.getDocumentLevelReadAccessPolicy(context), "ReadAccessPolicyProvider returned null");
    }
}

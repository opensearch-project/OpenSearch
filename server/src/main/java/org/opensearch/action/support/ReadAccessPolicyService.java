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

    private final ReadAccessPolicyProvider provider;

    public ReadAccessPolicyService(List<AccessPolicyProviderPlugin> plugins) {
        if (plugins.size() > 1) {
            throw new IllegalStateException("Only one AccessPolicyProviderPlugin may be installed, found [" + plugins.size() + "]");
        }
        this.provider = plugins.isEmpty() ? null : plugins.getFirst().getReadAccessPolicyProvider();
    }

    /** Returns the effective policy, or an unrestricted policy when no provider is installed. */
    public ReadAccessPolicy getReadAccessPolicy(ReadAccessContext context) {
        if (provider == null) {
            return ReadAccessPolicy.unrestricted();
        }
        return Objects.requireNonNull(provider.getReadAccessPolicy(context), "ReadAccessPolicyProvider returned null");
    }
}

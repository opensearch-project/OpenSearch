/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry mapping provider identifiers to their
 * {@link IndexFilterTreeProvider} implementations.
 * <p>
 * Used by the plan executor to look up the correct provider for
 * each {@code providerId} found in collector leaf nodes of the
 * boolean filter tree.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SearchBackendFactory {

    private final Map<Integer, IndexFilterTreeProvider<?, ?, ?>> providers = new ConcurrentHashMap<>();

    /**
     * Registers a provider under the given identifier.
     *
     * @param providerId the provider identifier (matches CollectorLeaf.providerId)
     * @param provider   the tree filter provider
     */
    public void register(int providerId, IndexFilterTreeProvider<?, ?, ?> provider) {
        providers.put(providerId, provider);
    }

    /**
     * Returns the provider registered under the given identifier.
     *
     * @param providerId the provider identifier
     * @return the registered provider
     * @throws IllegalArgumentException if no provider is registered for the given ID
     */
    public IndexFilterTreeProvider<?, ?, ?> getProvider(int providerId) {
        IndexFilterTreeProvider<?, ?, ?> provider = providers.get(providerId);
        if (provider == null) {
            throw new IllegalArgumentException("No IndexFilterTreeProvider registered for providerId: " + providerId);
        }
        return provider;
    }
}

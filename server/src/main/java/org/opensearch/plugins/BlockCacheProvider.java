/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Optional;

/**
 * SPI implemented by a {@link Plugin} that publishes a node-scoped
 * {@link BlockCache}.
 *
 * <p>Core resolves the cache at node boot by filtering plugins that implement
 * this interface. Consumers that want to use the cache discover it through
 * their own plugin hooks — this SPI only concerns publication, not fan-out.
 *
 * <p>Expected to be implemented by at most one plugin per node. If multiple
 * plugins publish a cache, core picks the first one discovered and logs a
 * warning.
 *
 * <p>Returning {@link Optional#empty()} is the same as not implementing the
 * interface at all — consumers see no cache and fall back to no-cache
 * behaviour. This lets implementing plugins no-op at runtime based on node
 * settings without changing their SPI participation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BlockCacheProvider {

    /**
     * Returns the node-scoped {@link BlockCache} published by this plugin, or
     * {@link Optional#empty()} if the plugin is present but has decided not to
     * publish a cache (e.g. cache disabled by settings).
     *
     * <p>Called at node boot, after {@code createComponents} completes.
     *
     * @return the cache, or {@link Optional#empty()}; never {@code null}
     */
    Optional<BlockCache> getBlockCache();
}

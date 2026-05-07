/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;

import java.util.Optional;

/**
 * SPI for plugins that publish a node-scoped {@link BlockCache}.
 *
 * <p>A plugin implements this interface to participate in two phases of
 * warm-node startup:
 *
 * <ol>
 *   <li><b>Budget phase</b> (before {@code createComponents}): core calls
 *       {@link #requestedCapacityBytes} on every registered provider and
 *       sums the results to determine how much SSD capacity to reserve for
 *       block caches. The remainder goes to the file cache.</li>
 *   <li><b>Construction phase</b> (inside {@code createComponents}): the plugin
 *       creates its {@link BlockCache} instance using the capacity it requested,
 *       then publishes it via {@link #getBlockCache()}. Core wires the instance
 *       into {@code NodeCacheOrchestrator} after {@code createComponents} returns.</li>
 * </ol>
 *
 * <p>Returning {@link Optional#empty()} from {@link #getBlockCache()} is equivalent
 * to not implementing this interface — consumers see no cache and fall back to
 * no-cache behaviour. This lets a plugin no-op at runtime (e.g. when capacity
 * is configured to zero) without changing its SPI participation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BlockCacheProvider {

    /**
     * Returns the SSD bytes this plugin wants from the total warm-cache budget.
     *
     * <p>Called before any {@code createComponents()} invocation so the budget
     * can be partitioned before other caches are created. The default returns
     * {@code 0} — no allocation requested.
     *
     * @param settings         the node settings
     * @param totalBudgetBytes total warm-cache SSD budget in bytes
     * @return bytes requested; must be &ge; 0
     */
    default long requestedCapacityBytes(Settings settings, long totalBudgetBytes) {
        return 0L;
    }

    /**
     * Returns the data-to-cache amplification ratio for this plugin's block cache.
     *
     * <p>Used by warm-node capacity reporting ({@code WarmFsService}) to estimate
     * how many bytes of remote data this cache's SSD reservation can serve.
     * A value of {@code 5.0} means one byte of cache SSD can serve five bytes of
     * remote data. The default is {@code 1.0} (no amplification).
     *
     * @param settings the node settings
     * @return the ratio; must be &ge; 1.0
     */
    default double dataToCapacityRatio(Settings settings) {
        return 1.0;
    }

    /**
     * Called by core after the budget phase to inform this plugin of the exact
     * bytes reserved for it. Plugins should store this value and use it in
     * {@code createComponents()} instead of re-deriving their capacity.
     *
     * <p>Called once, before {@code createComponents()}.
     *
     * @param bytes the exact SSD bytes reserved for this plugin's block cache
     */
    default void setReservedCapacityBytes(long bytes) {
        // Default no-op for backward compatibility.
    }

    /**
     * Returns the node-scoped {@link BlockCache} published by this plugin, or
     * {@link Optional#empty()} if no cache is available (e.g. capacity is zero).
     *
     * <p>Called after {@code createComponents} completes.
     *
     * @return the cache, or {@link Optional#empty()}; never {@code null}
     */
    Optional<BlockCache> getBlockCache();
}

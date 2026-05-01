/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Plugin interface for providing custom data format implementations.
 * Plugins implement this to register their data format (e.g., Parquet, Lucene)
 * with the DataFormatRegistry during node bootstrap.
 *
 * <p>There are two orthogonal pieces a plugin can contribute:
 * <ul>
 *   <li>{@link DataFormatDescriptor} via {@link #getFormatDescriptors} —
 *       <b>describes</b> the format (name, checksum strategy, static
 *       capabilities). Per-index value data.</li>
 *   <li>{@link StoreStrategy} via {@link #getStoreStrategy} —
 *       <b>behavior</b> for how the format participates in the tiered store
 *       (file ownership, remote layout, optional native registry).</li>
 * </ul>
 * A plugin may provide one, both, or neither.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatPlugin {

    /**
     * Returns the data format with its declared capabilities and priority.
     *
     * @return the data format descriptor
     */
    DataFormat getDataFormat();

    /**
     * Creates the indexing engine for the data format. This should be
     * instantiated per shard.
     */
    IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings);

    /**
     * Returns format descriptor suppliers for this plugin, filtered by the
     * given index settings. Each entry maps a format name to a
     * {@link Supplier} of its {@link DataFormatDescriptor}, deferring
     * descriptor object creation until the descriptor is actually needed.
     * Callers that only need format names can use {@code keySet()} without
     * triggering creation.
     */
    default Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(
        IndexSettings indexSettings,
        DataFormatRegistry dataFormatRegistry
    ) {
        return Map.of();
    }

    /**
     * Behavior describing how this format participates in the tiered store.
     *
     * <p>Plugins that need tiered-store support return a non-null strategy;
     * all other plumbing (per-shard lifecycle, seeding, routing, close) is
     * handled by the store layer.
     *
     * @return a strategy for this format, or {@code null} if the format does
     *         not participate in the tiered store
     */
    default StoreStrategy getStoreStrategy() {
        return null;
    }

    /**
     * Returns all strategies that apply for this plugin on the given index.
     *
     * <p>The default returns a singleton list containing
     * {@link #getStoreStrategy()} when it is non-null, otherwise an empty
     * list. Composite plugins (which expose multiple formats per index)
     * override this to contribute the strategies for every participating
     * format.
     *
     * @param indexSettings      the index settings
     * @param dataFormatRegistry the registry, used by composite plugins to
     *                           resolve sub-format plugins
     * @return the strategies that apply; never {@code null}
     */
    default List<StoreStrategy> getStoreStrategies(IndexSettings indexSettings, DataFormatRegistry dataFormatRegistry) {
        StoreStrategy s = getStoreStrategy();
        return s == null ? List.of() : List.of(s);
    }
}

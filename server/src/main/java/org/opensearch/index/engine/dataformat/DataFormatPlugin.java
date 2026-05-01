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
import org.opensearch.index.engine.exec.commit.Committer;

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
 *   <li>{@link StoreStrategy} via {@link #getStoreStrategies} —
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
     * Returns the data formats configured for use by the given index, in priority-walk order:
     * the primary format first (the format the index designates as authoritative), followed by
     * any secondary formats sorted by {@link DataFormat#priority()} ascending. Used by capability
     * coverage validation to scope checks to formats that actually participate in this index.
     *
     * <p>Single-format plugins return a singleton list containing their own format. Composite
     * plugins override this to return primary + secondaries in declared/priority order.
     *
     * @param indexSettings      the index settings
     * @param dataFormatRegistry the registry, used by composite plugins to resolve sub-format plugins
     * @return ordered list of configured formats; never {@code null}
     */
    default List<DataFormat> getConfiguredFormats(IndexSettings indexSettings, DataFormatRegistry dataFormatRegistry) {
        return List.of(getDataFormat());
    }

    /**
     * Returns the strategies describing how this format participates in the tiered store,
     * keyed by the format name the strategy applies to.
     *
     * <p>Most plugins contribute a single entry (their own format). Composite plugins,
     * which expose multiple formats per index, return one entry per participating format.
     * A plugin that does not participate in the tiered store returns an empty map (default).
     *
     * <p>All cross-cutting work (per-shard lifecycle, seeding, routing, close) is handled
     * by the store layer. Plugins only declare strategies here.
     *
     * @param indexSettings      the index settings
     * @param dataFormatRegistry the registry, used by composite plugins to resolve
     *                           sub-format plugins
     * @return the strategies that apply, keyed by data format; never {@code null}
     */
    default Map<DataFormat, StoreStrategy> getStoreStrategies(IndexSettings indexSettings, DataFormatRegistry dataFormatRegistry) {
        return Map.of();
    }

    /**
     * Returns the delete execution engine for this data format, or {@code null} if this
     * plugin does not support delete operations.
     *
     * @param committer the committer for durable delete tracking
     * @return the delete execution engine, or {@code null}
     */
    default DeleteExecutionEngine<?> getDeleteExecutionEngine(Committer committer) {
        return null;
    }
}

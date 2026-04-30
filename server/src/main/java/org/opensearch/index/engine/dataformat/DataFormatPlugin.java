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

import java.util.Map;
import java.util.function.Supplier;

/**
 * Plugin interface for providing custom data format implementations.
 * Plugins implement this to register their data format (e.g., Parquet, Lucene)
 * with the DataFormatRegistry during node bootstrap.
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
     * Creates the indexing engine for the data format. This should be instantiated per shard.
     *
     * @param settings          the engine initialization settings
     * @return the indexing execution engine instance
     */
    IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings);

    /**
     * Returns format descriptor suppliers for this plugin, filtered by the given index settings.
     * Each entry maps a format name to a {@link Supplier} of its {@link DataFormatDescriptor},
     * deferring descriptor object creation until the descriptor is actually needed.
     * Callers that only need format names can use {@code keySet()} without triggering creation.
     *
     * @param indexSettings the index settings used to determine active formats
     * @return map of format name to descriptor supplier
     */
    default Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(
        IndexSettings indexSettings,
        DataFormatRegistry dataFormatRegistry
    ) {
        return Map.of();
    }
}

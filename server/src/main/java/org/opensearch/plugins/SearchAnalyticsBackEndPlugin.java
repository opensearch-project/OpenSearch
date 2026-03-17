/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.IndexFilterProvider;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.engine.exec.SourceProvider;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.List;

/**
 * Interface for back-end query engines.
 *
 * @opensearch.internal
 */
public interface SearchAnalyticsBackEndPlugin {
    String name();

    List<DataFormat> getSupportedFormats();

    EngineReaderManager<?> createReaderManager(DataFormat format, ShardPath shardPath) throws IOException;

    /**
     * Create a search execution engine. Return null if this plugin is an index provider only.
     */
    default SearchExecEngine<?, ?> createSearchExecEngine(DataFormat format, ShardPath shardPath) throws IOException {
        return null;
    }

    /**
     * Create an index filter provider. Return null if this plugin is a search engine only.
     */
    default IndexFilterProvider<?, ?> createIndexFilterProvider(DataFormat format, ShardPath shardPath) throws IOException {
        return null;
    }

    /**
     * Create a source provider. Return null if this plugin does not provide source data.
     * <p>
     * A source provider executes the full query+scan+filter and streams back
     * result batches (projections, aggregations) to the primary engine.
     */
    default SourceProvider<?, ?> createSourceProvider(DataFormat format, ShardPath shardPath) throws IOException {
        return null;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.index.engine.exec.CatalogSnapshotAwareReaderManager;
import org.opensearch.index.engine.exec.IndexFilterProvider;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.engine.exec.SourceProvider;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.spi.vectorized.DataFormat;

import java.io.IOException;
import java.util.List;

/**
 * SPI extension point for pluggable search analytics back-end engines.
 *
 * @opensearch.internal
 */
public interface SearchAnalyticsBackEndPlugin {
    String name();

    List<DataFormat> getSupportedFormats();

    CatalogSnapshotAwareReaderManager<?> createReaderManager(DataFormat format, ShardPath shardPath) throws IOException;

    default SearchExecEngine<?, ?> createSearchExecEngine(DataFormat format, ShardPath shardPath) throws IOException {
        return null;
    }

    default IndexFilterProvider<?, ?> createIndexFilterProvider(DataFormat format, ShardPath shardPath) throws IOException {
        return null;
    }

    default SourceProvider<?, ?> createSourceProvider(DataFormat format, ShardPath shardPath) throws IOException {
        return null;
    }
}

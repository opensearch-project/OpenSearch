/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.CatalogSnapshotAwareReaderManager;
import org.opensearch.index.engine.exec.IndexFilterProvider;
import org.opensearch.index.engine.exec.IndexFilterTreeProvider;
import org.opensearch.index.engine.exec.SourceProvider;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.List;

/**
 * Plugin providing Lucene as an index filter or source provider.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchEnginePlugin implements SearchBackEndPlugin {

    /** Creates a new LuceneSearchEnginePlugin. */
    public LuceneSearchEnginePlugin() {}

    @Override
    public String name() {
        return "lucene-analytics-backend";
    }

    @Override
    public EngineReaderManager<?> createReaderManager(DataFormat format, ShardPath shardPath) throws IOException {
        return new LuceneReaderManager(format);
    }

    @Override
    public IndexFilterProvider<?, ?> createIndexFilterProvider(DataFormat format, ShardPath shardPath) throws IOException {
        return new LuceneIndexFilterProvider();
    }

    @Override
    public SourceProvider<?, ?> createSourceProvider(DataFormat format, ShardPath shardPath) throws IOException {
        return new LuceneSourceProvider();
    }

    @Override
    public IndexFilterTreeProvider<?, ?, ?> createIndexFilterTreeProvider(DataFormat format, ShardPath shardPath) throws IOException {
        return new LuceneIndexFilterTreeProvider();
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of();
    }
}

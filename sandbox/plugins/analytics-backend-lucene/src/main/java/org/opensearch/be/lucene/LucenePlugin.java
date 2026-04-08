/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterSettings;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Plugin providing Lucene as both a search back-end and a committer
 * for the composite engine.
 * <p>
 * Delegates to {@link LuceneSearchBackEnd} for reader management
 * and {@link LuceneEnginePlugin} for committer creation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LucenePlugin extends Plugin implements SearchBackEndPlugin<DirectoryReader>, EnginePlugin {

    /** Creates a new LucenePlugin. */
    public LucenePlugin() {}

    // --- SearchBackEndPlugin ---

    @Override
    public String name() {
        return "lucene-analytics-backend";
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of();
    }

    @Override
    public EngineReaderManager<DirectoryReader> createReaderManager(
        IndexStoreProvider indexStoreProvider,
        DataFormat format,
        ShardPath shardPath
    ) throws IOException {
        return LuceneSearchBackEnd.createReaderManager(indexStoreProvider, format, shardPath);
    }

    // --- EnginePlugin ---

    @Override
    public Optional<Committer> getCommitter(CommitterSettings committerSettings) throws IOException {
        return Optional.of(LuceneEnginePlugin.createCommitter(committerSettings));
    }
}

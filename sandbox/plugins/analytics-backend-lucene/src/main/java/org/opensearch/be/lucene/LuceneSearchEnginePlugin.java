/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Plugin providing Lucene as a search back-end and committer for the composite engine.
 * <p>
 * Implements:
 * <ul>
 *   <li>{@link EnginePlugin} — provides a {@link LuceneCommitter} for durable flush.</li>
 *   <li>{@link SearchBackEndPlugin} — provides {@link LuceneReaderManager} for search.</li>
 * </ul>
 * <p>
 * Both {@link #createReaderManager} accepts a {@link Committer}.
 * When the committer is a {@link LuceneCommitter}, the IndexWriter is extracted and shared
 * with the reader manager on a given shard.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchEnginePlugin implements SearchBackEndPlugin<OpenSearchDirectoryReader>, EnginePlugin {

    /** Creates a new LuceneSearchEnginePlugin. */
    public LuceneSearchEnginePlugin() {}

    @Override
    public String name() {
        return "lucene-analytics-backend";
    }

    // --- SearchBackEndPlugin ---

    @Override
    public EngineReaderManager<OpenSearchDirectoryReader> createReaderManager(Committer committer, DataFormat format, ShardPath shardPath)
        throws IOException {
        if (committer instanceof LuceneCommitter) {
            IndexWriter writer = ((LuceneCommitter) committer).getIndexWriter();
            if (writer != null) {
                OpenSearchDirectoryReader osReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer), shardPath.getShardId());
                return new LuceneReaderManager(format, osReader);
            }
        }
        throw new IllegalStateException("Cannot create LuceneReaderManager without an initialized LuceneCommitter");
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of();
    }

    // --- EnginePlugin ---

    @Override
    public Optional<Committer> getCommitter(IndexSettings indexSettings) {
        return Optional.of(new LuceneCommitter());
    }
}

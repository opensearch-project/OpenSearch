/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.StandardDirectoryReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.Optional;

/**
 * Static helpers for creating Lucene-based reader managers.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
final class LuceneSearchBackEnd {

    private LuceneSearchBackEnd() {}

    /**
     * Creates a {@link LuceneReaderManager} from the given provider.
     * Opens an NRT reader from the {@link org.apache.lucene.index.IndexWriter} when the
     * provider is a {@link LuceneIndexingExecutionEngine}, otherwise falls back to opening
     * a reader from the {@link org.opensearch.index.store.Store}'s directory.
     *
     * @param indexStoreProvider the store provider, must be present
     * @param format the data format
     * @param shardPath the shard path
     * @return a new reader manager
     * @throws IOException if reader creation fails
     */
    static EngineReaderManager<DirectoryReader> createReaderManager(
        Optional<IndexStoreProvider> indexStoreProvider,
        DataFormat format,
        ShardPath shardPath
    ) throws IOException {
        IndexStoreProvider provider = indexStoreProvider.orElseThrow(
            () -> new IllegalStateException("IndexStoreProvider is required to create LuceneReaderManager")
        );
        DirectoryReader directoryReader;
        if (provider instanceof LuceneIndexingExecutionEngine luceneProvider) {
            directoryReader = DirectoryReader.open(luceneProvider.getWriter());
        } else {
            directoryReader = StandardDirectoryReader.open(provider.getStore().directory());
        }
        return new LuceneReaderManager(format, directoryReader);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.opensearch.be.lucene.index.LuceneIndexingExecutionEngine;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static helpers for creating Lucene-based {@link EngineReaderManager} instances.
 * <p>
 * Called by {@link org.opensearch.be.lucene.LucenePlugin#createReaderManager} during
 * shard initialization. The factory method opens an NRT reader from the
 * {@link LuceneIndexingExecutionEngine}'s shared {@link org.apache.lucene.index.IndexWriter}
 * when available, falling back to a directory-based reader for read-only replicas.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
final class LuceneSearchBackEnd {

    private static final Logger logger = LogManager.getLogger(LuceneSearchBackEnd.class);

    private LuceneSearchBackEnd() {}

    /**
     * Creates a {@link LuceneReaderManager} from the given settings.
     * Opens an NRT reader from the {@link org.apache.lucene.index.IndexWriter} when the
     * provider is a {@link LuceneIndexingExecutionEngine}, otherwise falls back to opening
     * a reader from the {@link org.opensearch.index.store.Store}'s directory.
     *
     * @param settings the reader manager settings
     * @return a new reader manager
     * @throws IOException if reader creation fails
     */
    static EngineReaderManager<DirectoryReader> createReaderManager(ReaderManagerConfig settings) throws IOException {
        IndexStoreProvider provider = settings.indexStoreProvider()
            .orElseThrow(() -> new IllegalStateException("IndexStoreProvider is required to create LuceneReaderManager"));
        DirectoryReader directoryReader;
        Map<Long, DirectoryReader> readers = new ConcurrentHashMap<>();
        CheckedBiFunction<DirectoryReader, SegmentInfos, DirectoryReader, IOException> readerRefresher = null;
        if (provider.getStore(settings.format()) instanceof LuceneIndexingExecutionEngine.LuceneFormatStore luceneProvider) {
            directoryReader = DirectoryReader.open(luceneProvider.writer());
            readers = luceneProvider.readers();
            readerRefresher = (dr, sis) -> DirectoryReader.openIfChanged(dr);
        } else {
            logger.warn("Initialising it with a DirectorReader instead of a writer");
            directoryReader = StandardDirectoryReader.open(provider.getStore(settings.format()).store().directory());
            readerRefresher = LuceneSearchBackEnd::buildReader;
        }
        return new LuceneReaderManager(settings.format(), directoryReader, readers, readerRefresher);
    }

    private static DirectoryReader buildReader(DirectoryReader oldReader, SegmentInfos newSis) throws IOException {
        if (newSis == null || ((StandardDirectoryReader) oldReader).getSegmentInfos().version == newSis.version) {
            return null;
        }
        final List<LeafReader> subs = new ArrayList<>();
        for (LeafReaderContext ctx : oldReader.leaves()) {
            subs.add(ctx.reader());
        }
        // Segment_n here is ignored because it is either already committed on disk as part of previous commit point or
        // does not yet exist on store (not yet committed)
        return StandardDirectoryReader.open(oldReader.directory(), newSis, subs, null, null);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.index.engine.CombinedDeletionPolicy;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

public class LuceneCommitEngine implements Committer {

    private final Logger logger;
    private final IndexWriter indexWriter;
    private final CombinedDeletionPolicy combinedDeletionPolicy;
    private final Store store;

    public LuceneCommitEngine(Store store, TranslogDeletionPolicy translogDeletionPolicy, LongSupplier globalCheckpointSupplier)
        throws IOException {
        this.logger = Loggers.getLogger(LuceneCommitEngine.class, store.shardId());
        this.combinedDeletionPolicy = new CombinedDeletionPolicy(logger, translogDeletionPolicy, null, globalCheckpointSupplier);
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        indexWriterConfig.setIndexDeletionPolicy(combinedDeletionPolicy);
        this.store = store;
        this.indexWriter = new IndexWriter(store.directory(), indexWriterConfig);
    }

    @Override
    public void addLuceneIndexes(CatalogSnapshot catalogSnapshot) {
        Collection<WriterFileSet> luceneFileCollection = catalogSnapshot.getSearchableFiles(DataFormat.LUCENE.name());
        luceneFileCollection.forEach(writerFileSet -> {
            try {
                indexWriter.addIndexes(new NIOFSDirectory(Path.of(writerFileSet.getDirectory())));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CommitPoint commit(CatalogSnapshot catalogSnapshot) {

        if(catalogSnapshot == null) {
            catalogSnapshot = new CatalogSnapshot(new RefreshResult(), 0, 0);
        }
        addLuceneIndexes(catalogSnapshot);
        indexWriter.setLiveCommitData(commitData);
        try {
            indexWriter.commit();
            IndexCommit indexCommit = combinedDeletionPolicy.getLastCommit();
            return CommitPoint.builder()
                .commitFileName(indexCommit.getSegmentsFileName())
                .fileNames(indexCommit.getFileNames())
                .commitData(indexCommit.getUserData())
                .generation(indexCommit.getGeneration())
                .directory(Path.of(indexCommit.getSegmentsFileName()).getParent())
                .build();
        } catch (IOException e) {
            throw new RuntimeException("lucene commit engine failed", e);
        }
    }

    @Override
    public Map<String, String> getLastCommittedData() throws IOException {
        return store.readLastCommittedSegmentsInfo().getUserData();
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return this.combinedDeletionPolicy.getSafeCommitInfo();
    }

    /**
     * Acquires the most recent safe index commit snapshot.
     * All index files referenced by this commit won't be freed until the commit/snapshot is closed.
     * This method is required for replica recovery operations.
     */
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        try {
            // Use CombinedDeletionPolicy to acquire safe commit
            IndexCommit safeCommit = combinedDeletionPolicy.acquireIndexCommit(true);
            return new GatedCloseable<>(safeCommit, () -> {
                try {
                    combinedDeletionPolicy.releaseCommit(safeCommit);
                } catch (Exception e) {
                    logger.warn("Failed to release safe commit", e);
                }
            });
        } catch (Exception e) {
            throw new EngineException(store.shardId(), "Failed to acquire safe index commit", e);
        }
    }

    @Override
    public void close() throws IOException {
        this.indexWriter.close();
    }
}

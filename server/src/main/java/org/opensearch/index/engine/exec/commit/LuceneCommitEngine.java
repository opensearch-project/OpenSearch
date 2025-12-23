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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.index.engine.CombinedDeletionPolicy;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.function.LongSupplier;

public class LuceneCommitEngine implements Committer {

    private final Logger logger;
    private final IndexWriter indexWriter;
    private final CombinedDeletionPolicy combinedDeletionPolicy;
    private final Store store;
    private volatile SegmentInfos lastCommittedSegmentInfos;

    public LuceneCommitEngine(Store store, TranslogDeletionPolicy translogDeletionPolicy, LongSupplier globalCheckpointSupplier)
        throws IOException {
        this.logger = Loggers.getLogger(LuceneCommitEngine.class, store.shardId());
        this.combinedDeletionPolicy = new CombinedDeletionPolicy(logger, translogDeletionPolicy, null, globalCheckpointSupplier);
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        indexWriterConfig.setIndexDeletionPolicy(combinedDeletionPolicy);
        this.store = store;
        this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
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
    public synchronized CommitPoint commit(Iterable<Map.Entry<String, String>> commitData, CatalogSnapshot catalogSnapshot) {
        addLuceneIndexes(catalogSnapshot);
        indexWriter.setLiveCommitData(commitData);
        try {
            indexWriter.commit();
            IndexCommit indexCommit = combinedDeletionPolicy.getLastCommit();
            refreshLastCommittedSegmentInfos();
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

    private void refreshLastCommittedSegmentInfos() {
        store.incRef();
        try {
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        } catch (Exception e) {
            throw new RuntimeException("failed to read latest segment infos on commit", e);
        } finally {
            store.decRef();
        }
    }

    @Override
    public Map<String, String> getLastCommittedData() {
        return MapBuilder.<String, String>newMapBuilder().putAll(lastCommittedSegmentInfos.getUserData()).immutableMap();
    }

    @Override
    public CommitStats getCommitStats() {
        String segmentId = Base64.getEncoder().encodeToString(lastCommittedSegmentInfos.getId());
        // TODO: Implement numDocs
        return new CommitStats(lastCommittedSegmentInfos.getUserData(), lastCommittedSegmentInfos.getLastGeneration(), segmentId, 0);
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

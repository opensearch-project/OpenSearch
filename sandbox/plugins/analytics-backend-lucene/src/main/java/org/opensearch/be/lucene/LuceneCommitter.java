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
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.commit.SafeBootstrapCommitter;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Lucene-specific committer that owns the {@link IndexWriter} lifecycle.
 * Extends {@link SafeBootstrapCommitter} to enforce safe commit trimming on startup.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneCommitter extends SafeBootstrapCommitter {

    private static final Logger logger = LogManager.getLogger(LuceneCommitter.class);

    private final Store store;
    private final IndexWriter indexWriter;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    /**
     * Creates a new LuceneCommitter. Trims unsafe commits (via {@link SafeBootstrapCommitter}),
     * then opens the IndexWriter.
     *
     * @param committerConfig the committer committerConfig (shard path, index committerConfig, engine config, store)
     * @throws IOException if opening the IndexWriter fails
     */
    public LuceneCommitter(CommitterConfig committerConfig) throws IOException {
        super(committerConfig);
        this.store = Objects.requireNonNull(committerConfig.store());
        this.store.incRef();
        try {
            IndexWriterConfig iwc = createIndexWriterConfig(committerConfig.engineConfig());
            this.indexWriter = new IndexWriter(store.directory(), iwc);
        } catch (Exception e) {
            store.decRef();
            throw e;
        }
    }

    // --- SafeBootstrapCommitter abstract methods ---

    @Override
    protected List<CatalogSnapshot> discoverCommits(CommitterConfig config) throws IOException {
        Store s = config.store();
        if (s == null) {
            return List.of();
        }
        return deserializeCatalogSnapshots(s);
    }

    @Override
    protected void rewriteAtSafeCommit(CommitterConfig config, List<CatalogSnapshot> commits, CatalogSnapshot safeCommit)
        throws IOException {
        Store s = config.store();
        Function<String, String> resolver = fn -> s.shardPath().getDataPath().resolve(fn).toString();
        // TODO - s.directory() is being written at index/.. location but should be one level up
        List<IndexCommit> indexCommits = DirectoryReader.listCommits(s.directory());
        IndexCommit safeIndexCommit = null;
        for (IndexCommit ic : indexCommits) {
            String serialized = ic.getUserData().get(CatalogSnapshot.CATALOG_SNAPSHOT_KEY);
            if (serialized != null && serialized.isEmpty() == false) {
                CatalogSnapshot cs = DataformatAwareCatalogSnapshot.deserializeFromString(serialized, resolver);
                if (cs.getGeneration() == safeCommit.getGeneration()) {
                    safeIndexCommit = ic;
                    break;
                }
            }
        }
        if (safeIndexCommit == null) {
            return;
        }
        IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND)
            .setCommitOnClose(false)
            .setIndexCommit(safeIndexCommit);
        try (IndexWriter tempWriter = new IndexWriter(s.directory(), iwc)) {
            tempWriter.setLiveCommitData(safeIndexCommit.getUserData().entrySet());
            tempWriter.commit();
        }
    }

    // --- Committer interface ---

    @Override
    public synchronized void commit(Map<String, String> commitData) throws IOException {
        ensureOpen();
        indexWriter.setLiveCommitData(commitData.entrySet());
        indexWriter.commit();
    }

    @Override
    public List<CatalogSnapshot> listCommittedSnapshots() throws IOException {
        ensureOpen();
        return deserializeCatalogSnapshots(store);
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            indexWriter.close();
            this.store.decRef();
        }
    }

    @Override
    public Map<String, String> getLastCommittedData() throws IOException {
        ensureOpen();
        Iterable<Map.Entry<String, String>> liveCommitData = indexWriter.getLiveCommitData();
        if (liveCommitData == null) {
            return Map.of();
        }
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : liveCommitData) {
            result.put(entry.getKey(), entry.getValue());
        }
        return Map.copyOf(result);
    }

    @Override
    public CommitStats getCommitStats() {
        ensureOpen();
        try {
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(indexWriter.getDirectory());
            return new CommitStats(segmentInfos);
        } catch (IOException e) {
            logger.warn("Failed to read segment infos for commit stats", e);
            return null;
        }
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        throw new UnsupportedOperationException("TODO:: with index deleter");
    }

    /**
     * Returns the underlying IndexWriter.
     * Visible to other classes in this package (e.g., LuceneIndexingExecutionEngine).
     *
     * @return the index writer, or null if closed
     */
    IndexWriter getIndexWriter() {
        ensureOpen();
        return indexWriter;
    }

    // --- Internal ---

    private IndexWriterConfig createIndexWriterConfig(EngineConfig engineConfig) {
        if (engineConfig == null) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
            return iwc;
        }
        // TODO:: Merge Config needs to be wired in
        IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
        iwc.setCodec(engineConfig.getCodec());
        if (engineConfig.getSimilarity() != null) {
            iwc.setSimilarity(engineConfig.getSimilarity());
        }
        iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().getMbFrac());
        iwc.setUseCompoundFile(engineConfig.useCompoundFile());
        if (engineConfig.getIndexSort() != null) {
            iwc.setIndexSort(engineConfig.getIndexSort());
        }
        iwc.setCommitOnClose(false);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        return iwc;
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("LuceneCommitter is closed");
        }
    }

    /**
     * Deserializes CatalogSnapshots from Lucene IndexCommits in the given store.
     * Static-friendly — does not depend on instance fields.
     */
    static List<CatalogSnapshot> deserializeCatalogSnapshots(Store store) throws IOException {
        Function<String, String> resolver = fn -> store.shardPath().getDataPath().resolve(fn).toString();
        List<IndexCommit> commits;
        try {
            commits = DirectoryReader.listCommits(store.directory());
        } catch (org.apache.lucene.index.IndexNotFoundException e) {
            return List.of();
        }
        List<CatalogSnapshot> result = new ArrayList<>();
        for (IndexCommit ic : commits) {
            String serialized = ic.getUserData().get(CatalogSnapshot.CATALOG_SNAPSHOT_KEY);
            if (serialized != null && serialized.isEmpty() == false) {
                result.add(DataformatAwareCatalogSnapshot.deserializeFromString(serialized, resolver));
            }
        }
        return result;
    }
}

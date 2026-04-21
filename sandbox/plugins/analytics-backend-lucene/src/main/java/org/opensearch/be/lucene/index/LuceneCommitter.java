/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lucene-specific {@link Committer} that owns the shared {@link IndexWriter} lifecycle
 * for a single shard.
 * <p>
 * The shared writer is opened during construction using configuration from
 * {@link CommitterConfig} (analyzer, codec, similarity, RAM buffer, index sort, etc.).
 * All per-generation {@link LuceneWriter} instances produce segments in isolated temp
 * directories; those segments are later incorporated into this shared writer via
 * {@code IndexWriter.addIndexes} during refresh in {@link LuceneIndexingExecutionEngine}.
 * <p>
 * Commit data (catalog snapshot, translog UUID, sequence numbers) is persisted atomically
 * via {@link #commit(Map)}, which sets the live commit data on the writer and calls
 * {@link IndexWriter#commit()}.
 * <p>
 * The store reference is incremented on construction and decremented on {@link #close()}.
 * Closing the committer also closes the underlying IndexWriter.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneCommitter implements Committer {

    private static final Logger logger = LogManager.getLogger(LuceneCommitter.class);

    private final Store store;
    private final IndexWriter indexWriter;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    /**
     * Creates a new LuceneCommitter and opens the IndexWriter.
     *
     * @param committerConfig the committer committerConfig (shard path, index committerConfig, engine config, store)
     * @throws IOException if opening the IndexWriter fails
     */
    public LuceneCommitter(CommitterConfig committerConfig) throws IOException {
        this.store = Objects.requireNonNull(committerConfig.engineConfig().getStore());
        this.store.incRef();
        try {
            IndexWriterConfig iwc = createIndexWriterConfig(committerConfig.engineConfig());
            this.indexWriter = new IndexWriter(store.directory(), iwc);
        } catch (Exception e) {
            store.decRef();
            throw e;
        }
    }

    private IndexWriterConfig createIndexWriterConfig(EngineConfig engineConfig) {
        if (engineConfig == null) {
            return new IndexWriterConfig();
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
        return iwc;
    }

    /**
     * Atomically persists the given commit data (catalog snapshot, translog UUID,
     * sequence numbers) and commits the IndexWriter.
     *
     * @param commitData the key-value pairs to store as live commit data
     * @throws IOException if the commit fails
     * @throws IllegalStateException if this committer is closed
     */
    @Override
    public synchronized void commit(Map<String, String> commitData) throws IOException {
        ensureOpen();
        indexWriter.setLiveCommitData(commitData.entrySet());
        indexWriter.commit();
    }

    /**
     * Closes the IndexWriter and releases the store reference.
     * Subsequent calls are no-ops.
     *
     * @throws IOException if closing the IndexWriter fails
     */
    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            indexWriter.close();
            this.store.decRef();
        }
    }

    /**
     * Returns the last committed data as an unmodifiable map.
     * If no commit data has been set, returns an empty map.
     *
     * @return the last committed key-value pairs
     * @throws IOException if reading commit data fails
     * @throws IllegalStateException if this committer is closed
     */
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

    /**
     * Returns commit statistics derived from the latest committed segment infos.
     *
     * @return the commit stats, or {@code null} if segment infos cannot be read
     */
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

    /**
     * Not yet implemented. Will return safe commit info once the index deleter is wired in.
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always
     */
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

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("LuceneCommitter is closed");
        }
    }
}

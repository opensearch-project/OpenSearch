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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Lucene-specific {@link Committer} that owns the {@link IndexWriter} lifecycle.
 * <p>
 * The constructor takes {@link CommitterSettings} and opens the IndexWriter immediately.
 * No separate {@code init()} call is needed.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneCommitter implements Committer {

    private static final Logger logger = LogManager.getLogger(LuceneCommitter.class);

    /** Subdirectory under the shard data path where the Lucene index is stored. */
    static final String LUCENE_DIR_NAME = "lucene";

    private final Store store;
    private IndexWriter indexWriter;

    /**
     * Creates a new LuceneCommitter and opens the IndexWriter.
     *
     * @param settings the committer settings (shard path, index settings, engine config, store)
     * @throws IOException if opening the IndexWriter fails
     */
    public LuceneCommitter(CommitterSettings settings) throws IOException {
        this.store = settings.store();
        if (this.store == null) {
            throw new IllegalArgumentException("CommitterSettings must provide a non-null Store");
        }
        IndexWriterConfig iwc = createIndexWriterConfig(settings.engineConfig());
        this.indexWriter = new IndexWriter(store.directory(), iwc);
    }

    private IndexWriterConfig createIndexWriterConfig(EngineConfig engineConfig) {
        if (engineConfig == null) {
            return new IndexWriterConfig();
        }
        IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
        iwc.setCodec(engineConfig.getCodec());
        iwc.setSimilarity(engineConfig.getSimilarity());
        iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().getMbFrac());
        iwc.setUseCompoundFile(engineConfig.useCompoundFile());
        if (engineConfig.getIndexSort() != null) {
            iwc.setIndexSort(engineConfig.getIndexSort());
        }
        iwc.setCommitOnClose(false);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        return iwc;
    }

    // --- Committer ---

    @Override
    public void commit(Map<String, String> commitData) throws IOException {
        if (indexWriter == null) {
            throw new IllegalStateException("LuceneCommitter is closed");
        }
        indexWriter.setLiveCommitData(commitData.entrySet());
        indexWriter.commit();
    }

    @Override
    public void close() throws IOException {
        if (indexWriter != null) {
            indexWriter.close();
            indexWriter = null;
        }
    }

    @Override
    public Map<String, String> getLastCommittedData() throws IOException {
        if (indexWriter == null) {
            return Map.of();
        }
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
        if (indexWriter == null) {
            return null;
        }
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
        if (indexWriter == null) {
            return SafeCommitInfo.EMPTY;
        }
        try {
            Map<String, String> commitData = getLastCommittedData();
            long localCheckpoint = Long.parseLong(
                commitData.getOrDefault(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED))
            );
            int docCount = indexWriter.getDocStats().numDocs;
            return new SafeCommitInfo(localCheckpoint, docCount);
        } catch (IOException e) {
            logger.warn("Failed to get safe commit info", e);
            return SafeCommitInfo.EMPTY;
        }
    }

    // --- IndexWriter access (package-private for LuceneIndexingExecutionEngine) ---

    /**
     * Returns the underlying IndexWriter.
     * Visible to other classes in this package (e.g., LuceneIndexingExecutionEngine).
     *
     * @return the index writer, or null if closed
     */
    IndexWriter getIndexWriter() {
        return indexWriter;
    }
}

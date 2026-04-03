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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterSettings;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Lucene-specific {@link Committer} that owns the {@link IndexWriter} lifecycle.
 * <p>
 * Responsibilities:
 * <ul>
 *   <li>{@link #init} — opens the IndexWriter on the shard's Lucene directory</li>
 *   <li>{@link #commit} — serializes the CatalogSnapshot as Lucene commit userData</li>
 *   <li>{@link #close} — closes the IndexWriter</li>
 * </ul>
 * <p>
 * The IndexWriter is exposed via {@link #getIndexWriter()} so that
 * {@link LuceneIndexingExecutionEngine} (which handles {@code addIndexes} during refresh)
 * and {@link LuceneReaderManager} (which opens DirectoryReaders) can share the same writer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneCommitter implements Committer {

    private static final Logger logger = LogManager.getLogger(LuceneCommitter.class);

    /** Subdirectory under the shard data path where the Lucene index is stored. */
    static final String LUCENE_DIR_NAME = "lucene";

    private IndexWriter indexWriter;

    /** Creates a new LuceneCommitter. */
    public LuceneCommitter() {}

    @Override
    public void init(CommitterSettings settings) throws IOException {
        Path luceneDir = settings.shardPath().getDataPath().resolve(LUCENE_DIR_NAME);
        Files.createDirectories(luceneDir);
        Directory directory = FSDirectory.open(luceneDir);
        IndexWriterConfig iwc = createIndexWriterConfig(settings.engineConfig());
        this.indexWriter = new IndexWriter(directory, iwc);
    }

    /**
     * Creates an {@link IndexWriterConfig} from the engine configuration.
     * When an {@link EngineConfig} is provided, the analyzer, codec, index sort,
     * and similarity are taken from it. Otherwise a default config is used.
     */
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

    /**
     * Returns the underlying IndexWriter.
     * Used by {@link LuceneIndexingExecutionEngine} for {@code addIndexes} and
     * by {@link LuceneReaderManager} for opening DirectoryReaders.
     * Package-private — only accessible within the analytics-backend-lucene plugin.
     *
     * @return the IndexWriter, or null if not initialized
     */
    IndexWriter getIndexWriter() {
        return indexWriter;
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
}

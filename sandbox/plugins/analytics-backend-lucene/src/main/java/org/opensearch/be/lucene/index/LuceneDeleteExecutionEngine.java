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
import org.apache.lucene.index.Term;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.be.lucene.stats.LuceneStatsProvider;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.DeleterImpl;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.store.Store;
import org.opensearch.plugin.stats.DataFormatStatsProviderRegistry;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Lucene-based implementation of {@link DeleteExecutionEngine} that tracks per-generation
 * deleters paired with their corresponding writers. Each deleter delegates document
 * deletion to the underlying {@link LuceneWriter}.
 *
 * @opensearch.experimental
 */
public class LuceneDeleteExecutionEngine implements DeleteExecutionEngine<DataFormat> {

    private static final Logger logger = LogManager.getLogger(LuceneDeleteExecutionEngine.class);

    private final Map<Long, Deleter> generationToDeleterMap;
    private final DataFormat dataFormat;
    private final IndexWriter parentWriter;
    private final ConcurrentMap<String, GenRow> idToGen;
    private final Store store;

    /** Generation + insertion rowId where a document currently lives in an active child writer. */
    private record GenRow(long generation, long rowId) {
    }

    public LuceneDeleteExecutionEngine(DataFormat dataFormat, Committer committer) {
        this.generationToDeleterMap = new ConcurrentHashMap<>();
        this.idToGen = new ConcurrentHashMap<>();
        this.dataFormat = dataFormat;
        LuceneCommitter luceneCommitter = (LuceneCommitter) committer;
        this.parentWriter = luceneCommitter.getIndexWriter();
        this.store = luceneCommitter.getStore();
    }

    @Override
    public Deleter createDeleter(Writer<?> writer) {
        return writer.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME).map(w -> {
            LuceneWriter luceneWriter = (LuceneWriter) w;
            Deleter deleter = new DeleterImpl<>(luceneWriter);
            generationToDeleterMap.put(writer.generation(), deleter);
            return deleter;
        }).orElse(null);
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DeleteResult deleteDocument(DeleteInput deleteInput) throws IOException {
        long start = System.nanoTime();
        try {
            Deleter currentDeleter = generationToDeleterMap.get(deleteInput.generation());
            if (currentDeleter == null) {
                throw new IllegalArgumentException(
                    "Update/delete is not supported for this index: no delete-applicable data format "
                        + "(requires a format such as Lucene)"
                );
            }
            assert currentDeleter.isActive() : "current-gen deleter must be active while caller holds the writer lock; gen="
                + deleteInput.generation();

            currentDeleter.recordBufferedDeletes(deleteInput.id());
            GenRow previous = idToGen.get(deleteInput.id());
            if (previous != null) {
                Deleter previousDeleter = generationToDeleterMap.get(previous.generation());
                if (previousDeleter != null) {
                    previousDeleter.recordPositionalDelete(previous.rowId());
                    return new DeleteResult.Success(1L, 1L, 1L);
                }
            }
            return new DeleteResult.Success(1L, 1L, 1L);
        } finally {
            LuceneStatsProvider provider = (LuceneStatsProvider) DataFormatStatsProviderRegistry.INSTANCE.get(
                LuceneStatsProvider.FORMAT_NAME
            );
            if (provider != null) {
                LuceneShardStatsTracker tracker = provider.getTracker(store.shardId());
                if (tracker != null) {
                    tracker.incDeleteTotal();
                    tracker.addDeleteTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                }
            }
        }
    }

    @Override
    public DataFormat getDataFormat() {
        return this.dataFormat;
    }

    @Override
    public void close() throws IOException {
        // Engine shutdown: close all deleters and drop tracking.
        for (Deleter deleter : generationToDeleterMap.values()) {
            deleter.close();
        }

        generationToDeleterMap.clear();
        idToGen.clear();
    }

    @Override
    public void recordWrite(String id, long generation, long rowId) {
        idToGen.put(id, new GenRow(generation, rowId));
    }

    @Override
    public boolean onWriterCheckedOut(long generation) throws IOException {
        idToGen.entrySet().removeIf(e -> e.getValue().generation() == generation);

        Deleter deleter = generationToDeleterMap.remove(generation);
        if (deleter == null) {
            return false;
        }

        int totalApplied = 0;
        Queue<String> drained = deleter.deactivate();
        for (String deletedId : drained) {
            parentWriter.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId(deletedId)));
            totalApplied++;
        }

        return totalApplied > 0;
    }
}

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
import org.apache.lucene.util.RamUsageEstimator;
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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks per-generation Lucene deleters and document locations for updates and deletes.
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

    private static final long BASE_BYTES_PER_ID_TO_GEN_ENTRY = RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + RamUsageEstimator
        .shallowSizeOfInstance(GenRow.class);
    /** Maintained incrementally because {@link #ramBytesUsed()} must not scan {@link #idToGen}. */
    private final AtomicLong idToGenRamBytesUsed = new AtomicLong();

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

    /**
     * Registers a deleter for the writer's Lucene delegate. A missing delegate is tolerated until
     * the first update or delete.
     */
    @Override
    public Deleter createDeleter(Writer<?> writer) {
        // Resolve the Lucene delegate instead of casting the top-level writer, which may be composite.
        if (writer.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME).orElse(null) instanceof LuceneWriter luceneWriter) {
            Deleter deleter = new DeleterImpl<>(luceneWriter);
            generationToDeleterMap.put(writer.generation(), deleter);
            return deleter;
        }
        return null;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DeleteResult deleteDocument(DeleteInput deleteInput, Writer<?> writer) throws IOException {
        long start = System.nanoTime();
        try {
            Deleter currentDeleter = generationToDeleterMap.get(deleteInput.generation());
            if (currentDeleter == null) {
                // A missing deleter means either the index has no Lucene delegate or the generation
                // retired. The locked writer distinguishes these cases.
                if (writer.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME).isEmpty()) {
                    throw new IllegalArgumentException(
                        "Update/delete is not supported for this index: no delete-applicable data format "
                            + "(requires a format such as Lucene)"
                    );
                }
                // The generation retired; apply the late delete to the parent writer.
                parentWriter.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId(deleteInput.id())));
                recordPreviousPositionalDelete(deleteInput.id());
                return new DeleteResult.Success(1L, 1L, 1L);
            }
            assert currentDeleter.isActive() : "current-gen deleter must be active while caller holds the writer lock; gen="
                + deleteInput.generation();

            currentDeleter.recordBufferedDeletes(deleteInput.id());
            recordPreviousPositionalDelete(deleteInput.id());
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
        for (Deleter deleter : generationToDeleterMap.values()) {
            deleter.close();
        }

        generationToDeleterMap.clear();
        idToGen.clear();
        idToGenRamBytesUsed.set(0L);
    }

    @Override
    public void recordWrite(String id, long generation, long rowId) {
        if (idToGen.put(id, new GenRow(generation, rowId)) == null) {
            idToGenRamBytesUsed.addAndGet(entryBytes(id));
        }
    }

    /**
     * Returns heap used by active document-location and buffered-delete state. Location entries are
     * removed when their generation is checked out.
     */
    @Override
    public long ramBytesUsed() {
        long total = idToGenRamBytesUsed.get();
        for (Deleter deleter : generationToDeleterMap.values()) {
            total += deleter.ramBytesUsed();
        }
        return total;
    }

    private long entryBytes(String id) {
        return BASE_BYTES_PER_ID_TO_GEN_ENTRY + RamUsageEstimator.sizeOf(id);
    }

    @Override
    public boolean onWriterCheckedOut(long generation) throws IOException {
        // Conditional removal prevents concurrent retirement from subtracting an entry twice.
        idToGen.forEach((trackedId, genRow) -> {
            if (genRow.generation() == generation && idToGen.remove(trackedId, genRow)) {
                idToGenRamBytesUsed.addAndGet(-entryBytes(trackedId));
            }
        });

        Deleter deleter = generationToDeleterMap.remove(generation);
        if (deleter == null) {
            return false;
        }

        Queue<String> drained = deleter.deactivate();
        if (drained.isEmpty()) {
            return false;
        }

        Set<String> uniqueIds = new LinkedHashSet<>(drained);
        Term[] terms = new Term[uniqueIds.size()];
        int i = 0;
        for (String deletedId : uniqueIds) {
            terms[i++] = new Term(IdFieldMapper.NAME, Uid.encodeId(deletedId));
        }
        parentWriter.deleteDocuments(terms);

        return true;
    }

    /** Records a positional delete for the tracked previous copy, if its generation is active. */
    private void recordPreviousPositionalDelete(String id) {
        GenRow previous = idToGen.get(id);
        if (previous == null) {
            return;
        }
        Deleter previousDeleter = generationToDeleterMap.get(previous.generation());
        if (previousDeleter != null) {
            previousDeleter.recordPositionalDelete(previous.rowId());
        }
    }
}

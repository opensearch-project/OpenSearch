/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.stream;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class SourceLoader {

    private final IndexShard indexShard;
    private final Engine.Searcher searcher;
    private final ThreadPool threadPool;
    private final Query rewrittenQuery;
    private final MappedFieldType[] sortFieldTypes;
    private final int[] multipliers;
    private final MergeIterator mergeIterator;
    private final int batchSize;

    public SourceLoader(Engine.Searcher searcher, IndexShard indexShard, ThreadPool threadPool, Query rewrittenQuery, int batchSize,
                        MappedFieldType[] sortFieldTypes, int[] multipliers) throws IOException {
        this.searcher = searcher;
        this.indexShard = indexShard;
        this.threadPool = threadPool;
        this.rewrittenQuery = rewrittenQuery;
        this.batchSize = batchSize;
        this.sortFieldTypes = sortFieldTypes;
        this.multipliers = multipliers; // sorting order

        try (Engine.Searcher indexSearcher = indexShard.acquireSearcher("stream_source_loader")) {
            List<LeafReaderContext> leaves = indexSearcher.getIndexReader().leaves();
            FixedBitSet[] bitSets = new FixedBitSet[leaves.size()];
            ToBitSetCollector collector = new ToBitSetCollector(bitSets);
            indexSearcher.search(rewrittenQuery, collector); // TODO: use CollectorManager

            List<SegmentIterator> segmentIterators = new ArrayList<>();
            int segOrd = 0;
            for (int i = 0; i < leaves.size(); i++) {
                FixedBitSet bitSet = bitSets[i];
                segmentIterators.add(new SegmentIterator(bitSet, leaves.get(i),
                    segOrd++, batchSize, sortFieldTypes, multipliers));
            }
            this.mergeIterator = new MergeIterator(segmentIterators, batchSize);
        }
    }
    /**
     * Returns the next page of merged global document IDs.
     *
     * @return a list of global doc IDs.
     * @throws IOException if an I/O error occurs.
     */
    public List<Integer> nextPage() throws IOException {
        List<MultiSortDoc> mergedDocs = mergeIterator.nextBatch();
        // We can avoid this and have "mergeIterator.nextBatch()" produces docIds directly
        List<Integer> docIds = new ArrayList<>(mergedDocs.size());
        for (MultiSortDoc doc : mergedDocs) {
            docIds.add(doc.getDocId());
        }
        return docIds;
    }

    public boolean hasNext() {
        return mergeIterator.hasNext();
    }
    /**
     * Starts the asynchronous result stream.
     * The provided listener is called repeatedly with batches of fully fetched document results
     *  until no more batches remain.
     */
    public void startResultStream(final ActionListener<List<Tuple>> listener) {
        // Produce the initial batch asynchronously.
        produceNextBatch(new ActionListener<>() {
            @Override
            public void onResponse(List<MultiSortDoc> initialBatch) {
                processBatchCycle(initialBatch, listener);
            }
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Asynchronously produces the next batch of document IDs
     */
    private void produceNextBatch(final ActionListener<List<MultiSortDoc>> listener) {
        threadPool.executor(ThreadPool.Names.SEARCH).execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                List<MultiSortDoc> batch = null;
                if (mergeIterator.hasNext()) {
                    batch = mergeIterator.nextBatch();
                }
                listener.onResponse(batch);
            }
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
    /**
     * While we asynchronously fetch the current batch we compute the next one.
     * Executes one cycle of the pipeline:
     * - Asynchronously fetches document fields for the current batch.
     * - Concurrently computes the next batch of document IDs.
     * When both tasks complete, it delivers the fetched results to the client and recurses.
     */
    private void processBatchCycle(final List<MultiSortDoc> currentBatch,
                                   final ActionListener<List<Tuple>> listener) {
        if (currentBatch == null || currentBatch.isEmpty()) {
            // No more batches to process. Send EOF
            listener.onResponse(Collections.singletonList(new Tuple()));
            return;
        }

        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicReference<List<Tuple>> fetchResultsRef = new AtomicReference<>();
        final AtomicReference<List<MultiSortDoc>> nextBatchRef = new AtomicReference<>();

        // Task 1: Fetch document fields for the current batch
        fetchBatch(currentBatch, new ActionListener<>() {
            @Override
            public void onResponse(List<Tuple> fetchResults) {
                fetchResultsRef.set(fetchResults);
                latch.countDown();
            }
            @Override
            public void onFailure(Exception e) {
                latch.countDown();
                listener.onFailure(e);
            }
        });

        // Task 2: Produce the next batch concurrently
        produceNextBatch(new ActionListener<>() {
            @Override
            public void onResponse(List<MultiSortDoc> nextBatch) {
                nextBatchRef.set(nextBatch);
                latch.countDown();
            }
            @Override
            public void onFailure(Exception e) {
                latch.countDown();
                listener.onFailure(e);
            }
        });

        // Coordination: Wait until both tasks complete, then deliver results and continue.
        threadPool.executor(ThreadPool.Names.SEARCH).execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                latch.await();
                List<Tuple> currentResults = fetchResultsRef.get();
                List<MultiSortDoc> nextBatch = nextBatchRef.get();
                listener.onResponse(currentResults);
                processBatchCycle(nextBatch, listener);
            }
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Asynchronously fetches document fields for the given batch of document IDs.
     */
    private void fetchBatch(final List<MultiSortDoc> batch, final ActionListener<List<Tuple>> listener) {
        threadPool.executor(ThreadPool.Names.SEARCH).execute(new AbstractRunnable() {
            List<Tuple> results = new ArrayList<>();
            @Override
            protected void doRun() throws Exception {
                for (MultiSortDoc doc : batch) {
                    // TODO: call fetch service
                    results.add(new Tuple(doc.getDocId(), Collections.emptyMap()));
                }
                listener.onResponse(results);
            }
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TopDocs;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContextBuilder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.query.QuerySearchResult;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * A {@link ArraySearchPhaseResults} implementation that incrementally reduces aggregation results
 * as shard results are consumed.
 * This implementation adds the memory that it used to save and reduce the results of shard aggregations
 * in the {@link CircuitBreaker#REQUEST} circuit breaker. Before any partial or final reduce, the memory
 * needed to reduce the aggregations is estimated and a {@link CircuitBreakingException} is handled if it
 * exceeds the maximum memory allowed in this breaker.
 *
 * @opensearch.internal
 */
public class QueryPhaseResultConsumer extends ArraySearchPhaseResults<SearchPhaseResult> implements Releasable {
    private static final Logger logger = LogManager.getLogger(QueryPhaseResultConsumer.class);

    private final Executor executor;
    private final CircuitBreaker circuitBreaker;
    private final SearchPhaseController controller;
    private final SearchProgressListener progressListener;
    private final ReduceContextBuilder aggReduceContextBuilder;
    private final NamedWriteableRegistry namedWriteableRegistry;

    private final int topNSize;
    private final boolean hasTopDocs;
    private final boolean hasAggs;
    private final boolean performFinalReduce;

    final PendingReduces pendingReduces;
    private final Consumer<Exception> cancelTaskOnFailure;
    private final BooleanSupplier isTaskCancelled;

    public QueryPhaseResultConsumer(
        SearchRequest request,
        Executor executor,
        CircuitBreaker circuitBreaker,
        SearchPhaseController controller,
        SearchProgressListener progressListener,
        NamedWriteableRegistry namedWriteableRegistry,
        int expectedResultSize,
        Consumer<Exception> cancelTaskOnFailure
    ) {
        this(
            request,
            executor,
            circuitBreaker,
            controller,
            progressListener,
            namedWriteableRegistry,
            expectedResultSize,
            cancelTaskOnFailure,
            () -> false
        );
    }

    /**
     * Creates a {@link QueryPhaseResultConsumer} that incrementally reduces aggregation results
     * as shard results are consumed.
     */
    public QueryPhaseResultConsumer(
        SearchRequest request,
        Executor executor,
        CircuitBreaker circuitBreaker,
        SearchPhaseController controller,
        SearchProgressListener progressListener,
        NamedWriteableRegistry namedWriteableRegistry,
        int expectedResultSize,
        Consumer<Exception> cancelTaskOnFailure,
        BooleanSupplier isTaskCancelled
    ) {
        super(expectedResultSize);
        this.executor = executor;
        this.circuitBreaker = circuitBreaker;
        this.controller = controller;
        this.progressListener = progressListener;
        this.aggReduceContextBuilder = controller.getReduceContext(request);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.topNSize = SearchPhaseController.getTopDocsSize(request);
        this.performFinalReduce = request.isFinalReduce();
        this.cancelTaskOnFailure = cancelTaskOnFailure;

        SearchSourceBuilder source = request.source();
        this.hasTopDocs = source == null || source.size() != 0;
        this.hasAggs = source != null && source.aggregations() != null;
        int batchReduceSize = getBatchReduceSize(request.getBatchedReduceSize(), expectedResultSize);
        this.pendingReduces = new PendingReduces(batchReduceSize, request.resolveTrackTotalHitsUpTo());
        this.isTaskCancelled = isTaskCancelled;
    }

    int getBatchReduceSize(int requestBatchedReduceSize, int minBatchReduceSize) {
        return (hasAggs || hasTopDocs) ? Math.min(requestBatchedReduceSize, minBatchReduceSize) : minBatchReduceSize;
    }

    @Override
    public void close() {
        Releasables.close(pendingReduces);
    }

    @Override
    public void consumeResult(SearchPhaseResult result, Runnable next) {
        super.consumeResult(result, () -> {});
        QuerySearchResult querySearchResult = result.queryResult();
        progressListener.notifyQueryResult(querySearchResult.getShardIndex());
        pendingReduces.consume(querySearchResult, next);
    }

    @Override
    public SearchPhaseController.ReducedQueryPhase reduce() throws Exception {
        if (pendingReduces.hasPendingReduceTask()) {
            throw new AssertionError("partial reduce in-flight");
        }
        checkCancellation();
        if (pendingReduces.hasFailure()) {
            throw pendingReduces.failure.get();
        }

        // ensure consistent ordering
        pendingReduces.sortBuffer();
        final SearchPhaseController.TopDocsStats topDocsStats = pendingReduces.consumeTopDocsStats();
        final List<TopDocs> topDocsList = pendingReduces.consumeTopDocs();
        final List<InternalAggregations> aggsList = pendingReduces.consumeAggs();
        long breakerSize = pendingReduces.circuitBreakerBytes;
        if (hasAggs) {
            // Add an estimate of the final reduce size
            breakerSize = pendingReduces.addEstimateAndMaybeBreak(pendingReduces.estimateRamBytesUsedForReduce(breakerSize));
        }
        SearchPhaseController.ReducedQueryPhase reducePhase = controller.reducedQueryPhase(
            results.asList(),
            aggsList,
            topDocsList,
            topDocsStats,
            pendingReduces.numReducePhases,
            false,
            aggReduceContextBuilder,
            performFinalReduce
        );
        if (hasAggs) {
            // Update the circuit breaker to replace the estimation with the serialized size of the newly reduced result
            long finalSize = reducePhase.aggregations.getSerializedSize() - breakerSize;
            pendingReduces.addWithoutBreaking(finalSize);
            logger.trace(
                "aggs final reduction [{}] max [{}]",
                pendingReduces.aggsCurrentBufferSize,
                pendingReduces.maxAggsCurrentBufferSize
            );
        }
        progressListener.notifyFinalReduce(
            SearchProgressListener.buildSearchShards(results.asList()),
            reducePhase.totalHits,
            reducePhase.aggregations,
            reducePhase.numReducePhases
        );
        return reducePhase;
    }

    private ReduceResult partialReduce(
        QuerySearchResult[] toConsume,
        List<SearchShard> emptyResults,
        SearchPhaseController.TopDocsStats topDocsStats,
        ReduceResult lastReduceResult,
        int numReducePhases
    ) {
        checkCancellation();
        if (pendingReduces.hasFailure()) {
            return lastReduceResult;
        }
        // ensure consistent ordering
        Arrays.sort(toConsume, Comparator.comparingInt(QuerySearchResult::getShardIndex));

        for (QuerySearchResult result : toConsume) {
            topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
        }

        final TopDocs newTopDocs;
        if (hasTopDocs) {
            List<TopDocs> topDocsList = new ArrayList<>();
            if (lastReduceResult != null) {
                topDocsList.add(lastReduceResult.reducedTopDocs);
            }
            for (QuerySearchResult result : toConsume) {
                TopDocsAndMaxScore topDocs = result.consumeTopDocs();
                SearchPhaseController.setShardIndex(topDocs.topDocs, result.getShardIndex());
                topDocsList.add(topDocs.topDocs);
            }
            newTopDocs = SearchPhaseController.mergeTopDocs(
                topDocsList,
                // we have to merge here in the same way we collect on a shard
                topNSize,
                0
            );
        } else {
            newTopDocs = null;
        }

        final InternalAggregations newAggs;
        if (hasAggs) {
            List<InternalAggregations> aggsList = new ArrayList<>();
            if (lastReduceResult != null) {
                aggsList.add(lastReduceResult.reducedAggs);
            }
            for (QuerySearchResult result : toConsume) {
                aggsList.add(result.consumeAggs().expand());
            }
            newAggs = InternalAggregations.topLevelReduce(aggsList, aggReduceContextBuilder.forPartialReduction());
        } else {
            newAggs = null;
        }
        List<SearchShard> processedShards = new ArrayList<>(emptyResults);
        if (lastReduceResult != null) {
            processedShards.addAll(lastReduceResult.processedShards);
        }
        for (QuerySearchResult result : toConsume) {
            SearchShardTarget target = result.getSearchShardTarget();
            processedShards.add(new SearchShard(target.getClusterAlias(), target.getShardId()));
        }
        progressListener.notifyPartialReduce(processedShards, topDocsStats.getTotalHits(), newAggs, numReducePhases);
        // we leave the results un-serialized because serializing is slow but we compute the serialized
        // size as an estimate of the memory used by the newly reduced aggregations.
        long serializedSize = hasAggs ? newAggs.getSerializedSize() : 0;
        return new ReduceResult(processedShards, newTopDocs, newAggs, hasAggs ? serializedSize : 0);
    }

    private void checkCancellation() {
        if (isTaskCancelled.getAsBoolean()) {
            pendingReduces.onFailure(new TaskCancelledException("request has been terminated"));
        }
    }

    public int getNumReducePhases() {
        return pendingReduces.numReducePhases;
    }

    /**
     * Manages incremental query result reduction by buffering incoming results and
     * triggering partial reduce operations when the threshold is reached.
     * <ul>
     * <li>Handles circuit breaker memory accounting</li>
     * <li>Coordinates reduce task execution to be one at a time</li>
     * <li>Provides thread-safe failure handling with cleanup</li>
     * </ul>
     *
     * @opensearch.internal
     */
    class PendingReduces implements Releasable {
        private final int batchReduceSize;
        private final List<QuerySearchResult> buffer = new ArrayList<>();
        private final List<SearchShard> emptyResults = new ArrayList<>();
        // the memory that is accounted in the circuit breaker for this consumer
        private volatile long circuitBreakerBytes;
        // the memory that is currently used in the buffer
        private volatile long aggsCurrentBufferSize;
        private volatile long maxAggsCurrentBufferSize = 0;

        private final ArrayDeque<ReduceTask> queue = new ArrayDeque<>();
        private final AtomicReference<ReduceTask> runningTask = new AtomicReference<>(); // ensure only one task is running
        private final AtomicReference<Exception> failure = new AtomicReference<>();

        private final SearchPhaseController.TopDocsStats topDocsStats;
        private volatile ReduceResult reduceResult;
        private volatile boolean hasPartialReduce;
        private volatile int numReducePhases;

        PendingReduces(int batchReduceSize, int trackTotalHitsUpTo) {
            this.batchReduceSize = batchReduceSize;
            this.topDocsStats = new SearchPhaseController.TopDocsStats(trackTotalHitsUpTo);
        }

        @Override
        public synchronized void close() {
            assert hasPendingReduceTask() == false : "cannot close with partial reduce in-flight";
            if (hasFailure()) {
                assert circuitBreakerBytes == 0;
                return;
            }
            assert circuitBreakerBytes >= 0;
            circuitBreaker.addWithoutBreaking(-circuitBreakerBytes);
            circuitBreakerBytes = 0;
        }

        private boolean hasFailure() {
            return failure.get() != null;
        }

        private boolean hasPendingReduceTask() {
            return queue.isEmpty() == false || runningTask.get() != null;
        }

        private void sortBuffer() {
            if (buffer.size() > 0) {
                Collections.sort(buffer, Comparator.comparingInt(QuerySearchResult::getShardIndex));
            }
        }

        private synchronized long addWithoutBreaking(long size) {
            if (hasFailure()) {
                return circuitBreakerBytes;
            }
            circuitBreaker.addWithoutBreaking(size);
            circuitBreakerBytes += size;
            maxAggsCurrentBufferSize = Math.max(maxAggsCurrentBufferSize, circuitBreakerBytes);
            return circuitBreakerBytes;
        }

        private synchronized long addEstimateAndMaybeBreak(long estimatedSize) {
            if (hasFailure()) {
                return circuitBreakerBytes;
            }
            circuitBreaker.addEstimateBytesAndMaybeBreak(estimatedSize, "<reduce_aggs>");
            circuitBreakerBytes += estimatedSize;
            maxAggsCurrentBufferSize = Math.max(maxAggsCurrentBufferSize, circuitBreakerBytes);
            return circuitBreakerBytes;
        }

        private synchronized void resetCircuitBreaker() {
            if (circuitBreakerBytes > 0) {
                circuitBreaker.addWithoutBreaking(-circuitBreakerBytes);
                circuitBreakerBytes = 0;
            }
        }

        /**
         * Returns the size of the serialized aggregation that is contained in the
         * provided {@link QuerySearchResult}.
         */
        private long ramBytesUsedQueryResult(QuerySearchResult result) {
            if (hasAggs == false) {
                return 0;
            }
            return result.aggregations().asSerialized(InternalAggregations::readFrom, namedWriteableRegistry).ramBytesUsed();
        }

        /**
         * Returns an estimation of the size that a reduce of the provided size
         * would take on memory.
         * This size is estimated as roughly 1.5 times the size of the serialized
         * aggregations that need to be reduced. This estimation can be completely
         * off for some aggregations but it is corrected with the real size after
         * the reduce completes.
         */
        private long estimateRamBytesUsedForReduce(long size) {
            return Math.round(0.5d * size);
        }

        void consume(QuerySearchResult result, Runnable callback) {
            checkCancellation();

            if (consumeResult(result, callback)) {
                callback.run();
            }
        }

        private synchronized boolean consumeResult(QuerySearchResult result, Runnable callback) {
            if (hasFailure()) {
                result.consumeAll(); // release memory
                return true;
            }
            if (result.isNull()) {
                SearchShardTarget target = result.getSearchShardTarget();
                emptyResults.add(new SearchShard(target.getClusterAlias(), target.getShardId()));
                return true;
            }
            // Check circuit breaker before consuming
            if (hasAggs) {
                long aggsSize = ramBytesUsedQueryResult(result);
                try {
                    addEstimateAndMaybeBreak(aggsSize);
                    aggsCurrentBufferSize += aggsSize;
                } catch (CircuitBreakingException e) {
                    onFailure(e);
                    return true;
                }
            }
            // Process non-empty results
            int size = buffer.size() + (hasPartialReduce ? 1 : 0);
            if (size >= batchReduceSize) {
                hasPartialReduce = true;
                // the callback must wait for the new reduce task to complete to maintain proper result processing order
                QuerySearchResult[] clone = buffer.toArray(QuerySearchResult[]::new);
                ReduceTask task = new ReduceTask(clone, aggsCurrentBufferSize, new ArrayList<>(emptyResults), callback);
                aggsCurrentBufferSize = 0;
                buffer.clear();
                emptyResults.clear();
                queue.add(task);
                tryExecuteNext();
                buffer.add(result);
                return false; // callback will be run by reduce task
            }
            buffer.add(result);
            return true;
        }

        private void tryExecuteNext() {
            final ReduceTask task;
            synchronized (this) {
                if (hasFailure()) {
                    return;
                }
                if (queue.isEmpty() || runningTask.get() != null) {
                    return;
                }
                task = queue.poll();
                runningTask.compareAndSet(null, task);
            }

            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    final ReduceResult thisReduceResult = reduceResult;
                    long estimatedTotalSize = (thisReduceResult != null ? thisReduceResult.estimatedSize : 0) + task.aggsBufferSize;
                    final ReduceResult newReduceResult;
                    try {
                        final QuerySearchResult[] toConsume = task.consumeBuffer();
                        if (toConsume == null) {
                            onAfterReduce(task, null, 0);
                            return;
                        }
                        long estimateRamBytesUsedForReduce = estimateRamBytesUsedForReduce(estimatedTotalSize);
                        addEstimateAndMaybeBreak(estimateRamBytesUsedForReduce);
                        estimatedTotalSize += estimateRamBytesUsedForReduce;
                        ++numReducePhases;
                        newReduceResult = partialReduce(toConsume, task.emptyResults, topDocsStats, thisReduceResult, numReducePhases);
                    } catch (Exception t) {
                        PendingReduces.this.onFailure(t);
                        return;
                    }
                    onAfterReduce(task, newReduceResult, estimatedTotalSize);
                }

                @Override
                public void onFailure(Exception exc) {
                    PendingReduces.this.onFailure(exc);
                }
            });
        }

        private void onAfterReduce(ReduceTask task, ReduceResult newResult, long estimatedSize) {
            if (newResult != null) {
                synchronized (this) {
                    if (hasFailure()) {
                        return;
                    }
                    runningTask.compareAndSet(task, null);
                    reduceResult = newResult;
                    if (hasAggs) {
                        // Update the circuit breaker to remove the size of the source aggregations
                        // and replace the estimation with the serialized size of the newly reduced result.
                        long newSize = reduceResult.estimatedSize - estimatedSize;
                        addWithoutBreaking(newSize);
                        logger.trace(
                            "aggs partial reduction [{}->{}] max [{}]",
                            estimatedSize,
                            reduceResult.estimatedSize,
                            maxAggsCurrentBufferSize
                        );
                    }
                }
            }
            task.consumeListener();
            executor.execute(this::tryExecuteNext);
        }

        // Idempotent and thread-safe failure handling
        private synchronized void onFailure(Exception exc) {
            if (hasFailure()) {
                assert circuitBreakerBytes == 0;
                return;
            }
            assert circuitBreakerBytes >= 0;
            resetCircuitBreaker();
            failure.compareAndSet(null, exc);
            clearReduceTaskQueue();
            cancelTaskOnFailure.accept(exc);
        }

        private synchronized void clearReduceTaskQueue() {
            ReduceTask task = runningTask.get();
            runningTask.compareAndSet(task, null);
            List<ReduceTask> toCancels = new ArrayList<>();
            if (task != null) {
                toCancels.add(task);
            }
            toCancels.addAll(queue);
            queue.clear();
            reduceResult = null;
            for (ReduceTask toCancel : toCancels) {
                toCancel.cancel();
            }
        }

        private synchronized SearchPhaseController.TopDocsStats consumeTopDocsStats() {
            for (QuerySearchResult result : buffer) {
                topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
            }
            return topDocsStats;
        }

        private synchronized List<TopDocs> consumeTopDocs() {
            if (hasTopDocs == false) {
                return Collections.emptyList();
            }
            List<TopDocs> topDocsList = new ArrayList<>();
            if (reduceResult != null) {
                topDocsList.add(reduceResult.reducedTopDocs);
            }
            for (QuerySearchResult result : buffer) {
                TopDocsAndMaxScore topDocs = result.consumeTopDocs();
                SearchPhaseController.setShardIndex(topDocs.topDocs, result.getShardIndex());
                topDocsList.add(topDocs.topDocs);
            }
            return topDocsList;
        }

        private synchronized List<InternalAggregations> consumeAggs() {
            if (hasAggs == false) {
                return Collections.emptyList();
            }
            List<InternalAggregations> aggsList = new ArrayList<>();
            if (reduceResult != null) {
                aggsList.add(reduceResult.reducedAggs);
            }
            for (QuerySearchResult result : buffer) {
                aggsList.add(result.consumeAggs().expand());
            }
            return aggsList;
        }
    }

    /**
     * Immutable container holding the outcome of a partial reduce operation
     *
     * @opensearch.internal
     */
    private record ReduceResult(List<SearchShard> processedShards, TopDocs reducedTopDocs, InternalAggregations reducedAggs,
        long estimatedSize) {
    }

    /**
     * ReduceTask is created to reduce buffered query results when buffer size hits threshold
     *
     * @opensearch.internal
     */
    private static class ReduceTask {
        private final List<SearchShard> emptyResults;
        private QuerySearchResult[] buffer;
        private final long aggsBufferSize;
        private Runnable next;

        private ReduceTask(QuerySearchResult[] buffer, long aggsBufferSize, List<SearchShard> emptyResults, Runnable next) {
            this.buffer = buffer;
            this.aggsBufferSize = aggsBufferSize;
            this.emptyResults = emptyResults;
            this.next = next;
        }

        public synchronized QuerySearchResult[] consumeBuffer() {
            QuerySearchResult[] toRet = buffer;
            buffer = null;
            return toRet;
        }

        public void consumeListener() {
            if (next != null) {
                next.run();
                next = null;
            }
        }

        public synchronized void cancel() {
            consumeBuffer();
            consumeListener();
        }
    }
}

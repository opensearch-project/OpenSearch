/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TopDocs;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class for streaming TopDocs from shards.
 *
 * @opensearch.internal
 */
public abstract class AbstractShardTopDocsStreamer implements ShardTopDocsStreamer {
    protected static final Logger logger = LogManager.getLogger(AbstractShardTopDocsStreamer.class);

    // Fallback scheduler for time-based emissions
    private static volatile ScheduledExecutorService fallbackScheduler;
    private static final Object schedulerLock = new Object();

    protected SearchContext searchContext;
    protected CircuitBreaker circuitBreaker;
    protected StreamingMetrics metrics;

    protected final int batchDocThreshold;
    protected final TimeValue timeInterval;
    protected final boolean firstHitImmediate;
    protected final boolean enableCoalescing;

    protected final AtomicInteger collectedCount = new AtomicInteger(0);
    protected final AtomicInteger emissionCount = new AtomicInteger(0);
    protected final AtomicLong sequenceId = new AtomicLong(0);
    protected final AtomicBoolean cancelled = new AtomicBoolean(false);
    protected final AtomicBoolean firstDocSeen = new AtomicBoolean(false);

    protected long startTimeMillis;
    protected volatile long lastEmitMillis;
    protected ScheduledFuture<?> timeGuardFuture;

    protected TopDocs lastEmittedTopDocs;
    protected final Object emissionLock = new Object();

    protected volatile boolean started = false;

    protected AbstractShardTopDocsStreamer(
        int batchDocThreshold,
        TimeValue timeInterval,
        boolean firstHitImmediate,
        boolean enableCoalescing
    ) {
        this.batchDocThreshold = batchDocThreshold;
        this.timeInterval = timeInterval;
        this.firstHitImmediate = firstHitImmediate;
        this.enableCoalescing = enableCoalescing;
        this.metrics = StreamingMetrics.NOOP; // Default to no-op, can be overridden
    }

    @Override
    public void onStart(SearchContext context) {
        this.searchContext = Objects.requireNonNull(context, "SearchContext cannot be null");
        try {
            if (context.bigArrays() != null && context.bigArrays().breakerService() != null) {
                this.circuitBreaker = context.bigArrays()
                    .breakerService()
                    .getBreaker(org.opensearch.core.common.breaker.CircuitBreaker.REQUEST);
            } else {
                this.circuitBreaker = null;
            }
        } catch (Exception e) {
            // Circuit breaker unavailable
            this.circuitBreaker = null;
        }
        this.startTimeMillis = System.currentTimeMillis();
        this.lastEmitMillis = this.startTimeMillis;
        this.started = true;

        String shardId = getShardId();
        String mode = getClass().getSimpleName().replace("ShardTopDocsStreamer", "").toLowerCase();
        metrics.recordAggregationStart(shardId, mode);

        // Schedule time guard
        if (timeInterval != null && timeInterval.millis() > 0) {
            scheduleTimeGuard();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Started streaming aggregator for shard [{}]", shardId);
        }
    }

    @Override
    public void onDoc(int globalDocId, float score, Object[] sortValues) throws IOException {
        if (cancelled.get()) {
            throw new IOException("aggregator cancelled");
        }

        // Circuit breaker check
        int count = collectedCount.incrementAndGet();
        if (count % 1000 == 0) {
            checkCircuitBreaker();
            if (cancelled.get()) {
                throw new IOException("aggregator cancelled");
            }
        }

        // Cancellation check
        if (count % 512 == 0) {
            try {
                if (searchContext != null && searchContext.isCancelled()) {
                    cancel("search_cancelled");
                    throw new IOException("search cancelled");
                }
            } catch (Exception e) {
                // isCancelled() not available
            }
        }

        // First doc
        boolean isFirstDoc = firstDocSeen.compareAndSet(false, true);

        // Delegate to implementation first
        processDocument(globalDocId, score, sortValues);

        // Check batch and first-hit emission triggers only
        // Time-based emissions are handled by the scheduler
        if (shouldEmit(isFirstDoc, count)) {
            maybeEmit(false);
        }
    }

    /**
     * Process a document in the implementation-specific way.
     */
    protected abstract void processDocument(int globalDocId, float score, Object[] sortValues) throws IOException;

    @Override
    public void maybeEmit(boolean force) throws IOException {
        if (!started || cancelled.get()) {
            return;
        }

        synchronized (emissionLock) {
            TopDocs currentTopDocs = buildCurrentTopDocs();
            if (currentTopDocs == null || currentTopDocs.totalHits.value() == 0) {
                // Nothing to emit yet
                return;
            }

            // Check if we should coalesce (skip emission if unchanged)
            if (!force && enableCoalescing && isTopDocsEqual(lastEmittedTopDocs, currentTopDocs)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("coalescing");
                }
                metrics.recordCoalescedEmission(getShardId(), sequenceId.get() + 1);
                // Reset time guard
                lastEmitMillis = System.currentTimeMillis();
                return;
            }

            // Emit the partial result
            emitPartialResult(currentTopDocs);
            lastEmittedTopDocs = currentTopDocs;
            int emissionNum = emissionCount.incrementAndGet();

            // Update last emission time for periodic time-guard
            lastEmitMillis = System.currentTimeMillis();

            // Record metrics
            long currentSeqId = sequenceId.get();
            metrics.recordPartialEmission(getShardId(), currentSeqId, currentTopDocs.scoreDocs.length, collectedCount.get());

            // Record TTFB for first emission
            if (emissionNum == 1) {
                long ttfb = lastEmitMillis - startTimeMillis;
                metrics.recordTimeToFirstEmission(getShardId(), ttfb);
            }
        }
    }

    /**
     * Build the current TopDocs snapshot for emission.
     */
    protected abstract TopDocs buildCurrentTopDocs() throws IOException;

    /**
     * Emit a partial result through the streaming channel.
     */
    protected void emitPartialResult(TopDocs topDocs) {
        if (searchContext.getStreamChannelListener() == null) {
            return;
        }

        long seqId = sequenceId.incrementAndGet();

        try {
            // Create a QuerySearchResult for the partial result
            QuerySearchResult partialResult = new QuerySearchResult();
            partialResult.topDocs(new org.opensearch.common.lucene.search.TopDocsAndMaxScore(topDocs, Float.NaN), null);
            partialResult.setPartial(true);

            // Emit through the streaming channel
            searchContext.getStreamChannelListener().onStreamResponse(partialResult, false);

            if (logger.isDebugEnabled()) {
                logger.debug("Emitted partial result #{} with {} hits", seqId, topDocs.totalHits.value());
            }
        } catch (Exception e) {
            logger.warn("emission failed", e);
        }
    }

    @Override
    public void onFinish() {
        // Cancel time guard if scheduled
        if (timeGuardFuture != null) {
            timeGuardFuture.cancel(false);
        }

        // Emit final result if not cancelled
        if (!cancelled.get()) {
            try {
                maybeEmit(true);  // Force final emission
            } catch (IOException e) {
                logger.warn("final emission failed", e);
            }
        }

        // Record completion metrics
        long duration = System.currentTimeMillis() - startTimeMillis;
        metrics.recordAggregationComplete(getShardId(), collectedCount.get(), emissionCount.get(), duration);

        if (logger.isDebugEnabled()) {
            logger.debug(
                "Finished streaming aggregator: collected={}, emissions={}, duration={}ms",
                collectedCount.get(),
                emissionCount.get(),
                duration
            );
        }
    }

    @Override
    public int getCollectedCount() {
        return collectedCount.get();
    }

    @Override
    public int getEmissionCount() {
        return emissionCount.get();
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Cancel the aggregator.
     */
    public void cancel() {
        cancel("user_request");
    }

    /**
     * Cancel the aggregator with a specific reason.
     */
    public void cancel(String reason) {
        cancelled.set(true);
        if (timeGuardFuture != null) {
            timeGuardFuture.cancel(false);
        }
        metrics.recordCancellation(getShardId(), reason);

        if (logger.isDebugEnabled()) {
            logger.debug("Cancelled streaming aggregator for shard [{}]: {}", getShardId(), reason);
        }
    }

    /**
     * Determine if emission should occur based on triggers.
     */
    protected boolean shouldEmit(boolean isFirstDoc, int docCount) {
        // First-hit immediate emission
        if (isFirstDoc && firstHitImmediate) {
            return true;
        }

        // Batch threshold emission
        if (batchDocThreshold > 0 && docCount % batchDocThreshold == 0) {
            return true;
        }

        return false;
    }

    /**
     * Schedule a time-based emission guard.
     * Uses a fixed-rate scheduler to ensure reliable TTFB even when no documents arrive.
     */
    protected void scheduleTimeGuard() {
        if (timeInterval == null || timeInterval.millis() <= 0) {
            return;
        }

        try {
            ScheduledExecutorService scheduler = getScheduler();
            if (scheduler != null) {
                timeGuardFuture = scheduler.scheduleAtFixedRate(
                    this::timeBasedEmission,
                    timeInterval.millis(),
                    timeInterval.millis(),
                    TimeUnit.MILLISECONDS
                );

                if (logger.isDebugEnabled()) {
                    logger.debug("Scheduled time guard with interval [{}]", timeInterval);
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("No scheduler available, time guard disabled");
                }
            }
        } catch (Exception e) {
            logger.warn("scheduler failed", e);
        }
    }

    /**
     * Time-based emission callback for scheduler.
     * Implements the logic for scheduled emissions:
     * - Skip if cancelled or not started
     * - Never emit empty snapshots on first emission
     * - Update lastEmitMillis on coalesced skips to prevent repeated triggers
     */
    private void timeBasedEmission() {
        try {
            if (cancelled.get() || !started) {
                return;
            }

            synchronized (emissionLock) {
                TopDocs currentTopDocs = buildCurrentTopDocs();

                // Never emit empty snapshots for time-based emissions
                if (currentTopDocs == null || currentTopDocs.totalHits.value() == 0) {
                    return;
                }

                // If coalescing is enabled and snapshot unchanged, update time and skip
                if (enableCoalescing && isTopDocsEqual(lastEmittedTopDocs, currentTopDocs)) {
                    lastEmitMillis = System.currentTimeMillis();
                    return;
                }

                // Emit the partial result
                emitPartialResult(currentTopDocs);
                lastEmittedTopDocs = currentTopDocs;
                emissionCount.incrementAndGet();
                lastEmitMillis = System.currentTimeMillis();

                // Record metrics
                long currentSeqId = sequenceId.get();
                metrics.recordPartialEmission(getShardId(), currentSeqId, currentTopDocs.scoreDocs.length, collectedCount.get());

                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "Time-based emission: {} docs, total hits: {}",
                        currentTopDocs.scoreDocs.length,
                        currentTopDocs.totalHits.value()
                    );
                }
            }
        } catch (Exception e) {
            logger.warn("time emission failed", e);
        }
    }

    /**
     * Get scheduler from search context or fallback scheduler.
     */
    private ScheduledExecutorService getScheduler() {
        try {
            // Try to get from search context threadpool first
            if (searchContext != null) {
                // TODO: Add proper threadpool access when available
                // return searchContext.getThreadPool().scheduler(ThreadPool.Names.STREAM_SEARCH);
            }
        } catch (Exception e) {
            // ThreadPool not available from context
        }

        // Fall back to shared scheduler
        return getFallbackScheduler();
    }

    /**
     * Get or create a fallback scheduler for time-based emissions.
     */
    private static ScheduledExecutorService getFallbackScheduler() {
        if (fallbackScheduler == null) {
            synchronized (schedulerLock) {
                if (fallbackScheduler == null) {
                    ThreadFactory factory = r -> {
                        Thread t = new Thread(r, "streaming-time-guard");
                        t.setDaemon(true);
                        return t;
                    };
                    fallbackScheduler = Executors.newSingleThreadScheduledExecutor(factory);

                    // Add shutdown hook to clean up
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        if (fallbackScheduler != null && !fallbackScheduler.isShutdown()) {
                            fallbackScheduler.shutdown();
                        }
                    }));
                }
            }
        }
        return fallbackScheduler;
    }

    /**
     * Shutdown the fallback scheduler - for testing only.
     */
    public static void shutdownFallbackScheduler() {
        synchronized (schedulerLock) {
            if (fallbackScheduler != null && !fallbackScheduler.isShutdown()) {
                fallbackScheduler.shutdown();
                fallbackScheduler = null;
            }
        }
    }

    /**
     * Check circuit breaker to prevent memory issues.
     */
    protected void checkCircuitBreaker() {
        if (circuitBreaker != null) {
            try {
                // Estimate memory usage (implementation-specific)
                long estimatedBytes = estimateMemoryUsage();
                circuitBreaker.addEstimateBytesAndMaybeBreak(estimatedBytes, "streaming aggregator");
                metrics.recordCircuitBreakerCheck(getShardId(), estimatedBytes, false);
            } catch (CircuitBreakingException e) {
                metrics.recordCircuitBreakerCheck(getShardId(), estimateMemoryUsage(), true);
                cancel("circuit_breaker");
                throw e;
            }
        }

        // Check search context cancellation if available
        try {
            if (searchContext != null && searchContext.isCancelled()) {
                cancel("search_context_cancelled");
                throw new IOException("Search context has been cancelled");
            }
        } catch (Exception e) {
            // SearchContext.isCancelled() not available in this implementation
        }
    }

    /**
     * Estimate memory usage for circuit breaker.
     * Implementations should override for accurate estimates.
     */
    protected long estimateMemoryUsage() {
        // Basic estimate: collected docs * bytes per doc
        return collectedCount.get() * 100L;  // 100 bytes per doc estimate
    }

    /**
     * Check if two TopDocs are effectively equal for coalescing.
     */
    protected boolean isTopDocsEqual(TopDocs td1, TopDocs td2) {
        if (td1 == null || td2 == null) {
            return false;
        }

        if (td1.totalHits.value() != td2.totalHits.value()) {
            return false;
        }

        if (td1.scoreDocs.length != td2.scoreDocs.length) {
            return false;
        }

        // Check if the actual docs have changed
        for (int i = 0; i < td1.scoreDocs.length; i++) {
            if (td1.scoreDocs[i].doc != td2.scoreDocs[i].doc) {
                return false;
            }
            if (Float.compare(td1.scoreDocs[i].score, td2.scoreDocs[i].score) != 0) {
                return false;
            }
        }

        return true;
    }

    /**
     * Get the shard identifier for metrics and logging.
     */
    protected String getShardId() {
        // TODO: Extract shard ID from search context when API is available
        return "unknown";
    }

    /**
     * Set custom metrics implementation for testing or monitoring.
     */
    public void setMetrics(StreamingMetrics metrics) {
        this.metrics = metrics != null ? metrics : StreamingMetrics.NOOP;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.merge.MergeStatsTracker;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Schedules and coordinates segment merge operations for a shard.
 * <p>
 * This scheduler delegates merge selection to a {@link MergeHandler} and controls
 * concurrency via configurable merge count limits sourced from
 * {@link MergeSchedulerConfig}. Merge tasks are submitted to the OpenSearch
 * {@link ThreadPool} using the {@link ThreadPool.Names#FORCE_MERGE} executor.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergeScheduler {

    private final Logger logger;
    private final MergeHandler mergeHandler;
    private final BiConsumer<MergeResult, OneMerge> applyMergeChanges;
    private final ThreadPool threadPool;
    private final AtomicInteger activeMerges = new AtomicInteger(0);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private volatile int maxConcurrentMerges;
    private volatile int maxMergeCount;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final MergeStatsTracker mergeStatsTracker = new MergeStatsTracker();

    /** true if we should rate-limit writes for each merge */
    private boolean doAutoIOThrottle = false;

    /** Initial value for IO write rate limit when doAutoIOThrottle is true */
    private static final double START_MB_PER_SEC = 20.0;

    /** Current IO writes throttle rate */
    protected double targetMBPerSec = START_MB_PER_SEC;

    /**
     * Creates a new merge scheduler.
     *
     * @param mergeHandler      the handler that selects and executes merges
     * @param applyMergeChanges callback to apply merge results (e.g., update the catalog)
     * @param shardId           the shard this scheduler is associated with
     * @param indexSettings     the index settings providing merge scheduler configuration
     * @param threadPool        the OpenSearch thread pool for executing merge tasks
     */
    public MergeScheduler(
        MergeHandler mergeHandler,
        BiConsumer<MergeResult, OneMerge> applyMergeChanges,
        ShardId shardId,
        IndexSettings indexSettings,
        ThreadPool threadPool
    ) {
        this.mergeHandler = mergeHandler;
        this.applyMergeChanges = applyMergeChanges;
        this.threadPool = threadPool;
        logger = Loggers.getLogger(getClass(), shardId);
        this.mergeSchedulerConfig = indexSettings.getMergeSchedulerConfig();
        refreshConfig();
    }

    /**
     * Refreshes the max concurrent merge thread count and max merge count from
     * the current {@link MergeSchedulerConfig}. No-op if the values have not changed.
     */
    public synchronized void refreshConfig() {
        int newMaxThreadCount = mergeSchedulerConfig.getMaxThreadCount();
        int newMaxMergeCount = mergeSchedulerConfig.getMaxMergeCount();

        if (newMaxThreadCount == this.maxConcurrentMerges && newMaxMergeCount == this.maxMergeCount) {
            return;
        }

        logger.info(
            () -> new ParameterizedMessage(
                "Updating from merge scheduler config: maxThreadCount {} -> {}, " + "maxMergeCount {} -> {}",
                this.maxConcurrentMerges,
                newMaxThreadCount,
                this.maxMergeCount,
                newMaxMergeCount
            )
        );

        this.maxConcurrentMerges = newMaxThreadCount;
        this.maxMergeCount = newMaxMergeCount;
    }

    /**
     * Triggers pending merge operations. Merges are selected by the
     * underlying {@link MergeHandler} and executed up to the configured
     * concurrency limits.
     */
    public void triggerMerges() {
        if (isShutdown.get()) {
            logger.warn("MergeScheduler is shutdown, ignoring merge trigger");
            return;
        }

        mergeHandler.findAndRegisterMerges();

        executeMerge();
    }

    /**
     * Forces a merge down to at most {@code maxNumSegment} segments.
     * Runs synchronously on the calling thread.
     *
     * @param maxNumSegment the maximum number of segments after the force merge
     */
    public void forceMerge(int maxNumSegment) throws IOException {
        if (activeMerges.get() > 0) {
            logger.warn("Cannot force merge while background merges are active");
            throw new IllegalStateException("Cannot force merge while background merges are active");
        }
        Collection<OneMerge> oneMerges = mergeHandler.findForceMerges(maxNumSegment);

        for (OneMerge oneMerge : oneMerges) {
            threadPool.executor(ThreadPool.Names.FORCE_MERGE).execute(() -> {
                try {
                    MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
                    applyMergeChanges.accept(mergeResult, oneMerge);
                    mergeHandler.onMergeFinished(oneMerge);
                } catch (Exception e) {
                    logger.error(new ParameterizedMessage("Force merge failed for: {}", oneMerge), e);
                    mergeHandler.onMergeFailure(oneMerge);
                }
            });
        }
    }

    /**
     * Turn on dynamic IO throttling, to adaptively rate limit writes bytes/sec to the minimal rate
     * necessary so merges do not fall behind. By default, this is disabled and writes are not
     * rate-limited.
     */
    public synchronized void enableAutoIOThrottle() {
        doAutoIOThrottle = true;
        targetMBPerSec = START_MB_PER_SEC;
    }

    /**
     * Returns the currently set per-merge IO writes rate limit, if {@link #enableAutoIOThrottle} was
     * called, else {@code Double.POSITIVE_INFINITY}.
     */
    public synchronized double getIORateLimitMBPerSec() {
        if (doAutoIOThrottle) {
            return targetMBPerSec;
        }

        return Double.POSITIVE_INFINITY;
    }

    /**
     * Shuts down this merge scheduler, preventing new merges from being submitted.
     */
    public void shutdown() {
        isShutdown.set(true);
    }

    /**
     * Returns the current merge statistics for this scheduler.
     *
     * @return the merge stats
     */
    public MergeStats stats() {
        return mergeStatsTracker.toMergeStats(mergeSchedulerConfig.isAutoThrottle() ? getIORateLimitMBPerSec() : Double.POSITIVE_INFINITY);
    }

    /**
     * Drains the pending-merge queue up to {@link #maxConcurrentMerges},
     * submitting each merge as a task to the thread pool.
     */
    private void executeMerge() {
        while (activeMerges.get() < maxConcurrentMerges && mergeHandler.hasPendingMerges()) {
            OneMerge oneMerge = mergeHandler.getNextMerge();
            if (oneMerge == null) {
                return;
            }
            try {
                submitMergeTask(oneMerge);
            } catch (Exception e) {
                mergeHandler.onMergeFailure(oneMerge);
            }
        }
    }

    /**
     * Submits a merge task to the thread pool's force merge executor.
     *
     * @param oneMerge the merge to execute
     */
    private void submitMergeTask(OneMerge oneMerge) {
        activeMerges.incrementAndGet();
        threadPool.executor(ThreadPool.Names.MERGE).execute(() -> {
            long totalSizeInBytes = oneMerge.getTotalSizeInBytes();
            long totalNumDocs = oneMerge.getTotalNumDocs();
            long timeNS = System.nanoTime();
            long tookMS = 0;
            try {
                if (isShutdown.get()) {
                    logger.debug("MergeScheduler is shutdown, skipping merge");
                    return;
                }

                mergeStatsTracker.beforeMerge(totalNumDocs, totalSizeInBytes);

                MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
                applyMergeChanges.accept(mergeResult, oneMerge);
                mergeHandler.onMergeFinished(oneMerge);

                tookMS = TimeValue.nsecToMSec((System.nanoTime() - timeNS));
                logger.info("Merge {} completed in {}ms, result: {}", oneMerge, tookMS, mergeResult.getMergedWriterFileSet());

            } catch (Exception e) {
                logger.error(new ParameterizedMessage("Unexpected error during merge for: {}", oneMerge), e);
                mergeHandler.onMergeFailure(oneMerge);
            } finally {
                mergeStatsTracker.afterMerge(tookMS, totalNumDocs, totalSizeInBytes);

                activeMerges.decrementAndGet();
                // A completed merge may free up capacity for new merges, so check again.
                executeMerge();
            }
        });
    }
}

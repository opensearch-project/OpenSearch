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
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.merge.MergeStats;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Schedules and coordinates segment merge operations for a shard.
 * <p>
 * This scheduler delegates merge selection to a {@link MergeHandler} and controls
 * concurrency via configurable thread and merge count limits sourced from
 * {@link MergeSchedulerConfig}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergeScheduler {

    private final Logger logger;
    private final MergeHandler mergeHandler;
    /** Counter used to generate unique merge thread names. */
    protected int mergeThreadCounter = 0;
    private final BiConsumer<MergeResult, OneMerge> applyMergeChanges;
    private final List<MergeThread> mergeThreads = new CopyOnWriteArrayList<>();
    private final AtomicInteger activeMerges = new AtomicInteger(0);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private volatile int maxConcurrentMerges;
    private volatile int maxMergeCount;
    private final ShardId shardId;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final Settings indexSettings;

    private final MeanMetric totalMerges = new MeanMetric();
    private final CounterMetric totalMergesNumDocs = new CounterMetric();
    private final CounterMetric totalMergesSizeInBytes = new CounterMetric();
    private final CounterMetric currentMerges = new CounterMetric();
    private final CounterMetric currentMergesNumDocs = new CounterMetric();
    private final CounterMetric currentMergesSizeInBytes = new CounterMetric();
    private final CounterMetric totalMergeStoppedTime = new CounterMetric();
    private final CounterMetric totalMergeThrottledTime = new CounterMetric();

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
     */
    public MergeScheduler(
        MergeHandler mergeHandler,
        BiConsumer<MergeResult, OneMerge> applyMergeChanges,
        ShardId shardId,
        IndexSettings indexSettings
    ) {
        this.mergeHandler = mergeHandler;
        this.applyMergeChanges = applyMergeChanges;
        logger = Loggers.getLogger(getClass(), shardId);
        this.shardId = shardId;
        this.indexSettings = indexSettings.getSettings();
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

        mergeHandler.updatePendingMerges();

        executeMerge();
    }

    /**
     * Forces a merge down to at most {@code maxNumSegment} segments.
     *
     * @param maxNumSegment the maximum number of segments after the force merge
     */
    public void forceMerge(int maxNumSegment) {
        if (!mergeThreads.isEmpty()) {
            logger.warn("Cannot force merge while background merges are active");
            throw new IllegalStateException("Cannot force merge while background merges are active");
        }
        Collection<OneMerge> oneMerges = mergeHandler.findForceMerges(maxNumSegment);

        for (OneMerge oneMerge : oneMerges) {
            MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
            this.applyMergeChanges.accept(mergeResult, oneMerge);
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
     * Returns the current merge statistics for this scheduler.
     *
     * @return the merge stats
     */
    public MergeStats stats() {
        final MergeStats mergeStats = new MergeStats();
        mergeStats.add(
            totalMerges.count(),
            totalMerges.sum(),
            totalMergesNumDocs.count(),
            totalMergesSizeInBytes.count(),
            currentMerges.count(),
            currentMergesNumDocs.count(),
            currentMergesSizeInBytes.count(),
            // TODO: update the below values from the Rust
            totalMergeStoppedTime.count(),
            totalMergeThrottledTime.count(),
            mergeSchedulerConfig.isAutoThrottle() ? getIORateLimitMBPerSec() : Double.POSITIVE_INFINITY
        );
        return mergeStats;
    }

    /**
     * Daemon thread that executes a single {@link OneMerge} operation.
     * On completion (success or failure) the thread decrements active-merge
     * counters and re-triggers {@link #executeMerge()} to drain any remaining
     * pending merges.
     */
    private class MergeThread extends Thread {
        private final OneMerge oneMerge;

        MergeThread(OneMerge oneMerge) {
            super();
            this.oneMerge = oneMerge;
            setDaemon(true);
        }

        @Override
        public void run() {
            long totalSizeInBytes = oneMerge.getTotalSizeInBytes();
            long totalNumDocs = oneMerge.getTotalNumDocs();
            long timeNS = System.nanoTime();
            long tookMS = 0;
            try {
                if (isShutdown.get()) {
                    logger.debug("[{}] MergeScheduler is shutdown, skipping merge", getName());
                    return;
                }

                currentMerges.inc();
                currentMergesNumDocs.inc(totalNumDocs);
                currentMergesSizeInBytes.inc(totalSizeInBytes);

                logger.debug("[{}] Starting merge for: {}", getName(), oneMerge);

                MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
                applyMergeChanges.accept(mergeResult, oneMerge);
                mergeHandler.onMergeFinished(oneMerge);

                tookMS = TimeValue.nsecToMSec((System.nanoTime() - timeNS));
                logger.info("[{}] Merge completed in {}ms for: {} and output is stored in: {}", getName(), tookMS, oneMerge, mergeResult);

            } catch (Exception e) {
                logger.error(new ParameterizedMessage("[{}] Unexpected error during merge for: {}", getName(), oneMerge), e);
                mergeHandler.onMergeFailure(oneMerge);
            } finally {

                currentMerges.dec();
                currentMergesNumDocs.dec(totalNumDocs);
                currentMergesSizeInBytes.dec(totalSizeInBytes);

                totalMergesNumDocs.inc(totalNumDocs);
                totalMergesSizeInBytes.inc(totalSizeInBytes);
                totalMerges.inc(tookMS);

                activeMerges.decrementAndGet();
                mergeThreads.remove(this);
                // triggering merge at the end
                executeMerge();
            }
        }
    }

    /**
     * Drains the pending-merge queue up to {@link #maxConcurrentMerges},
     * submitting each merge as a new {@link MergeThread}.
     */
    private void executeMerge() {
        // Submit merges up to available capacity
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
     * Creates and starts a daemon {@link MergeThread} for the given merge.
     *
     * @param oneMerge the merge to execute in a new thread
     */
    private void submitMergeTask(OneMerge oneMerge) {
        activeMerges.incrementAndGet();
        MergeThread thread = new MergeThread(oneMerge);
        thread.setName(
            OpenSearchExecutors.threadName(
                indexSettings,
                "[" + shardId.getIndexName() + "][" + shardId.id() + "]: Merge thread #" + mergeThreadCounter++
            )
        );
        mergeThreads.add(thread);
        thread.start();
    }
}

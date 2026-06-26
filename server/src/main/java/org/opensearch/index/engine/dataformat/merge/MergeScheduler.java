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
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.merge.MergeStatsTracker;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
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
    private final Runnable onMergeFailureCleanup;
    private final Runnable activateThrottling;
    private final Runnable deactivateThrottling;
    private final ThreadPool threadPool;
    private final AtomicInteger activeMerges = new AtomicInteger(0);
    private final AtomicBoolean isThrottling = new AtomicBoolean(false);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final Semaphore forceMergeLock = new Semaphore(1);
    private final AtomicBoolean frozen = new AtomicBoolean(false);
    private final List<Runnable> onDrainedListeners = new CopyOnWriteArrayList<>();
    private volatile int maxConcurrentMerges;
    private volatile int maxMergeCount;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final IndexSettings indexSettings;
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
     * @param mergeHandler          the handler that selects and executes merges
     * @param applyMergeChanges     callback to apply merge results (e.g., update the catalog)
     * @param onMergeFailureCleanup callback invoked when a merge fails and cleanup is performed
     * @param activateThrottling    callback to activate indexing throttle when merge pressure is high
     * @param deactivateThrottling  callback to deactivate indexing throttle when merge pressure subsides
     * @param shardId               the shard this scheduler is associated with
     * @param indexSettings         the index settings providing merge scheduler configuration
     * @param threadPool            the OpenSearch thread pool for executing merge tasks
     */
    public MergeScheduler(
        MergeHandler mergeHandler,
        BiConsumer<MergeResult, OneMerge> applyMergeChanges,
        Runnable onMergeFailureCleanup,
        Runnable activateThrottling,
        Runnable deactivateThrottling,
        ShardId shardId,
        IndexSettings indexSettings,
        ThreadPool threadPool
    ) {
        this.mergeHandler = mergeHandler;
        this.applyMergeChanges = applyMergeChanges;
        this.onMergeFailureCleanup = onMergeFailureCleanup;
        this.activateThrottling = activateThrottling;
        this.deactivateThrottling = deactivateThrottling;
        this.threadPool = threadPool;
        logger = Loggers.getLogger(getClass(), shardId);
        this.indexSettings = indexSettings;
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
        // Only register new merges if not frozen. Already-pending merges
        // should still be executed to drain the queue to completion.
        if (!isFrozen()) {
            mergeHandler.findAndRegisterMerges();
        }
        evaluateThrottle();
        executeMerge();
    }

    /**
     * Forces a merge down to at most {@code maxNumSegment} segments.
     * Runs synchronously on the calling thread, which must be a
     * {@link ThreadPool.Names#FORCE_MERGE} thread. Only one force merge
     * may execute per shard at a time — concurrent callers block until
     * the ongoing force merge completes.
     *
     * @param maxNumSegment the maximum number of segments after the force merge
     */
    public void forceMerge(int maxNumSegment) throws IOException {
        assert Thread.currentThread().getName().contains(ThreadPool.Names.FORCE_MERGE)
            : "forceMerge must be called on FORCE_MERGE thread but was: " + Thread.currentThread().getName();
        forceMergeLock.acquireUninterruptibly();
        try {
            if (isShutdown.get()) {
                logger.debug("MergeScheduler is shutdown, skipping force merge");
                return;
            }
            Collection<OneMerge> oneMerges = mergeHandler.findForceMerges(maxNumSegment);
            for (OneMerge oneMerge : oneMerges) {
                if (isShutdown.get()) {
                    logger.debug("MergeScheduler shutdown during force merge, aborting remaining merges");
                    break;
                }
                runMerge(oneMerge);
            }
        } finally {
            forceMergeLock.release();
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
     * Freezes the merge scheduler: blocks new merges (in-flight and already-pending merges still drain
     * to completion). Used during tiering preparation to ensure no catalog mutations from merges.
     * <p>
     * Idempotent via {@code compareAndSet} — only the first call that actually flips the state takes
     * effect; redundant calls are no-ops.
     *
     * @return {@code true} if this call transitioned the scheduler from unfrozen to frozen,
     *         {@code false} if it was already frozen
     */
    public boolean freeze() {
        return frozen.compareAndSet(false, true);
    }

    /**
     * Unfreezes the merge scheduler, allowing merges to resume. Called when tiering is cancelled.
     * <p>
     * Idempotent via {@code compareAndSet}: {@link #triggerMerges()} runs only on a real
     * frozen-to-unfrozen transition, so a redundant unfreeze does not kick off a spurious merge cycle.
     *
     * @return {@code true} if this call transitioned the scheduler from frozen to unfrozen,
     *         {@code false} if it was already unfrozen
     */
    public boolean unfreeze() {
        if (frozen.compareAndSet(true, false)) {
            triggerMerges();
            return true;
        }
        return false;
    }

    /**
     * Returns true if the merge scheduler is frozen — either explicitly via {@link #freeze()}
     * or because the index tiering state indicates preparation/migration is in progress.
     */
    public boolean isFrozen() {
        if (frozen.get()) {
            return true;
        }
        String state = indexSettings.getSettings().get(IndexModule.INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.name());
        return IndexModule.TieringState.HOT_TO_WARM.name().equals(state);
    }

    /**
     * Registers a listener that fires when all active merges complete.
     * If already drained (no active merges and no pending), fires the listener immediately
     * inline. Otherwise, adds the listener to the list — all registered listeners will be
     * invoked on the merge thread when the last merge finishes.
     * <p>
     * Multiple listeners can be registered concurrently (thread-safe via CopyOnWriteArrayList).
     * <p>
     * Listeners must be idempotent: under a narrow race between this method's double-check and the
     * merge thread's snapshot-then-clear, the listener may be invoked twice. Gate any side-effects
     * with a {@code compareAndSet} (or equivalent) — see {@code TransportPrepareTieringAction} for
     * the canonical pattern.
     *
     * @param listener the callback to fire when merges are drained
     */
    public void onDrained(Runnable listener) {
        if (activeMerges.get() == 0 && !mergeHandler.hasPendingMerges()) {
            listener.run();
            return;
        }
        onDrainedListeners.add(listener);
        // Double-check after adding — merges may have finished between the check and the add
        if (activeMerges.get() == 0 && !mergeHandler.hasPendingMerges()) {
            if (onDrainedListeners.remove(listener)) {
                listener.run();
            }
        }
    }

    /**
     * Shuts down this merge scheduler, preventing new merges from being submitted.
     */
    public void shutdown() {
        isShutdown.set(true);
    }

    /**
     * Returns the number of currently active (in-flight) merge tasks.
     *
     * @return the active merge count
     */
    public int getActiveMergeCount() {
        return activeMerges.get();
    }

    /**
     * Returns whether there are any merges queued but not yet started.
     * <p>
     * Reports pending state orthogonally from active state: a {@code true} result here
     * means the queue is non-empty regardless of how many merges are currently running.
     * Callers that want a "any work outstanding" signal should combine this with
     * {@link #getActiveMergeCount()}.
     *
     * @return {@code true} if {@link MergeHandler#hasPendingMerges()} is {@code true}
     */
    public boolean hasPendingMerges() {
        return mergeHandler.hasPendingMerges();
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
                onMergeFailureCleanup.run();
            }
        }
    }

    /**
     * Submits a merge task to the thread pool's merge executor.
     *
     * @param oneMerge the merge to execute
     */
    private void submitMergeTask(OneMerge oneMerge) {
        activeMerges.incrementAndGet();
        threadPool.executor(ThreadPool.Names.MERGE).execute(() -> {
            try {
                if (isShutdown.get()) {
                    logger.debug("MergeScheduler is shutdown, skipping merge");
                    return;
                }
                runMerge(oneMerge);
            } catch (Exception e) {
                // runMerge already invoked onMergeFailureCleanup; swallow to prevent
                // uncaught exception on the merge thread pool.
            } finally {
                activeMerges.decrementAndGet();
                evaluateThrottle();
                // Fire all drain listeners if all merges completed and none pending
                if (isFrozen() && activeMerges.get() == 0 && !mergeHandler.hasPendingMerges() && !onDrainedListeners.isEmpty()) {
                    List<Runnable> listeners = List.copyOf(onDrainedListeners);
                    onDrainedListeners.clear();
                    for (Runnable listener : listeners) {
                        try {
                            listener.run();
                        } catch (Exception ex) {
                            logger.warn("Exception in onDrained listener", ex);
                        }
                    }
                }
                // A completed merge may free up capacity for new merges, so check again.
                executeMerge();
            }
        });
    }

    /**
     * Executes a single merge and applies or cleans up the result.
     * <p>
     * This is the single point that owns the merge lifecycle:
     * <ol>
     *   <li>{@code doMerge} — may acquire {@code refreshLock} via the pre-merge-commit hook</li>
     *   <li>On success: {@code applyMergeChanges} — releases {@code refreshLock}</li>
     *   <li>On failure: {@code onMergeFailureCleanup} — releases {@code refreshLock} if still held</li>
     * </ol>
     * By funnelling both background and force merges through this method, the lock
     * release guarantee is maintained in exactly one code path.
     */
    private void runMerge(OneMerge oneMerge) throws IOException {
        long totalSizeInBytes = oneMerge.getTotalSizeInBytes();
        long totalNumDocs = oneMerge.getTotalNumDocs();
        long timeNS = System.nanoTime();
        long tookMS = 0;
        try {
            mergeStatsTracker.beforeMerge(totalNumDocs, totalSizeInBytes);
            MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
            applyMergeChanges.accept(mergeResult, oneMerge);
            mergeHandler.onMergeFinished(oneMerge, isFrozen());
            tookMS = TimeValue.nsecToMSec((System.nanoTime() - timeNS));
            logger.info("Merge {} completed in {}ms, result: {}", oneMerge, tookMS, mergeResult.getMergedWriterFileSet());
        } catch (Exception e) {
            logger.error(new ParameterizedMessage("Merge failed for: {}", oneMerge), e);
            mergeHandler.onMergeFailure(oneMerge);
            onMergeFailureCleanup.run();
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        } finally {
            mergeStatsTracker.afterMerge(tookMS, totalNumDocs, totalSizeInBytes);
        }
    }

    private synchronized void evaluateThrottle() {
        int numMergesInFlight = activeMerges.get() + mergeHandler.getPendingMergeCount();
        if (numMergesInFlight > maxMergeCount) {
            if (isThrottling.getAndSet(true) == false) {
                logger.info("now throttling indexing: numMergesInFlight={}, maxMergeCount={}", numMergesInFlight, maxMergeCount);
                try {
                    activateThrottling.run();
                } catch (Exception e) {
                    logger.warn("exception in activateThrottling callback", e);
                }
            }
        } else if (numMergesInFlight < maxMergeCount) {
            if (isThrottling.getAndSet(false)) {
                logger.info("stop throttling indexing: numMergesInFlight={}, maxMergeCount={}", numMergesInFlight, maxMergeCount);
                try {
                    deactivateThrottling.run();
                } catch (Exception e) {
                    logger.warn("exception in deactivateThrottling callback", e);
                }
            }
        }
    }
}

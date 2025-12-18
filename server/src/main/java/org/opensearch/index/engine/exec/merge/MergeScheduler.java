/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.index.MergeSchedulerConfig;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class MergeScheduler {

    private static Logger logger;
    private final MergeHandler mergeHandler;
    private final CompositeEngine compositeEngine;
    protected int mergeThreadCounter = 0;
    private final List<MergeThread> mergeThreads = new CopyOnWriteArrayList<>();
    private final AtomicInteger activeMerges = new AtomicInteger(0);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private volatile int maxConcurrentMerges;
    private volatile int maxMergeCount;
    private final ShardId shardId;
    private final Settings indexSettings;

    public MergeScheduler(
        MergeHandler mergeHandler,
        CompositeEngine compositeEngine,
        ShardId shardId,
        IndexSettings indexSettings
    ) {
        logger = Loggers.getLogger(getClass(), shardId);
        this.mergeHandler = mergeHandler;
        this.compositeEngine = compositeEngine;
        this.indexSettings = indexSettings.getSettings();
        this.shardId = shardId;
        this.maxConcurrentMerges = 2;
        this.maxMergeCount = maxConcurrentMerges + 5;
    }

    //TODO use this function to refresh the config from IndexSettings.MergeSchedulerConfig
    /**
     * Refreshes merge scheduler configuration from MergeSchedulerConfig.
     * Updates max thread count and max merge count dynamically.
     */
    public synchronized void refreshConfig(MergeSchedulerConfig config) {
        int newMaxThreadCount = config.getMaxThreadCount();
        int newMaxMergeCount = config.getMaxMergeCount();

        if (newMaxThreadCount == this.maxConcurrentMerges && newMaxMergeCount == this.maxMergeCount) {
            return;
        }

        logger.info(() -> new ParameterizedMessage("Updating merge scheduler config: maxThreadCount {} -> {}, " +
            "maxMergeCount {} -> {}", this.maxConcurrentMerges, newMaxThreadCount, this.maxMergeCount, newMaxMergeCount));

        this.maxConcurrentMerges = newMaxThreadCount;
        this.maxMergeCount = newMaxMergeCount;
    }

    /**
     * Triggers merges asynchronously in background threads.
     * This method returns immediately, allowing the calling thread to continue.
     */
    public void triggerMerges() {
        if (isShutdown.get()) {
            logger.warn("MergeScheduler is shutdown, ignoring merge trigger");
            return;
        }

        mergeHandler.updatePendingMerges();

        executeMerge();
    }

    public void forceMerge(int maxNumSegment) throws IOException {
        if(!mergeThreads.isEmpty()) {
            logger.warn("Cannot force merge while background merges are active");
            throw new IllegalStateException("Cannot force merge while background merges are active");
        }
        Collection<OneMerge> oneMerges = mergeHandler.findForceMerges(maxNumSegment);

        for(OneMerge oneMerge : oneMerges) {
            MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
            this.compositeEngine.applyMergeChanges(mergeResult, oneMerge);
        }
    }

    private void executeMerge() {
        // Submit merges up to available capacity
        while(mergeThreads.size() < maxConcurrentMerges && mergeHandler.hasPendingMerges()) {
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
     * Calculates available merge slots based on current system resources.
     */
    private int getAvailableMergeSlots() {
        int currentActive = activeMerges.get();
        return Math.max(0, maxConcurrentMerges - currentActive);
    }

    /**
     * Starts a single merge thread.
     */
    private void submitMergeTask(OneMerge oneMerge) {
        activeMerges.incrementAndGet();
        MergeThread thread = new MergeThread(oneMerge);
        thread.setName(
            OpenSearchExecutors.threadName(indexSettings, "[" + shardId.getIndexName() + "][" + shardId.id() + "]: Merge thread #" + mergeThreadCounter++)
        );
        mergeThreads.add(thread);
        thread.start();
    }

    /**
     * Thread that executes a single merge operation.
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
            try {
                if (isShutdown.get()) {
                    logger.debug("[{}] MergeScheduler is shutdown, skipping merge", getName());
                    return;
                }

                logger.debug("[{}] Starting merge for: {}", getName(), oneMerge);
                long startTime = System.nanoTime();

                MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
                compositeEngine.applyMergeChanges(mergeResult, oneMerge);
                mergeHandler.onMergeFinished(oneMerge);

                long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                logger.info("[{}] Merge completed in {}ms for: {} and output is stored in: {}",
                    getName(), durationMs, oneMerge, mergeResult);

            } catch (Exception e) {
                logger.error("[{}] Unexpected error during merge for: {}", getName(), oneMerge, e);
                mergeHandler.onMergeFailure(oneMerge);
            } finally {
                activeMerges.decrementAndGet();
                mergeThreads.remove(this);
                // triggering merge at the end
                executeMerge();
            }
        }
    }

    /**
     * Returns the number of currently active merge operations.
     */
    public int getActiveMergeCount() {
        return activeMerges.get();
    }

    /**
     * Returns the maximum number of concurrent merges allowed.
     */
    public int getMaxConcurrentMerges() {
        return maxConcurrentMerges;
    }

    /**
     * Returns the maximum number of merges allowed.
     */
    public int getMaxMergeCount() {
        return maxMergeCount;
    }

    /**
     * Shuts down the merge scheduler and waits for active merges to complete.
     */
    //TODO see where we want to call this function for the Merge shutdown
    public void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            logger.info("Shutting down MergeScheduler with {} active merges", activeMerges.get());

            for (MergeThread thread : mergeThreads) {
                try {
                    thread.join(30000);
                    if (thread.isAlive()) {
                        logger.warn("MergeThread {} did not terminate within 30 seconds", thread.getName());
                        thread.interrupt();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            mergeThreads.clear();
        }
    }

    /**
     * Checks if the merge scheduler is shutdown.
     */
    public boolean isShutdown() {
        return isShutdown.get();
    }
}

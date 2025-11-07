/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.index.MergePolicy;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.index.MergeSchedulerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class MergeScheduler {

    private static final Logger logger = LogManager.getLogger(MergeScheduler.class);

    private final MergeHandler mergeHandler;
    private final CompositeEngine compositeEngine;
    private final List<MergeThread> mergeThreads = new CopyOnWriteArrayList<>();
    private final AtomicInteger activeMerges = new AtomicInteger(0);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private volatile int maxConcurrentMerges;
    private volatile int maxMergeCount;

    public MergeScheduler(MergeHandler mergeHandler, CompositeEngine compositeEngine) {
//        this(mergeHandler, compositeEngine, Math.max(1, Runtime.getRuntime().availableProcessors() / 4));
        this(mergeHandler, compositeEngine, 2);

    }

    public MergeScheduler(MergeHandler mergeHandler, CompositeEngine compositeEngine, int maxConcurrentMerges) {
        this.mergeHandler = mergeHandler;
        this.compositeEngine = compositeEngine;
        this.maxConcurrentMerges = maxConcurrentMerges;
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

        logger.info("Updating merge scheduler config: maxThreadCount {} -> {}, maxMergeCount {} -> {}",
            this.maxConcurrentMerges, newMaxThreadCount, this.maxMergeCount, newMaxMergeCount);

        this.maxConcurrentMerges = newMaxThreadCount;
        this.maxMergeCount = newMaxMergeCount;
    }

    /**
     * Triggers merges asynchronously in background threads.
     * This method returns immediately, allowing the calling thread to continue.
     */
    public void triggerMerges() throws IOException {
        if (isShutdown.get()) {
            logger.warn("MergeScheduler is shutdown, ignoring merge trigger");
            return;
        }

        mergeHandler.updatePendingMerges();


        // Submit merges up to available capacity
        int scheduled = 0;
        int availableToSchedule = getAvailableMergeSlots();

        while(mergeThreads.size() < maxConcurrentMerges && mergeHandler.hasPendingMerges()) {
            OneMerge oneMerge = mergeHandler.getNextMerge();
            if (oneMerge == null) {
                return;
            }
            try {
                submitMergeTask(oneMerge);
                scheduled++;
            } catch (Exception e) {
                mergeHandler.onMergeFailure(oneMerge);
            }
        }
    }

    public void forceMerge(int maxNumSegment) throws IOException {
        // TODO: Add validation for background merge before executing force merge
        Collection<OneMerge> oneMerges = mergeHandler.findForceMerges(maxNumSegment);

        for(OneMerge oneMerge : oneMerges) {
            MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
            this.compositeEngine.applyMergeChanges(mergeResult, oneMerge);
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
        mergeThreads.add(thread);
        thread.start();
        System.out.println("Total merge threads : " + mergeThreads.size() + " Active merges : " + activeMerges.get());
    }

    /**
     * Thread that executes a single merge operation.
     */
    private class MergeThread extends Thread {
        private final OneMerge oneMerge;

        MergeThread(OneMerge oneMerge) {
            super("merge-scheduler-" + (mergeThreads.size()+1));
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

                logger.info("[{}] Starting merge for: {}", getName(), oneMerge);
                long startTime = System.nanoTime();

                MergeResult mergeResult = mergeHandler.doMerge(oneMerge);
                compositeEngine.applyMergeChanges(mergeResult, oneMerge);
                mergeHandler.onMergeFinished(oneMerge);

                long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                logger.info("[{}] Merge completed in {}ms for: {}", getName(), durationMs, oneMerge);

                // triggering merge at the end
                triggerMerges();
            } catch (Exception e) {
                logger.error("[{}] Unexpected error during merge for: {}", getName(), oneMerge, e);
                mergeHandler.onMergeFailure(oneMerge);
            } finally {
                activeMerges.decrementAndGet();
                mergeThreads.remove(this);
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-stage atomic counters for query lifecycle metrics: rowsProcessed,
 * bytesRead, tasksCompleted, tasksFailed, startTimeMs, endTimeMs.
 * <p>
 * Created by PlanWalker in {@code dispatchStage()} and stored in a
 * per-stage metrics map. For MVP the counters are incremented but
 * nothing reads them — future: exposed via explain/profile API.
 *
 * @opensearch.internal
 */
public class StageMetrics {

    private final int stageId;
    private final AtomicLong tasksCompleted = new AtomicLong();
    private final AtomicLong tasksFailed = new AtomicLong();
    private final AtomicLong rowsProcessed = new AtomicLong();
    private final AtomicLong bytesRead = new AtomicLong();
    private volatile long startTimeMs;
    private volatile long endTimeMs;

    public StageMetrics(int stageId) {
        this.stageId = stageId;
    }

    /** Record the dispatch start time as current wall-clock millis. */
    public void recordStart() {
        this.startTimeMs = System.currentTimeMillis();
    }

    /** Record the dispatch end time as current wall-clock millis. */
    public void recordEnd() {
        this.endTimeMs = System.currentTimeMillis();
    }

    /** Atomically increment the completed-tasks counter. */
    public void incrementTasksCompleted() {
        tasksCompleted.incrementAndGet();
    }

    /** Atomically increment the failed-tasks counter. */
    public void incrementTasksFailed() {
        tasksFailed.incrementAndGet();
    }

    /** Atomically adds to rowsProcessed. Requires n >= 0. */
    public void addRowsProcessed(long n) {
        if (n < 0) {
            throw new IllegalArgumentException("rowsProcessed delta must be >= 0, got " + n);
        }
        rowsProcessed.addAndGet(n);
    }

    /** Atomically adds to bytesRead. Requires n >= 0. */
    public void addBytesRead(long n) {
        if (n < 0) {
            throw new IllegalArgumentException("bytesRead delta must be >= 0, got " + n);
        }
        bytesRead.addAndGet(n);
    }

    public int getStageId() {
        return stageId;
    }

    public long getTasksCompleted() {
        return tasksCompleted.get();
    }

    public long getTasksFailed() {
        return tasksFailed.get();
    }

    public long getRowsProcessed() {
        return rowsProcessed.get();
    }

    public long getBytesRead() {
        return bytesRead.get();
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getEndTimeMs() {
        return endTimeMs;
    }
}

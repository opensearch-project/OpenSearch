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
 * Stage-aggregate metrics: rowsProcessed, bytesRead, wall-clock start/end.
 * Created by {@link AbstractStageExecution} at construction time and owned
 * for the lifetime of the stage.
 *
 * <p>Per-task identity / state / timing is a separate concern handled by
 * the planned {@code StageTask} model; this class is intentionally
 * task-agnostic.
 *
 * @opensearch.internal
 */
public class StageMetrics {

    private final AtomicLong rowsProcessed = new AtomicLong();
    private final AtomicLong bytesRead = new AtomicLong();
    private volatile long startTimeMs;
    private volatile long endTimeMs;

    public StageMetrics() {}

    /** Record the dispatch start time as current wall-clock millis. */
    public void recordStart() {
        this.startTimeMs = System.currentTimeMillis();
    }

    /** Record the dispatch end time as current wall-clock millis. */
    public void recordEnd() {
        this.endTimeMs = System.currentTimeMillis();
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

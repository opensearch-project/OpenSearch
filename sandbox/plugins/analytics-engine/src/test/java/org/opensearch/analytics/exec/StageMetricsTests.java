/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.stage.StageMetrics;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link StageMetrics} counter accuracy and timestamp recording.
 */
public class StageMetricsTests extends OpenSearchTestCase {

    public void testStageMetricsCounterAccuracy() {
        int stageId = randomIntBetween(0, 100);
        StageMetrics metrics = new StageMetrics(stageId);

        int n = randomIntBetween(1, 50);
        int m = randomIntBetween(1, 50);

        for (int i = 0; i < n; i++) {
            metrics.incrementTasksCompleted();
        }
        for (int i = 0; i < m; i++) {
            metrics.incrementTasksFailed();
        }

        assertEquals("stageId must match the value passed to constructor", stageId, metrics.getStageId());
        assertEquals("tasksCompleted must equal the number of incrementTasksCompleted() calls", n, metrics.getTasksCompleted());
        assertEquals("tasksFailed must equal the number of incrementTasksFailed() calls", m, metrics.getTasksFailed());
    }

    public void testStageMetricsTimestamps() {
        StageMetrics metrics = new StageMetrics(randomIntBetween(0, 100));

        assertEquals("startTimeMs must be 0 before recordStart()", 0L, metrics.getStartTimeMs());
        assertEquals("endTimeMs must be 0 before recordEnd()", 0L, metrics.getEndTimeMs());

        metrics.recordStart();
        assertTrue("startTimeMs must be > 0 after recordStart()", metrics.getStartTimeMs() > 0);

        metrics.recordEnd();
        assertTrue("endTimeMs must be >= startTimeMs after recordEnd()", metrics.getEndTimeMs() >= metrics.getStartTimeMs());
    }

    public void testAddRowsProcessedAtomic() {
        StageMetrics metrics = new StageMetrics(randomIntBetween(0, 100));

        metrics.addRowsProcessed(5);
        metrics.addRowsProcessed(5);

        assertEquals("rowsProcessed must equal the sum of addRowsProcessed calls", 10L, metrics.getRowsProcessed());
    }

    public void testAddBytesReadAtomic() {
        StageMetrics metrics = new StageMetrics(randomIntBetween(0, 100));

        metrics.addBytesRead(5);
        metrics.addBytesRead(5);

        assertEquals("bytesRead must equal the sum of addBytesRead calls", 10L, metrics.getBytesRead());
    }

    public void testNegativeDeltaThrows() {
        StageMetrics metrics = new StageMetrics(randomIntBetween(0, 100));

        expectThrows(IllegalArgumentException.class, () -> metrics.addRowsProcessed(-1));
        expectThrows(IllegalArgumentException.class, () -> metrics.addBytesRead(-1));
    }
}

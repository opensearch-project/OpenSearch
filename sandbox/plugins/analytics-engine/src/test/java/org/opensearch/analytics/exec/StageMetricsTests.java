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

    public void testStageMetricsTimestamps() {
        StageMetrics metrics = new StageMetrics();

        assertEquals("startTimeMs must be 0 before recordStart()", 0L, metrics.getStartTimeMs());
        assertEquals("endTimeMs must be 0 before recordEnd()", 0L, metrics.getEndTimeMs());

        metrics.recordStart();
        assertTrue("startTimeMs must be > 0 after recordStart()", metrics.getStartTimeMs() > 0);

        metrics.recordEnd();
        assertTrue("endTimeMs must be >= startTimeMs after recordEnd()", metrics.getEndTimeMs() >= metrics.getStartTimeMs());
    }

    public void testAddRowsProcessedAtomic() {
        StageMetrics metrics = new StageMetrics();

        metrics.addRowsProcessed(5);
        metrics.addRowsProcessed(5);

        assertEquals("rowsProcessed must equal the sum of addRowsProcessed calls", 10L, metrics.getRowsProcessed());
    }

    public void testAddBytesReadAtomic() {
        StageMetrics metrics = new StageMetrics();

        metrics.addBytesRead(5);
        metrics.addBytesRead(5);

        assertEquals("bytesRead must equal the sum of addBytesRead calls", 10L, metrics.getBytesRead());
    }

    public void testNegativeDeltaThrows() {
        StageMetrics metrics = new StageMetrics();

        expectThrows(IllegalArgumentException.class, () -> metrics.addRowsProcessed(-1));
        expectThrows(IllegalArgumentException.class, () -> metrics.addBytesRead(-1));
    }
}

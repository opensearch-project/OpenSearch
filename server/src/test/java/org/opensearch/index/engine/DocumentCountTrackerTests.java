/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine.Operation;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocumentCountTrackerTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId("test", "_na_", 0);

    private Operation mockPrimaryOp() {
        Operation op = mock(Operation.class);
        when(op.origin()).thenReturn(Operation.Origin.PRIMARY);
        when(op.seqNo()).thenReturn(SequenceNumbers.UNASSIGNED_SEQ_NO);
        return op;
    }

    public void testAcquireWithinLimitReturnsNull() {
        DocumentCountTracker tracker = new DocumentCountTracker(SHARD_ID, () -> 0L, 100);
        assertNull(tracker.tryAcquireInFlightDocs(mockPrimaryOp(), 10));
        assertEquals(10, tracker.getInFlightDocCount());
    }

    public void testAcquireExceedingLimitReturnsException() {
        DocumentCountTracker tracker = new DocumentCountTracker(SHARD_ID, () -> 90L, 100);
        Exception ex = tracker.tryAcquireInFlightDocs(mockPrimaryOp(), 20);
        assertNotNull(ex);
        assertTrue(ex instanceof IllegalArgumentException);
        assertTrue(ex.getMessage().contains("exceeds the limit"));
        // Should have rolled back
        assertEquals(0, tracker.getInFlightDocCount());
    }

    public void testAcquireExactlyAtLimitSucceeds() {
        DocumentCountTracker tracker = new DocumentCountTracker(SHARD_ID, () -> 90L, 100);
        assertNull(tracker.tryAcquireInFlightDocs(mockPrimaryOp(), 10));
        assertEquals(10, tracker.getInFlightDocCount());
    }

    public void testReleaseDecrementsCount() {
        DocumentCountTracker tracker = new DocumentCountTracker(SHARD_ID, () -> 0L, 100);
        tracker.tryAcquireInFlightDocs(mockPrimaryOp(), 5);
        assertEquals(5, tracker.getInFlightDocCount());
        tracker.releaseInFlightDocs(3);
        assertEquals(2, tracker.getInFlightDocCount());
    }

    public void testMultipleAcquiresAccumulate() {
        DocumentCountTracker tracker = new DocumentCountTracker(SHARD_ID, () -> 0L, 100);
        assertNull(tracker.tryAcquireInFlightDocs(mockPrimaryOp(), 30));
        assertNull(tracker.tryAcquireInFlightDocs(mockPrimaryOp(), 30));
        assertNull(tracker.tryAcquireInFlightDocs(mockPrimaryOp(), 30));
        assertEquals(90, tracker.getInFlightDocCount());
        // Next one should fail (90 in-flight + 0 indexed + 20 = 110 > 100)
        Exception ex = tracker.tryAcquireInFlightDocs(mockPrimaryOp(), 20);
        assertNotNull(ex);
        assertEquals(90, tracker.getInFlightDocCount());
    }

    public void testIndexedDocsCountedAgainstLimit() {
        AtomicLong indexedDocs = new AtomicLong(50);
        DocumentCountTracker tracker = new DocumentCountTracker(SHARD_ID, indexedDocs::get, 100);
        // 50 indexed + 40 in-flight = 90, OK
        assertNull(tracker.tryAcquireInFlightDocs(mockPrimaryOp(), 40));
        // 50 indexed + 40 in-flight + 20 = 110, exceeds
        Exception ex = tracker.tryAcquireInFlightDocs(mockPrimaryOp(), 20);
        assertNotNull(ex);
        assertEquals(40, tracker.getInFlightDocCount());
    }
}

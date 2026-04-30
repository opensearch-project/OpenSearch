/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.SourcePartitionAwarePointer;
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for the per-partition pointer tracking added to {@link MessageProcessorRunnable}
 * for the multi-partition checkpoint model.
 */
public class MessageProcessorRunnablePerPartitionTests extends OpenSearchTestCase {

    private MessageProcessorRunnable runnable;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        runnable = new MessageProcessorRunnable(
            new ArrayBlockingQueue<>(1),
            mock(MessageProcessorRunnable.MessageProcessor.class),
            new DropIngestionErrorStrategy("ingestion_source"),
            "test_index",
            0
        );
    }

    public void testNoPointerProcessedYetReturnsNullAndEmpty() {
        assertNull(runnable.getCurrentShardPointer());
        assertTrue(runnable.getCurrentPartitionPointers().isEmpty());
    }

    public void testLegacyPointerOnlyUpdatesCurrentShardPointer() {
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(42);
        runnable.markCurrentPointer(pointer);

        assertEquals(pointer, runnable.getCurrentShardPointer());
        // Legacy pointer is NOT a SourcePartitionAwarePointer — per-partition map should remain empty
        assertTrue(
            "Per-partition map should be empty for legacy (non-SourcePartitionAwarePointer) pointers",
            runnable.getCurrentPartitionPointers().isEmpty()
        );
    }

    public void testSourcePartitionAwarePointerUpdatesBothFields() {
        TestSourcePartitionAwarePointer pointer = new TestSourcePartitionAwarePointer(3, 100L);
        runnable.markCurrentPointer(pointer);

        assertEquals(pointer, runnable.getCurrentShardPointer());

        Map<Integer, IngestionShardPointer> map = runnable.getCurrentPartitionPointers();
        assertEquals(1, map.size());
        assertEquals(pointer, map.get(3));
    }

    public void testMultiplePartitionsTrackedIndependently() {
        TestSourcePartitionAwarePointer p3a = new TestSourcePartitionAwarePointer(3, 100L);
        TestSourcePartitionAwarePointer p7a = new TestSourcePartitionAwarePointer(7, 50L);
        TestSourcePartitionAwarePointer p3b = new TestSourcePartitionAwarePointer(3, 200L);

        runnable.markCurrentPointer(p3a);
        runnable.markCurrentPointer(p7a);
        runnable.markCurrentPointer(p3b);

        // currentShardPointer reflects the last marked pointer
        assertEquals(p3b, runnable.getCurrentShardPointer());

        // Per-partition map has the latest pointer per partition
        Map<Integer, IngestionShardPointer> map = runnable.getCurrentPartitionPointers();
        assertEquals(2, map.size());
        assertEquals(p3b, map.get(3)); // overwritten with newer offset
        assertEquals(p7a, map.get(7));
    }

    public void testReturnedMapIsImmutableSnapshot() {
        runnable.markCurrentPointer(new TestSourcePartitionAwarePointer(3, 100L));
        Map<Integer, IngestionShardPointer> snapshot = runnable.getCurrentPartitionPointers();

        // Snapshot is immutable
        expectThrows(UnsupportedOperationException.class, () -> snapshot.put(99, new TestSourcePartitionAwarePointer(99, 1L)));

        // Snapshot does NOT reflect later updates
        runnable.markCurrentPointer(new TestSourcePartitionAwarePointer(7, 50L));
        assertEquals(1, snapshot.size());
        assertFalse(snapshot.containsKey(7));

        // A new call to getCurrentPartitionPointers() reflects the update
        assertEquals(2, runnable.getCurrentPartitionPointers().size());
    }

    public void testMixedPartitionAwareAndLegacyPointers() {
        // Mark a partition-aware pointer first
        TestSourcePartitionAwarePointer p3 = new TestSourcePartitionAwarePointer(3, 100L);
        runnable.markCurrentPointer(p3);
        assertEquals(1, runnable.getCurrentPartitionPointers().size());

        // Mark a legacy pointer next — currentShardPointer updates, per-partition map untouched
        FakeIngestionSource.FakeIngestionShardPointer legacy = new FakeIngestionSource.FakeIngestionShardPointer(999);
        runnable.markCurrentPointer(legacy);
        assertEquals(legacy, runnable.getCurrentShardPointer());

        // Per-partition entry from earlier still present
        Map<Integer, IngestionShardPointer> map = runnable.getCurrentPartitionPointers();
        assertEquals(1, map.size());
        assertEquals(p3, map.get(3));
    }

    /**
     * Test-only pointer that implements both {@link IngestionShardPointer} and
     * {@link SourcePartitionAwarePointer}. Mirrors the structure of {@code KafkaPartitionOffset}
     * without depending on the Kafka plugin.
     */
    static class TestSourcePartitionAwarePointer implements IngestionShardPointer, SourcePartitionAwarePointer {
        private final int partition;
        private final long offset;

        TestSourcePartitionAwarePointer(int partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }

        @Override
        public int getSourcePartition() {
            return partition;
        }

        @Override
        public byte[] serialize() {
            ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
            buf.putInt(partition);
            buf.putLong(offset);
            return buf.array();
        }

        @Override
        public String asString() {
            return partition + ":" + offset;
        }

        @Override
        public Field asPointField(String fieldName) {
            return new LongPoint(fieldName, ((long) partition << 48) | (offset & 0x0000FFFFFFFFFFFFL));
        }

        @Override
        public Query newRangeQueryGreaterThan(String fieldName) {
            long lower = ((long) partition << 48) | (offset & 0x0000FFFFFFFFFFFFL);
            long upper = ((long) partition << 48) | 0x0000FFFFFFFFFFFFL;
            return LongPoint.newRangeQuery(fieldName, lower, upper);
        }

        @Override
        public int compareTo(IngestionShardPointer o) {
            TestSourcePartitionAwarePointer other = (TestSourcePartitionAwarePointer) o;
            int cmp = Integer.compare(partition, other.partition);
            return cmp != 0 ? cmp : Long.compare(offset, other.offset);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestSourcePartitionAwarePointer that)) return false;
            return partition == that.partition && offset == that.offset;
        }

        @Override
        public int hashCode() {
            return 31 * partition + Long.hashCode(offset);
        }
    }
}

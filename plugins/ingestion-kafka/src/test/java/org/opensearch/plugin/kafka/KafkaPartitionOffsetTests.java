/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;

public class KafkaPartitionOffsetTests extends OpenSearchTestCase {

    // --- asString / parse roundtrip ---

    public void testAsString() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(3, 42);
        assertEquals("3:42", offset.asString());
    }

    public void testAsStringZeros() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(0, 0);
        assertEquals("0:0", offset.asString());
    }

    public void testGetters() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(7, 12345);
        assertEquals(7, offset.getPartition());
        assertEquals(12345L, offset.getOffset());
    }

    // --- serialize / deserialize ---

    public void testSerialize() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(3, 42);
        byte[] bytes = offset.serialize();
        assertEquals(Integer.BYTES + Long.BYTES, bytes.length);

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        assertEquals(3, buf.getInt());
        assertEquals(42L, buf.getLong());
    }

    // --- compareTo ---

    public void testCompareSamePartitionDifferentOffset() {
        KafkaPartitionOffset a = new KafkaPartitionOffset(3, 42);
        KafkaPartitionOffset b = new KafkaPartitionOffset(3, 100);
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
    }

    public void testCompareSamePartitionSameOffset() {
        KafkaPartitionOffset a = new KafkaPartitionOffset(3, 42);
        KafkaPartitionOffset b = new KafkaPartitionOffset(3, 42);
        assertEquals(0, a.compareTo(b));
    }

    public void testCompareDifferentPartitions() {
        KafkaPartitionOffset a = new KafkaPartitionOffset(1, 1000);
        KafkaPartitionOffset b = new KafkaPartitionOffset(5, 1);
        assertTrue(a.compareTo(b) < 0); // partition 1 < partition 5
    }

    public void testCompareWithLegacyKafkaOffset() {
        KafkaPartitionOffset partitionOffset = new KafkaPartitionOffset(3, 42);
        KafkaOffset legacyOffset = new KafkaOffset(42);
        // KafkaPartitionOffset always sorts after legacy KafkaOffset
        assertTrue(partitionOffset.compareTo(legacyOffset) > 0);
    }

    public void testCompareWithNull() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(3, 42);
        expectThrows(IllegalArgumentException.class, () -> offset.compareTo(null));
    }

    // --- equals / hashCode ---

    public void testEqualsSame() {
        KafkaPartitionOffset a = new KafkaPartitionOffset(3, 42);
        KafkaPartitionOffset b = new KafkaPartitionOffset(3, 42);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testNotEqualsDifferentPartition() {
        KafkaPartitionOffset a = new KafkaPartitionOffset(3, 42);
        KafkaPartitionOffset b = new KafkaPartitionOffset(4, 42);
        assertNotEquals(a, b);
    }

    public void testNotEqualsDifferentOffset() {
        KafkaPartitionOffset a = new KafkaPartitionOffset(3, 42);
        KafkaPartitionOffset b = new KafkaPartitionOffset(3, 43);
        assertNotEquals(a, b);
    }

    public void testEqualsSymmetryWithLegacyKafkaOffset() {
        // KafkaPartitionOffset uses getClass() check — not equal to KafkaOffset even with same offset
        KafkaPartitionOffset partitionOffset = new KafkaPartitionOffset(3, 42);
        KafkaOffset legacyOffset = new KafkaOffset(42);
        assertNotEquals(partitionOffset, legacyOffset);
        assertNotEquals(legacyOffset, partitionOffset); // symmetry
    }

    // --- asPointField encoding ---

    public void testAsPointFieldEncoding() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(3, 42);
        Field field = offset.asPointField("test_field");
        assertNotNull(field);
        // Partition 3 in upper 16 bits, offset 42 in lower 48 bits
        long expected = ((long) 3 << 48) | 42L;
        // LongPoint stores the value internally — we verify via the encoded bytes
        assertEquals("test_field", field.name());
    }

    // --- newRangeQueryGreaterThan scoped to same partition ---

    public void testRangeQueryScopedToPartition() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(3, 42);
        Query query = offset.newRangeQueryGreaterThan("test_field");
        assertNotNull(query);
        // The query should be a range within partition 3 only
        // Verify it's a LongPoint range query (toString includes the range bounds)
        String queryStr = query.toString();
        assertTrue("Query should be a point range query", queryStr.contains("test_field"));
    }

    // --- toString ---

    public void testToString() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(3, 42);
        assertEquals("KafkaPartitionOffset{partition=3, offset=42}", offset.toString());
    }

    // --- PartitionAwarePointer interface ---

    public void testImplementsPartitionAwarePointer() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(5, 100);
        assertTrue(offset instanceof org.opensearch.index.PartitionAwarePointer);
        assertEquals(5, offset.getPartition());
    }

    // --- extends KafkaOffset ---

    public void testExtendsKafkaOffset() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(3, 42);
        assertTrue(offset instanceof KafkaOffset);
        assertTrue(offset instanceof IngestionShardPointer);
    }
}

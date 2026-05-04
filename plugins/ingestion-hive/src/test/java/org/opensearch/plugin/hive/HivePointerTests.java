/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.opensearch.test.OpenSearchTestCase;

public class HivePointerTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        HivePointer pointer = new HivePointer("dt=2026-04-15", "file:///data/part-00000.parquet", 42, 100);

        assertEquals("dt=2026-04-15", pointer.getPartitionName());
        assertEquals("file:///data/part-00000.parquet", pointer.getFilePath());
        assertEquals(42, pointer.getRowIndex());
        assertEquals(100, pointer.getSequenceNumber());
    }

    public void testAsString() {
        HivePointer pointer = new HivePointer("dt=2026-04-15", "file:///data/part-00000.parquet", 42, 100);
        assertEquals("dt=2026-04-15|file:///data/part-00000.parquet|42|100", pointer.asString());
    }

    public void testFromString() {
        HivePointer pointer = HivePointer.fromString("dt=2026-04-15|file:///data/part-00000.parquet|42|100");

        assertEquals("dt=2026-04-15", pointer.getPartitionName());
        assertEquals("file:///data/part-00000.parquet", pointer.getFilePath());
        assertEquals(42, pointer.getRowIndex());
        assertEquals(100, pointer.getSequenceNumber());
    }

    public void testRoundTrip() {
        HivePointer original = new HivePointer("dt=2026-04-15/hour=03", "s3://bucket/path/file.parquet", 1000, 5000);
        HivePointer restored = HivePointer.fromString(original.asString());

        assertEquals(original, restored);
        assertEquals(original.hashCode(), restored.hashCode());
    }

    public void testSerialize() {
        HivePointer pointer = new HivePointer("dt=2026-04-15", "file:///data/part.parquet", 0, 123);
        byte[] bytes = pointer.serialize();

        assertNotNull(bytes);
        assertTrue(bytes.length > Long.BYTES);
    }

    public void testAsPointField() {
        HivePointer pointer = new HivePointer("dt=2026-04-15", "file:///data/part.parquet", 0, 123);
        Field field = pointer.asPointField("pointer_field");

        assertTrue(field instanceof LongPoint);
    }

    public void testNewRangeQueryGreaterThan() {
        HivePointer pointer = new HivePointer("dt=2026-04-15", "file:///data/part.parquet", 0, 123);
        Query query = pointer.newRangeQueryGreaterThan("pointer_field");

        assertTrue(query instanceof PointRangeQuery);
    }

    public void testCompareTo() {
        HivePointer p1 = new HivePointer("dt=2026-04-15", "file:///a.parquet", 0, 100);
        HivePointer p2 = new HivePointer("dt=2026-04-16", "file:///b.parquet", 0, 200);
        HivePointer p3 = new HivePointer("dt=2026-04-15", "file:///a.parquet", 0, 100);

        assertTrue(p1.compareTo(p2) < 0);
        assertTrue(p2.compareTo(p1) > 0);
        assertEquals(0, p1.compareTo(p3));
    }

    public void testEqualsAndHashCode() {
        HivePointer p1 = new HivePointer("dt=2026-04-15", "file:///a.parquet", 10, 100);
        HivePointer p2 = new HivePointer("dt=2026-04-15", "file:///a.parquet", 10, 100);
        HivePointer p3 = new HivePointer("dt=2026-04-16", "file:///b.parquet", 20, 200);

        assertEquals(p1, p2);
        assertEquals(p1.hashCode(), p2.hashCode());
        assertNotEquals(p1, p3);
    }

    public void testToString() {
        HivePointer pointer = new HivePointer("dt=2026-04-15", "file:///a.parquet", 0, 100);
        assertTrue(pointer.toString().contains("dt=2026-04-15"));
    }
}

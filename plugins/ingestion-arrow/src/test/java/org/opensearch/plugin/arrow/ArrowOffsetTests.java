/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.nio.ByteBuffer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowOffsetTests extends OpenSearchTestCase {

    public void testSerializeDeserialize() {
        ArrowOffset offset = new ArrowOffset(42L);
        byte[] serialized = offset.serialize();
        long deserialized = ByteBuffer.wrap(serialized).getLong();
        assertEquals(offset.getOffset(), deserialized);
    }

    public void testAsStringAndFromStringRoundTrip() {
        ArrowOffset offset = new ArrowOffset(123L);
        ArrowOffset parsed = ArrowOffset.fromString(offset.asString());
        assertEquals(offset.getOffset(), parsed.getOffset());
    }

    public void testToString() {
        ArrowOffset offset = new ArrowOffset(7L);
        assertEquals("ArrowOffset{offset=7}", offset.toString());
    }

    public void testAsPointField() {
        ArrowOffset offset = new ArrowOffset(10L);
        Field field = offset.asPointField("my_field");
        assertNotNull(field);
    }

    public void testNewRangeQueryGreaterThan() {
        ArrowOffset offset = new ArrowOffset(10L);
        Query query = offset.newRangeQueryGreaterThan("my_field");
        assertNotNull(query);
    }

    public void testCompareTo() {
        ArrowOffset smaller = new ArrowOffset(1L);
        ArrowOffset larger = new ArrowOffset(2L);
        assertTrue(smaller.compareTo(larger) < 0);
        assertTrue(larger.compareTo(smaller) > 0);
        assertEquals(0, smaller.compareTo(new ArrowOffset(1L)));
    }

    public void testCompareToNullThrows() {
        ArrowOffset offset = new ArrowOffset(1L);
        expectThrows(IllegalArgumentException.class, () -> offset.compareTo(null));
    }

    public void testCompareToWrongTypeThrows() {
        ArrowOffset offset = new ArrowOffset(1L);
        IngestionShardPointer other = new IngestionShardPointer() {
            @Override
            public byte[] serialize() {
                return new byte[0];
            }

            @Override
            public String asString() {
                return "other";
            }

            @Override
            public Field asPointField(String fieldName) {
                return null;
            }

            @Override
            public Query newRangeQueryGreaterThan(String fieldName) {
                return null;
            }

            @Override
            public int compareTo(IngestionShardPointer o) {
                return 0;
            }
        };
        expectThrows(IllegalArgumentException.class, () -> offset.compareTo(other));
    }
}

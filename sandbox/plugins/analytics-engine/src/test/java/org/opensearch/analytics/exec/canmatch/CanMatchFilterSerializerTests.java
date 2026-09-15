/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class CanMatchFilterSerializerTests extends OpenSearchTestCase {

    public void testRoundTripSingleFilter() throws IOException {
        LongRange original = new LongRange("@timestamp", 1720000000000L, Long.MAX_VALUE);
        byte[] bytes = CanMatchFilterSerializer.serialize(List.of(original));
        List<CanMatchFilter> result = CanMatchFilterSerializer.deserialize(bytes);

        assertEquals(1, result.size());
        assertTrue(result.get(0) instanceof LongRange);
        LongRange decoded = (LongRange) result.get(0);
        assertEquals("@timestamp", decoded.column());
        assertEquals(1720000000000L, decoded.min());
        assertEquals(Long.MAX_VALUE, decoded.max());
    }

    public void testRoundTripMultipleFilters() throws IOException {
        List<CanMatchFilter> originals = List.of(
            new LongRange("@timestamp", 1720000000000L, 1720600000000L),
            new LongRange("port", 80L, 443L)
        );
        byte[] bytes = CanMatchFilterSerializer.serialize(originals);
        List<CanMatchFilter> result = CanMatchFilterSerializer.deserialize(bytes);

        assertEquals(2, result.size());
        LongRange ts = (LongRange) result.get(0);
        assertEquals("@timestamp", ts.column());
        assertEquals(1720000000000L, ts.min());
        assertEquals(1720600000000L, ts.max());

        LongRange port = (LongRange) result.get(1);
        assertEquals("port", port.column());
        assertEquals(80L, port.min());
        assertEquals(443L, port.max());
    }

    public void testEmptyListSerializesToZeroBytes() throws IOException {
        byte[] bytes = CanMatchFilterSerializer.serialize(List.of());
        assertEquals(0, bytes.length);
    }

    public void testNullListSerializesToZeroBytes() throws IOException {
        byte[] bytes = CanMatchFilterSerializer.serialize(null);
        assertEquals(0, bytes.length);
    }

    public void testDeserializeEmptyBytesReturnsEmptyList() throws IOException {
        List<CanMatchFilter> result = CanMatchFilterSerializer.deserialize(new byte[0]);
        assertTrue(result.isEmpty());
    }

    public void testDeserializeNullReturnsEmptyList() throws IOException {
        List<CanMatchFilter> result = CanMatchFilterSerializer.deserialize(null);
        assertTrue(result.isEmpty());
    }

    public void testUnknownFormatVersionReturnsEmptyList() throws IOException {
        // Write a fake payload with format version 99
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(99);  // unknown version
            out.writeVInt(1);   // 1 filter
            // write some garbage for the filter
            out.writeVInt(20);  // byteLength
            out.writeString("LongRange");
            out.writeString("col");
            out.writeLong(0);
            out.writeLong(100);
            byte[] bytes = org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes());

            List<CanMatchFilter> result = CanMatchFilterSerializer.deserialize(bytes);
            assertTrue(result.isEmpty());
        }
    }

    public void testUnknownTypeStringIsSkipped() throws IOException {
        // Manually build: [FORMAT_VERSION=1, count=2, filter1=unknown, filter2=LongRange]
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);   // FORMAT_VERSION
            out.writeVInt(2);   // 2 filters

            // Filter 1: unknown type
            try (BytesStreamOutput filterOut = new BytesStreamOutput()) {
                filterOut.writeString("FutureType");
                filterOut.writeString("some_column");
                filterOut.writeLong(42);
                byte[] filterBytes = org.opensearch.core.common.bytes.BytesReference.toBytes(filterOut.bytes());
                int filterLen = filterOut.size();
                out.writeVInt(filterLen);
                out.writeBytes(filterBytes, 0, filterLen);
            }

            // Filter 2: valid LongRange
            try (BytesStreamOutput filterOut = new BytesStreamOutput()) {
                filterOut.writeString("LongRange");
                filterOut.writeString("@timestamp");
                filterOut.writeLong(1000L);
                filterOut.writeLong(2000L);
                byte[] filterBytes = org.opensearch.core.common.bytes.BytesReference.toBytes(filterOut.bytes());
                int filterLen = filterOut.size();
                out.writeVInt(filterLen);
                out.writeBytes(filterBytes, 0, filterLen);
            }

            byte[] bytes = org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes());
            int length = bytes.length;
            byte[] trimmed = new byte[length];
            System.arraycopy(bytes, 0, trimmed, 0, length);

            List<CanMatchFilter> result = CanMatchFilterSerializer.deserialize(trimmed);
            // Unknown filter skipped, valid filter preserved
            assertEquals(1, result.size());
            LongRange decoded = (LongRange) result.get(0);
            assertEquals("@timestamp", decoded.column());
            assertEquals(1000L, decoded.min());
            assertEquals(2000L, decoded.max());
        }
    }

    public void testMinMaxBoundaries() throws IOException {
        LongRange original = new LongRange("col", Long.MIN_VALUE, Long.MAX_VALUE);
        byte[] bytes = CanMatchFilterSerializer.serialize(List.of(original));
        List<CanMatchFilter> result = CanMatchFilterSerializer.deserialize(bytes);

        assertEquals(1, result.size());
        LongRange decoded = (LongRange) result.get(0);
        assertEquals(Long.MIN_VALUE, decoded.min());
        assertEquals(Long.MAX_VALUE, decoded.max());
    }

    public void testRecordEquality() {
        LongRange a = new LongRange("col", 10, 20);
        LongRange b = new LongRange("col", 10, 20);
        LongRange c = new LongRange("col", 10, 21);
        assertEquals(a, b);
        assertNotEquals(a, c);
        assertEquals(a.hashCode(), b.hashCode());
    }
}

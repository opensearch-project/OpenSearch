/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class NativeAllocatorPoolStatsTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        List<NativeAllocatorPoolStats.PoolStats> pools = List.of(
            new NativeAllocatorPoolStats.PoolStats("flight", 1000, 2000, 3000),
            new NativeAllocatorPoolStats.PoolStats("query", 4000, 5000, 6000)
        );
        NativeAllocatorPoolStats original = new NativeAllocatorPoolStats(10000, 20000, 30000, pools);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        NativeAllocatorPoolStats deserialized = new NativeAllocatorPoolStats(in);

        assertEquals(original.getRootAllocatedBytes(), deserialized.getRootAllocatedBytes());
        assertEquals(original.getRootPeakBytes(), deserialized.getRootPeakBytes());
        assertEquals(original.getRootLimitBytes(), deserialized.getRootLimitBytes());
        assertEquals(original.getPools().size(), deserialized.getPools().size());

        for (int i = 0; i < pools.size(); i++) {
            NativeAllocatorPoolStats.PoolStats orig = original.getPools().get(i);
            NativeAllocatorPoolStats.PoolStats deser = deserialized.getPools().get(i);
            assertEquals(orig.getName(), deser.getName());
            assertEquals(orig.getAllocatedBytes(), deser.getAllocatedBytes());
            assertEquals(orig.getPeakBytes(), deser.getPeakBytes());
            assertEquals(orig.getLimitBytes(), deser.getLimitBytes());
        }
    }

    public void testEmptyPoolsSerialization() throws IOException {
        NativeAllocatorPoolStats original = new NativeAllocatorPoolStats(0, 0, 16000000000L, List.of());

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        NativeAllocatorPoolStats deserialized = new NativeAllocatorPoolStats(in);

        assertEquals(0, deserialized.getRootAllocatedBytes());
        assertEquals(0, deserialized.getRootPeakBytes());
        assertEquals(16000000000L, deserialized.getRootLimitBytes());
        assertTrue(deserialized.getPools().isEmpty());
    }

    /**
     * Asserts the JSON shape: {@code root}/{@code pools.<name>} blocks expose
     * {@code allocated_bytes}, {@code peak_bytes}, and {@code limit_bytes}. Caller is
     * responsible for the outer {@code native_allocator} wrapper, so this test does
     * not expect it.
     */
    public void testToXContent() throws IOException {
        List<NativeAllocatorPoolStats.PoolStats> pools = List.of(
            new NativeAllocatorPoolStats.PoolStats("flight", 1024, 1048576, 2147483648L)
        );
        NativeAllocatorPoolStats stats = new NativeAllocatorPoolStats(4096, 8192, 17179869184L, pools);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        assertTrue(json.contains("\"root\""));
        assertTrue(json.contains("\"pools\""));
        assertTrue(json.contains("\"flight\""));
        assertTrue(json.contains("\"allocated_bytes\""));
        assertTrue(json.contains("\"peak_bytes\""));
        assertTrue(json.contains("\"limit_bytes\""));

        // Removed fields must NOT appear in the JSON.
        assertFalse("child_count was dropped from the stats shape", json.contains("\"child_count\""));
        assertFalse("human-readable byte string was dropped", json.contains("\"allocated\":"));
        assertFalse("human-readable byte string was dropped", json.contains("\"limit\":"));
    }

    public void testPoolStatsSerializationRoundTrip() throws IOException {
        NativeAllocatorPoolStats.PoolStats original = new NativeAllocatorPoolStats.PoolStats("datafusion", 123456, 234567, 8589934592L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        NativeAllocatorPoolStats.PoolStats deserialized = new NativeAllocatorPoolStats.PoolStats(in);

        assertEquals("datafusion", deserialized.getName());
        assertEquals(123456, deserialized.getAllocatedBytes());
        assertEquals(234567, deserialized.getPeakBytes());
        assertEquals(8589934592L, deserialized.getLimitBytes());
    }
}

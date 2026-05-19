/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.opensearch.arrow.spi.NativeAllocatorPoolStats;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class NativeAllocatorPluginStatsTests extends OpenSearchTestCase {

    public void testWriteableNameIsStable() {
        NativeAllocatorPluginStats stats = new NativeAllocatorPluginStats(
            new NativeAllocatorPoolStats(0L, 0L, 0L, List.of())
        );
        assertEquals("native_allocator", stats.getWriteableName());
    }

    public void testStreamRoundTrip() throws Exception {
        List<NativeAllocatorPoolStats.PoolStats> pools = List.of(
            new NativeAllocatorPoolStats.PoolStats("flight", 100L, 200L, 1000L, 1),
            new NativeAllocatorPoolStats.PoolStats("query", 300L, 400L, 2000L, 2)
        );
        NativeAllocatorPluginStats original = new NativeAllocatorPluginStats(
            new NativeAllocatorPoolStats(400L, 600L, 8000L, pools)
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
            NativeAllocatorPluginStats deser = new NativeAllocatorPluginStats(in);
            assertEquals(original.getPoolStats().getRootAllocatedBytes(), deser.getPoolStats().getRootAllocatedBytes());
            assertEquals(original.getPoolStats().getRootPeakBytes(), deser.getPoolStats().getRootPeakBytes());
            assertEquals(original.getPoolStats().getRootLimitBytes(), deser.getPoolStats().getRootLimitBytes());
            assertEquals(original.getPoolStats().getPools().size(), deser.getPoolStats().getPools().size());
        }
    }

    public void testToXContentEmitsInnerFieldsOnly() throws Exception {
        NativeAllocatorPluginStats stats = new NativeAllocatorPluginStats(
            new NativeAllocatorPoolStats(
                100L,
                200L,
                1000L,
                List.of(new NativeAllocatorPoolStats.PoolStats("flight", 50L, 75L, 500L, 1))
            )
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        // Caller (NodeStats) is responsible for opening the named object.
        builder.startObject(stats.getWriteableName());
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();

        String json = BytesReference.bytes(builder).utf8ToString();
        assertTrue("expected native_allocator block at top level", json.contains("\"native_allocator\":"));
        assertTrue("expected inner root block", json.contains("\"root\""));
        assertTrue("expected inner pools block", json.contains("\"pools\""));
        assertTrue("expected pool name 'flight'", json.contains("\"flight\""));
    }
}

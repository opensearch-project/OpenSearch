/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPoolStats;

import java.util.Map;

/** Unit tests for {@link ParquetIngestPoolStats}: XContent shape, stream round-trip, factory. */
public class ParquetIngestPoolStatsTests extends OpenSearchTestCase {

    public void testToXContentShape() {
        ParquetIngestPoolStats stats = new ParquetIngestPoolStats(4, 12, 3, 7L, 100L);
        Map<String, Object> json = toMap(stats);
        @SuppressWarnings("unchecked")
        Map<String, Object> pool = (Map<String, Object>) json.get("native_ingest_pool");
        assertNotNull("native_ingest_pool block must be present", pool);
        assertEquals(4, ((Number) pool.get("threads")).intValue());
        assertEquals(12, ((Number) pool.get("queue_depth")).intValue());
        assertEquals(3, ((Number) pool.get("active")).intValue());
        assertEquals(7L, ((Number) pool.get("rejected")).longValue());
        assertEquals(100L, ((Number) pool.get("completed")).longValue());
    }

    public void testStreamRoundTrip() throws Exception {
        ParquetIngestPoolStats original = new ParquetIngestPoolStats(2, 5, 1, 9L, 42L);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
            ParquetIngestPoolStats restored = new ParquetIngestPoolStats(in);
            assertEquals(toMap(original), toMap(restored));
        }
    }

    public void testFromThreadPoolStats() {
        ThreadPoolStats.Stats s = new ThreadPoolStats.Stats.Builder().name("parquet_native_write")
            .threads(4)
            .queue(12)
            .active(3)
            .rejected(7L)
            .largest(20)
            .completed(100L)
            .build();
        ParquetIngestPoolStats stats = ParquetIngestPoolStats.from(s);
        assertNotNull(stats);
        Map<String, Object> pool = poolBlock(stats);
        assertEquals(12, ((Number) pool.get("queue_depth")).intValue());
        assertEquals(7L, ((Number) pool.get("rejected")).longValue());
    }

    public void testFromNullReturnsNull() {
        assertNull(ParquetIngestPoolStats.from(null));
    }

    private static Map<String, Object> toMap(ParquetIngestPoolStats stats) {
        try {
            var builder = XContentFactory.jsonBuilder().startObject();
            stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, builder.toString(), true);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> poolBlock(ParquetIngestPoolStats stats) {
        return (Map<String, Object>) toMap(stats).get("native_ingest_pool");
    }
}

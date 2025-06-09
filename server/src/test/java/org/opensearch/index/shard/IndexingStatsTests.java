/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

public class IndexingStatsTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        IndexingStats stats = createTestInstance();

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                IndexingStats deserializedStats = new IndexingStats(in);

                if (stats.getTotal() == null) {
                    assertNull(deserializedStats.getTotal());
                    return;
                }

                IndexingStats.Stats totalStats = stats.getTotal();
                IndexingStats.Stats deserializedTotalStats = deserializedStats.getTotal();

                assertEquals(totalStats.getIndexCount(), deserializedTotalStats.getIndexCount());
                assertEquals(totalStats.getIndexTime(), deserializedTotalStats.getIndexTime());
                assertEquals(totalStats.getIndexCurrent(), deserializedTotalStats.getIndexCurrent());
                assertEquals(totalStats.getIndexFailedCount(), deserializedTotalStats.getIndexFailedCount());
                assertEquals(totalStats.getDeleteCount(), deserializedTotalStats.getDeleteCount());
                assertEquals(totalStats.getDeleteTime(), deserializedTotalStats.getDeleteTime());
                assertEquals(totalStats.getDeleteCurrent(), deserializedTotalStats.getDeleteCurrent());
                assertEquals(totalStats.getNoopUpdateCount(), deserializedTotalStats.getNoopUpdateCount());
                assertEquals(totalStats.isThrottled(), deserializedTotalStats.isThrottled());
                assertEquals(totalStats.getThrottleTime(), deserializedTotalStats.getThrottleTime());

                if (totalStats.getDocStatusStats() == null) {
                    assertNull(deserializedTotalStats.getDocStatusStats());
                    return;
                }

                IndexingStats.Stats.DocStatusStats docStatusStats = totalStats.getDocStatusStats();
                IndexingStats.Stats.DocStatusStats deserializedDocStatusStats = deserializedTotalStats.getDocStatusStats();

                assertTrue(
                    Arrays.equals(
                        docStatusStats.getDocStatusCounter(),
                        deserializedDocStatusStats.getDocStatusCounter(),
                        Comparator.comparingLong(AtomicLong::longValue)
                    )
                );
            }
        }
    }

    public void testToXContentForIndexingStats() throws IOException {
        IndexingStats stats = createTestInstance();
        IndexingStats.Stats totalStats = stats.getTotal();
        AtomicLong[] counter = totalStats.getDocStatusStats().getDocStatusCounter();

        String expected = "{\"indexing\":{\"index_total\":"
            + totalStats.getIndexCount()
            + ",\"index_time_in_millis\":"
            + totalStats.getIndexTime().getMillis()
            + ",\"index_current\":"
            + totalStats.getIndexCurrent()
            + ",\"index_failed\":"
            + totalStats.getIndexFailedCount()
            + ",\"delete_total\":"
            + totalStats.getDeleteCount()
            + ",\"delete_time_in_millis\":"
            + totalStats.getDeleteTime().getMillis()
            + ",\"delete_current\":"
            + totalStats.getDeleteCurrent()
            + ",\"noop_update_total\":"
            + totalStats.getNoopUpdateCount()
            + ",\"is_throttled\":"
            + totalStats.isThrottled()
            + ",\"throttle_time_in_millis\":"
            + totalStats.getThrottleTime().getMillis()
            + ",\"doc_status\":{\"1xx\":"
            + counter[0]
            + ",\"2xx\":"
            + counter[1]
            + ",\"3xx\":"
            + counter[2]
            + ",\"4xx\":"
            + counter[3]
            + ",\"5xx\":"
            + counter[4]
            + "}}}";

        XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        xContentBuilder.startObject();
        xContentBuilder = stats.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();

        assertEquals(expected, xContentBuilder.toString());
    }

    /**
     * Tests aggregation logic for maxLastIndexRequestTimestamp in IndexingStats.Stats.
     * Uses reflection because the field is private and not settable via public API.
     * This ensures that aggregation (add) always surfaces the maximum value, even across multiple adds and random values.
     */
    public void testMaxLastIndexRequestTimestampAggregation() throws Exception {
        IndexingStats.Stats stats1 = new IndexingStats.Stats();
        IndexingStats.Stats stats2 = new IndexingStats.Stats();
        IndexingStats.Stats stats3 = new IndexingStats.Stats();
        java.lang.reflect.Field tsField = IndexingStats.Stats.class.getDeclaredField("maxLastIndexRequestTimestamp");
        tsField.setAccessible(true);

        // Use random values for robustness
        long ts1 = randomLongBetween(0, 1000000);
        long ts2 = randomLongBetween(0, 1000000);
        long ts3 = randomLongBetween(0, 1000000);

        tsField.set(stats1, ts1);
        tsField.set(stats2, ts2);
        tsField.set(stats3, ts3);

        // Aggregate stats1 + stats2
        stats1.add(stats2);
        assertEquals(Math.max(ts1, ts2), stats1.getMaxLastIndexRequestTimestamp());

        // Aggregate stats1 + stats3
        stats1.add(stats3);
        assertEquals(Math.max(Math.max(ts1, ts2), ts3), stats1.getMaxLastIndexRequestTimestamp());

        // Test with zero and negative values
        tsField.set(stats1, 0L);
        tsField.set(stats2, -100L);
        stats1.add(stats2);
        assertEquals(0L, stats1.getMaxLastIndexRequestTimestamp());

        tsField.set(stats1, -50L);
        tsField.set(stats2, -100L);
        stats1.add(stats2);
        assertEquals(-50L, stats1.getMaxLastIndexRequestTimestamp());
    }

    private IndexingStats createTestInstance() {
        IndexingStats.Stats.DocStatusStats docStatusStats = new IndexingStats.Stats.DocStatusStats();
        for (int i = 1; i < 6; ++i) {
            docStatusStats.add(RestStatus.fromCode(i * 100), randomNonNegativeLong());
        }

        IndexingStats.Stats stats = new IndexingStats.Stats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            randomNonNegativeLong(),
            docStatusStats,
            randomNonNegativeLong()
        );

        return new IndexingStats(stats);
    }

}

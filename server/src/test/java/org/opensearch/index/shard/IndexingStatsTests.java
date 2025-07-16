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
            + "},\"max_last_index_request_timestamp\":"
            + totalStats.getMaxLastIndexRequestTimestamp()
            + "}}";

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
        // Use explicit values for all fields except the timestamp
        IndexingStats.Stats.DocStatusStats docStatusStats = new IndexingStats.Stats.DocStatusStats();
        long ts1 = randomLongBetween(0, 1000000);
        long ts2 = randomLongBetween(0, 1000000);
        long ts3 = randomLongBetween(0, 1000000);
        IndexingStats.Stats stats1 = new IndexingStats.Stats(1, 2, 3, 4, 5, 6, 7, 8, false, 9, docStatusStats, ts1);
        IndexingStats.Stats stats2 = new IndexingStats.Stats(1, 2, 3, 4, 5, 6, 7, 8, false, 9, docStatusStats, ts2);
        IndexingStats.Stats stats3 = new IndexingStats.Stats(1, 2, 3, 4, 5, 6, 7, 8, false, 9, docStatusStats, ts3);

        // Aggregate stats1 + stats2
        stats1.add(stats2);
        assertEquals(Math.max(ts1, ts2), stats1.getMaxLastIndexRequestTimestamp());

        // Aggregate stats1 + stats3
        stats1.add(stats3);
        assertEquals(Math.max(Math.max(ts1, ts2), ts3), stats1.getMaxLastIndexRequestTimestamp());

        // Test with zero and negative values
        IndexingStats.Stats statsZero = new IndexingStats.Stats(1, 2, 3, 4, 5, 6, 7, 8, false, 9, docStatusStats, 0L);
        IndexingStats.Stats statsNeg = new IndexingStats.Stats(1, 2, 3, 4, 5, 6, 7, 8, false, 9, docStatusStats, -100L);
        statsZero.add(statsNeg);
        assertEquals(0L, statsZero.getMaxLastIndexRequestTimestamp());

        IndexingStats.Stats statsNeg2 = new IndexingStats.Stats(1, 2, 3, 4, 5, 6, 7, 8, false, 9, docStatusStats, -50L);
        statsNeg.add(statsNeg2);
        assertEquals(-50L, statsNeg.getMaxLastIndexRequestTimestamp());
    }

    public void testMaxLastIndexRequestTimestampBackwardCompatibility() throws IOException {
        IndexingStats.Stats.DocStatusStats docStatusStats = new IndexingStats.Stats.DocStatusStats();
        long ts = randomLongBetween(0, 1000000);
        IndexingStats.Stats stats = new IndexingStats.Stats(1, 2, 3, 4, 5, 6, 7, 8, false, 9, docStatusStats, ts);

        // Serialize with V_3_1_0 (should include the field)
        BytesStreamOutput outNew = new BytesStreamOutput();
        outNew.setVersion(org.opensearch.Version.V_3_2_0);
        stats.writeTo(outNew);
        StreamInput inNew = outNew.bytes().streamInput();
        inNew.setVersion(org.opensearch.Version.V_3_2_0);
        IndexingStats.Stats deserializedNew = new IndexingStats.Stats(inNew);
        assertEquals(ts, deserializedNew.getMaxLastIndexRequestTimestamp());

        // Serialize with V_2_11_0 (should NOT include the field, should default to 0)
        BytesStreamOutput outOld = new BytesStreamOutput();
        outOld.setVersion(org.opensearch.Version.V_2_11_0);
        stats.writeTo(outOld);
        StreamInput inOld = outOld.bytes().streamInput();
        inOld.setVersion(org.opensearch.Version.V_2_11_0);
        IndexingStats.Stats deserializedOld = new IndexingStats.Stats(inOld);
        assertEquals(0L, deserializedOld.getMaxLastIndexRequestTimestamp());
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
            randomLong()
        );

        return new IndexingStats(stats);
    }

}

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
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

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
            }
        }
    }

    public void testToXContentForIndexingStats() throws IOException {
        IndexingStats stats = createTestInstance();
        IndexingStats.Stats totalStats = stats.getTotal();

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
            + ",\"max_last_index_request_timestamp\":"
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
        long ts1 = randomLongBetween(0, 1000000);
        long ts2 = randomLongBetween(0, 1000000);
        long ts3 = randomLongBetween(0, 1000000);

        IndexingStats.Stats.Builder defaultStats = new IndexingStats.Stats.Builder().indexCount(1)
            .indexTimeInMillis(2)
            .indexCurrent(3)
            .indexFailedCount(4)
            .deleteCount(5)
            .deleteTimeInMillis(6)
            .deleteCurrent(7)
            .noopUpdateCount(8)
            .isThrottled(false)
            .throttleTimeInMillis(9);
        IndexingStats.Stats stats1 = defaultStats.maxLastIndexRequestTimestamp(ts1).build();
        IndexingStats.Stats stats2 = defaultStats.maxLastIndexRequestTimestamp(ts2).build();
        IndexingStats.Stats stats3 = defaultStats.maxLastIndexRequestTimestamp(ts3).build();

        // Aggregate stats1 + stats2
        stats1.add(stats2);
        assertEquals(Math.max(ts1, ts2), stats1.getMaxLastIndexRequestTimestamp());

        // Aggregate stats1 + stats3
        stats1.add(stats3);
        assertEquals(Math.max(Math.max(ts1, ts2), ts3), stats1.getMaxLastIndexRequestTimestamp());

        // Test with zero and negative values
        IndexingStats.Stats statsZero = defaultStats.maxLastIndexRequestTimestamp(0L).build();
        IndexingStats.Stats statsNeg = defaultStats.maxLastIndexRequestTimestamp(-100L).build();

        statsZero.add(statsNeg);
        assertEquals(0L, statsZero.getMaxLastIndexRequestTimestamp());

        IndexingStats.Stats statsNeg2 = defaultStats.maxLastIndexRequestTimestamp(-50L).build();
        statsNeg.add(statsNeg2);
        assertEquals(-50L, statsNeg.getMaxLastIndexRequestTimestamp());
    }

    public void testMaxLastIndexRequestTimestampBackwardCompatibility() throws IOException {
        long ts = randomLongBetween(0, 1000000);
        IndexingStats.Stats stats = new IndexingStats.Stats.Builder().indexCount(1)
            .indexTimeInMillis(2)
            .indexCurrent(3)
            .indexFailedCount(4)
            .deleteCount(5)
            .deleteTimeInMillis(6)
            .deleteCurrent(7)
            .noopUpdateCount(8)
            .isThrottled(false)
            .throttleTimeInMillis(9)
            .maxLastIndexRequestTimestamp(ts)
            .build();

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
        IndexingStats.Stats stats = new IndexingStats.Stats.Builder().indexCount(randomNonNegativeLong())
            .indexTimeInMillis(randomNonNegativeLong())
            .indexCurrent(randomNonNegativeLong())
            .indexFailedCount(randomNonNegativeLong())
            .deleteCount(randomNonNegativeLong())
            .deleteTimeInMillis(randomNonNegativeLong())
            .deleteCurrent(randomNonNegativeLong())
            .noopUpdateCount(randomNonNegativeLong())
            .isThrottled(randomBoolean())
            .throttleTimeInMillis(randomNonNegativeLong())
            .maxLastIndexRequestTimestamp(randomLong())
            .build();

        return new IndexingStats(stats);
    }

}

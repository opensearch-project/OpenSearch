/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Unit tests for {@link SearchStats}.
 *
 * <p>Covers transport serialization round-trip, JSON rendering shape, and
 * equality semantics.
 */
public class SearchStatsTests extends OpenSearchTestCase {

    /** Build a SearchStats with sequential values 1..30 for deterministic field verification. */
    private static SearchStats sequentialStats() {
        return new SearchStats(
            1,
            2, 3, 4, 5, 6, 7, 8, 9, 10,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30
        );
    }

    public void testFieldsArePopulated() {
        SearchStats s = sequentialStats();
        assertEquals(1L, s.queriesCompleted);
        assertEquals(2L, s.outputRows);
        assertEquals(3L, s.rowsMatched);
        assertEquals(4L, s.rowsPrunedByPageIndex);
        assertEquals(5L, s.rowGroupsProcessed);
        assertEquals(6L, s.rowGroupsSkipped);
        assertEquals(10L, s.ffmCollectorCalls);
        assertEquals(20L, s.elapsedComputeMs);
        assertEquals(22L, s.parquetTimeMs);
        assertEquals(30L, s.parquetPollTimeMs);
    }

    public void testWriteableRoundTrip() throws IOException {
        SearchStats original = sequentialStats();
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        SearchStats decoded = new SearchStats(in);
        assertEquals(original, decoded);
    }

    public void testToXContentShape() throws IOException {
        SearchStats s = sequentialStats();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        s.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        assertTrue(json.contains("\"search_stats\""));
        assertTrue(json.contains("\"queries_completed\":1"));
        assertTrue(json.contains("\"output_rows\":2"));
        assertTrue(json.contains("\"rows_matched\":3"));
        assertTrue(json.contains("\"row_groups_processed\":5"));
        assertTrue(json.contains("\"ffm_collector_calls\":10"));
        assertTrue(json.contains("\"elapsed_compute_ms\":20"));
        assertTrue(json.contains("\"parquet_time_ms\":22"));
        assertTrue(json.contains("\"parquet_poll_time_ms\":30"));
    }

    public void testToXContentRendersAllThirtyFields() throws IOException {
        SearchStats s = sequentialStats();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        s.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        String[] keys = {
            "queries_completed",
            "output_rows",
            "rows_matched",
            "rows_pruned_by_page_index",
            "row_groups_processed",
            "row_groups_skipped",
            "pages_pruned",
            "pages_total",
            "page_pruning_unavailable",
            "ffm_collector_calls",
            "batches_produced",
            "parquet_batches_received",
            "position_map_identity",
            "position_map_bitmap",
            "position_map_runs",
            "min_skip_run_row_granular",
            "min_skip_run_block_granular",
            "prefetch_wait_count",
            "batches_pre_coalesce",
            "elapsed_compute_ms",
            "index_time_ms",
            "parquet_time_ms",
            "prefetch_wait_time_ms",
            "coalesce_time_ms",
            "build_mask_time_ms",
            "filter_record_batch_time_ms",
            "on_batch_mask_time_ms",
            "mask_slice_time_ms",
            "projection_fixup_time_ms",
            "parquet_poll_time_ms" };
        for (String k : keys) {
            assertTrue("expected JSON to contain field: " + k, json.contains("\"" + k + "\""));
        }
    }

    public void testEqualsAndHashCode() {
        SearchStats a = sequentialStats();
        SearchStats b = sequentialStats();
        SearchStats c = new SearchStats(
            999,
            2, 3, 4, 5, 6, 7, 8, 9, 10,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30
        );

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    public void testZeroValuesRoundTrip() throws IOException {
        SearchStats zero = new SearchStats(
            0,
            0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0
        );
        BytesStreamOutput out = new BytesStreamOutput();
        zero.writeTo(out);
        SearchStats decoded = new SearchStats(out.bytes().streamInput());
        assertEquals(zero, decoded);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.analytics.spi.ShardSortBounds;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Wire round-trip coverage for the can-match request/response, focused on the optional
 * sort-bounds fields: they must survive serialization intact and, when absent, must not
 * perturb the pre-existing boolean-only shape.
 */
public class AnalyticsCanMatchWireTests extends OpenSearchTestCase {

    public void testRequestRoundTripWithSortColumn() throws Exception {
        AnalyticsCanMatchRequest original = new AnalyticsCanMatchRequest(
            new ShardId(new Index("logs", "_na_"), 3),
            new byte[] { 7, 8, 9 },
            "datafusion",
            "@timestamp"
        );

        AnalyticsCanMatchRequest copy = roundTrip(original);

        assertEquals(original.getShardId(), copy.getShardId());
        assertArrayEquals(original.getFilterBytes(), copy.getFilterBytes());
        assertEquals("datafusion", copy.getBackendId());
        assertEquals("@timestamp", copy.getSortColumn());
    }

    public void testRequestRoundTripWithoutSortColumn() throws Exception {
        AnalyticsCanMatchRequest original = new AnalyticsCanMatchRequest(
            new ShardId(new Index("logs", "_na_"), 0),
            new byte[] { 1 },
            "datafusion"
        );

        AnalyticsCanMatchRequest copy = roundTrip(original);

        assertNull("absent sort column must stay absent", copy.getSortColumn());
        assertEquals("datafusion", copy.getBackendId());
    }

    public void testResponseRoundTripWithBounds() throws Exception {
        ShardSortBounds bounds = new ShardSortBounds(100L, 5000L, ShardSortBounds.VALUE_KIND_INT64);
        AnalyticsCanMatchResponse copy = roundTrip(new AnalyticsCanMatchResponse(true, bounds));

        assertTrue(copy.canMatch());
        assertNotNull(copy.bounds());
        assertEquals(100L, copy.bounds().min());
        assertEquals(5000L, copy.bounds().max());
        assertEquals(ShardSortBounds.VALUE_KIND_INT64, copy.bounds().valueKind());
    }

    public void testResponseRoundTripWithoutBounds() throws Exception {
        AnalyticsCanMatchResponse copy = roundTrip(new AnalyticsCanMatchResponse(false));

        assertFalse(copy.canMatch());
        assertNull("bounds must be optional on the wire", copy.bounds());
    }

    /** The single-arg constructor is the pre-existing shape and must keep meaning "no bounds". */
    public void testResponseBooleanOnlyConstructorHasNoBounds() {
        assertNull(new AnalyticsCanMatchResponse(true).bounds());
    }

    public void testBoundsRoundTripPreservesNegativeAndExtremeValues() throws Exception {
        ShardSortBounds bounds = new ShardSortBounds(Long.MIN_VALUE, Long.MAX_VALUE, ShardSortBounds.VALUE_KIND_INT32);
        AnalyticsCanMatchResponse copy = roundTrip(new AnalyticsCanMatchResponse(true, bounds));

        assertEquals(Long.MIN_VALUE, copy.bounds().min());
        assertEquals(Long.MAX_VALUE, copy.bounds().max());
        assertEquals(ShardSortBounds.VALUE_KIND_INT32, copy.bounds().valueKind());
    }

    /**
     * A shard that matches but has no usable statistics for the sort column (e.g. a
     * {@code keyword} column) is a normal outcome and must serialize cleanly — the coordinator
     * reads it as "no hint" and defers the shard.
     */
    public void testMatchingShardWithoutBoundsRoundTrips() throws Exception {
        AnalyticsCanMatchResponse copy = roundTrip(new AnalyticsCanMatchResponse(true, null));

        assertTrue(copy.canMatch());
        assertNull(copy.bounds());
    }

    private static AnalyticsCanMatchRequest roundTrip(AnalyticsCanMatchRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new AnalyticsCanMatchRequest(in);
            }
        }
    }

    private static AnalyticsCanMatchResponse roundTrip(AnalyticsCanMatchResponse response) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new AnalyticsCanMatchResponse(in);
            }
        }
    }
}

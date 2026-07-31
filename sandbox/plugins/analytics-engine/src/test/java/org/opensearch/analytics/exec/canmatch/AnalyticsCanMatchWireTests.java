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
 * Wire round-trip coverage for the can-match request/response, focused on the optional sort-bounds
 * fields: they must survive serialization intact and, when absent, leave the boolean-only shape unchanged.
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
        ShardSortBounds bounds = new ShardSortBounds(100L, 5000L, false, ShardSortBounds.VALUE_KIND_INT64);
        AnalyticsCanMatchResponse copy = roundTrip(new AnalyticsCanMatchResponse(true, bounds));

        assertTrue(copy.canMatch());
        assertNotNull(copy.bounds());
        assertEquals(100L, copy.bounds().min());
        assertEquals(5000L, copy.bounds().max());
        assertFalse(copy.bounds().hasNulls());
        assertEquals(ShardSortBounds.VALUE_KIND_INT64, copy.bounds().valueKind());
    }

    /**
     * Every value kind and {@code hasNulls} must round-trip: the coordinator compares kinds for
     * equality, and a dropped {@code hasNulls=true} would let it eliminate a shard holding a top null.
     */
    public void testBoundsRoundTripPreservesEveryValueKindAndHasNulls() throws Exception {
        for (byte kind : new byte[] {
            ShardSortBounds.VALUE_KIND_INT32,
            ShardSortBounds.VALUE_KIND_INT64,
            ShardSortBounds.VALUE_KIND_INT64_MILLIS,
            ShardSortBounds.VALUE_KIND_INT64_MICROS,
            ShardSortBounds.VALUE_KIND_INT64_NANOS }) {
            for (boolean hasNulls : new boolean[] { false, true }) {
                ShardSortBounds bounds = new ShardSortBounds(0L, 1L, hasNulls, kind);
                AnalyticsCanMatchResponse copy = roundTrip(new AnalyticsCanMatchResponse(true, bounds));

                assertEquals("value kind " + kind + " must round-trip", kind, copy.bounds().valueKind());
                assertEquals("hasNulls must survive the wire", hasNulls, copy.bounds().hasNulls());
            }
        }
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
        ShardSortBounds bounds = new ShardSortBounds(Long.MIN_VALUE, Long.MAX_VALUE, false, ShardSortBounds.VALUE_KIND_INT32);
        AnalyticsCanMatchResponse copy = roundTrip(new AnalyticsCanMatchResponse(true, bounds));

        assertEquals(Long.MIN_VALUE, copy.bounds().min());
        assertEquals(Long.MAX_VALUE, copy.bounds().max());
        assertEquals(ShardSortBounds.VALUE_KIND_INT32, copy.bounds().valueKind());
    }

    /** A matching shard with no statistics (e.g. a {@code keyword} sort) must serialize cleanly as no-bounds. */
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

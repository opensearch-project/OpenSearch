/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RangeResponseStrategyTests extends OpenSearchTestCase {

    private static RangeAggregationBuilder keyedRanges() {
        return new RangeAggregationBuilder("price_ranges").field("price")
            .addUnboundedTo("cheap", 100)
            .addRange("mid", 100, 200)
            .addUnboundedFrom("expensive", 200);
    }

    @SuppressWarnings("unchecked")
    private static List<? extends InternalRange.Bucket> bucketsOf(InternalAggregation agg) {
        assertTrue("expected InternalRange, got " + agg.getClass().getSimpleName(), agg instanceof InternalRange);
        return ((InternalRange<InternalRange.Bucket, ?>) agg).getBuckets();
    }

    public void testAllRangesMaterializedInOrderWithEmptyAsZero() {
        // rows only for ordinals 0 and 2 (Integer and Long, to prove numeric key coercion);
        // ordinal 1 has no row and must still appear with doc_count 0.
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of(0), 5L, InternalAggregations.EMPTY),
            new BucketEntry(List.of(2L), 3L, InternalAggregations.EMPTY)
        );

        InternalAggregation agg = RangeResponseStrategy.build(keyedRanges(), entries, DocValueFormat.RAW);
        assertEquals("price_ranges", agg.getName());

        List<? extends InternalRange.Bucket> buckets = bucketsOf(agg);
        assertEquals(3, buckets.size());

        assertEquals("cheap", buckets.get(0).getKeyAsString());
        assertEquals(5L, buckets.get(0).getDocCount());
        assertEquals("mid", buckets.get(1).getKeyAsString());
        assertEquals(0L, buckets.get(1).getDocCount()); // empty range still present
        assertEquals("expensive", buckets.get(2).getKeyAsString());
        assertEquals(3L, buckets.get(2).getDocCount());
    }

    public void testFromToRenderingIncludingInfinities() {
        InternalAggregation agg = RangeResponseStrategy.build(keyedRanges(), List.of(), DocValueFormat.RAW);
        List<? extends InternalRange.Bucket> buckets = bucketsOf(agg);

        // [*,100)
        assertEquals(Double.NEGATIVE_INFINITY, ((Number) buckets.get(0).getFrom()).doubleValue(), 0.0);
        assertEquals(100.0, ((Number) buckets.get(0).getTo()).doubleValue(), 0.0);
        assertNull("open lower bound renders as null string", buckets.get(0).getFromAsString());
        assertEquals("100.0", buckets.get(0).getToAsString());
        // [200,*)
        assertEquals(200.0, ((Number) buckets.get(2).getFrom()).doubleValue(), 0.0);
        assertEquals(Double.POSITIVE_INFINITY, ((Number) buckets.get(2).getTo()).doubleValue(), 0.0);
        assertNull("open upper bound renders as null string", buckets.get(2).getToAsString());
    }

    public void testEmptyResultYieldsAllRangesAtZero() {
        InternalAggregation agg = RangeResponseStrategy.build(keyedRanges(), List.of(), DocValueFormat.RAW);
        List<? extends InternalRange.Bucket> buckets = bucketsOf(agg);
        assertEquals(3, buckets.size());
        for (InternalRange.Bucket b : buckets) {
            assertEquals(0L, b.getDocCount());
        }
    }

    public void testNullOrdinalGroupIsIgnored() {
        // documents that matched no range form a null-ordinal group — it is not a bucket
        List<BucketEntry> entries = Arrays.asList(
            new BucketEntry(List.of(1), 7L, InternalAggregations.EMPTY),
            new BucketEntry(Collections.singletonList(null), 99L, InternalAggregations.EMPTY)
        );
        InternalAggregation agg = RangeResponseStrategy.build(keyedRanges(), entries, DocValueFormat.RAW);
        List<? extends InternalRange.Bucket> buckets = bucketsOf(agg);
        assertEquals(3, buckets.size());
        assertEquals(0L, buckets.get(0).getDocCount());
        assertEquals(7L, buckets.get(1).getDocCount());
        assertEquals(0L, buckets.get(2).getDocCount());
        long total = buckets.stream().mapToLong(InternalRange.Bucket::getDocCount).sum();
        assertEquals("the 99 null-ordinal docs must not leak into any bucket", 7L, total);
    }

    public void testKeyedFlagPropagated() {
        RangeAggregationBuilder keyed = keyedRanges();
        keyed.keyed(true);
        List<? extends InternalRange.Bucket> buckets = bucketsOf(RangeResponseStrategy.build(keyed, List.of(), DocValueFormat.RAW));
        assertTrue(buckets.get(0).getKeyed());
    }

    public void testMetaEchoedWhenSupplied() {
        RangeAggregationBuilder agg = keyedRanges();
        agg.setMetadata(Map.of("owner", "search"));
        InternalAggregation result = RangeResponseStrategy.build(agg, List.of(), DocValueFormat.RAW);
        assertEquals(Map.of("owner", "search"), result.getMetadata());
    }
}

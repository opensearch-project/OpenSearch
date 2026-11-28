/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a {@code range} aggregation response ({@link InternalRange}) from grouped bucket
 * entries. Unlike terms, {@code range} returns its <b>full</b> bucket set in declaration order:
 * every requested range appears, including ranges the plan produced no rows for (rendered with
 * {@code doc_count = 0}).
 *
 * <p>Each {@link BucketEntry} carries the range <b>ordinal</b> as its single key — the value the
 * group expression assigned to the documents that fell in that range (ordinal {@code i} =
 * {@code agg.ranges().get(i)}). Documents that fell in no range (or whose field was null) form a
 * null-ordinal group that is not a bucket, so it is ignored here.
 */
final class RangeResponseStrategy {

    private RangeResponseStrategy() {}

    /**
     * Builds the {@link InternalRange} response.
     *
     * @param agg     the original range aggregation builder (name, ranges, keyed, meta)
     * @param entries grouped bucket entries keyed by range ordinal, as the plan produced them
     * @param format  the resolved {@link DocValueFormat} for key/from/to rendering
     * @return the range aggregation, all declared ranges present in declaration order
     */
    static InternalAggregation build(RangeAggregationBuilder agg, Iterable<BucketEntry> entries, DocValueFormat format) {
        Map<Integer, BucketEntry> byOrdinal = new HashMap<>();
        for (BucketEntry entry : entries) {
            Object key = entry.keys().isEmpty() ? null : entry.keys().get(0);
            if (key instanceof Number number) {
                byOrdinal.put(number.intValue(), entry);
            }
            // A null / non-numeric key is the "matched no range" group — not a bucket.
        }

        List<RangeAggregator.Range> ranges = agg.ranges();
        boolean keyed = agg.keyed();
        List<InternalRange.Bucket> buckets = new ArrayList<>(ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            RangeAggregator.Range range = ranges.get(i);
            BucketEntry entry = byOrdinal.get(i);
            long docCount = entry != null ? entry.docCount() : 0L;
            InternalAggregations subAggs = entry != null ? entry.subAggs() : InternalAggregations.EMPTY;
            buckets.add(new InternalRange.Bucket(range.getKey(), range.getFrom(), range.getTo(), docCount, subAggs, keyed, format));
        }
        return newRange(agg.getName(), buckets, format, keyed, AggregationTranslator.userMetadata(agg));
    }

    /**
     * Constructs the {@link InternalRange} via a raw {@link InternalRange.Factory} instance. The
     * factory's recursively-bounded generics ({@code R extends InternalRange<B, R>}) cannot be
     * satisfied by a diamond or wildcard at a call site, so a raw factory is the idiomatic
     * construction path (the same one {@code RangeAggregator} uses); the package-private
     * {@code InternalRange.FACTORY} singleton is not visible here, so we instantiate our own.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static InternalAggregation newRange(
        String name,
        List<InternalRange.Bucket> buckets,
        DocValueFormat format,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        return new InternalRange.Factory().create(name, buckets, format, keyed, metadata);
    }
}

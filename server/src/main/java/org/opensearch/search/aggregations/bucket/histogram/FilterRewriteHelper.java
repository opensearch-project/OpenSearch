/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.query.DateRangeIncludingNowQuery;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.time.ZoneId;
import java.time.format.TextStyle;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Helpers functions to rewrite and optimize aggregations using
 * range filter queries
 *
 * @opensearch.internal
 */
public class FilterRewriteHelper {
    private static final int MAX_NUM_FILTER_BUCKETS = 1024;
    private static final Map<Class, Function<Query, Query>> queryWrappers;

    // Initialize the wrappers map for unwrapping the query
    static {
        queryWrappers = new HashMap<>();
        queryWrappers.put(ConstantScoreQuery.class, q -> ((ConstantScoreQuery) q).getQuery());
        queryWrappers.put(FunctionScoreQuery.class, q -> ((FunctionScoreQuery) q).getSubQuery());
        queryWrappers.put(DateRangeIncludingNowQuery.class, q -> ((DateRangeIncludingNowQuery) q).getQuery());
        queryWrappers.put(IndexOrDocValuesQuery.class, q -> ((IndexOrDocValuesQuery) q).getIndexQuery());
    }

    /**
     * Recursively unwraps query into the concrete form
     * for applying the optimization
     */
    public static Query unwrapIntoConcreteQuery(Query query) {
        while (queryWrappers.containsKey(query.getClass())) {
            query = queryWrappers.get(query.getClass()).apply(query);
        }

        return query;
    }

    /**
     * Finds the min and max bounds for segments within the passed search context
     */
    private static long[] getIndexBoundsFromLeaves(final SearchContext context, final String fieldName) throws IOException {
        final List<LeafReaderContext> leaves = context.searcher().getIndexReader().leaves();
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        // Since the query does not specify bounds for aggregation, we can
        // build the global min/max from local min/max within each segment
        for (LeafReaderContext leaf : leaves) {
            final PointValues values = leaf.reader().getPointValues(fieldName);
            if (values != null) {
                min = Math.min(min, NumericUtils.sortableBytesToLong(values.getMinPackedValue(), 0));
                max = Math.max(max, NumericUtils.sortableBytesToLong(values.getMaxPackedValue(), 0));
            }
        }

        if (min == Long.MAX_VALUE || max == Long.MIN_VALUE) return null;

        return new long[] { min, max };
    }

    public static long[] getAggregationBounds(final SearchContext context, final String fieldName) throws IOException {
        final Query cq = unwrapIntoConcreteQuery(context.query());
        final long[] indexBounds = getIndexBoundsFromLeaves(context, fieldName);
        if (cq instanceof PointRangeQuery) {
            final PointRangeQuery prq = (PointRangeQuery) cq;
            // Ensure that the query and aggregation are on the same field
            if (prq.getField().equals(fieldName)) {
                return new long[] {
                    // Minimum bound for aggregation is the max between query and global
                    Math.max(NumericUtils.sortableBytesToLong(prq.getLowerPoint(), 0), indexBounds[0]),
                    // Maximum bound for aggregation is the min between query and global
                    Math.min(NumericUtils.sortableBytesToLong(prq.getUpperPoint(), 0), indexBounds[1]) };
            }
        } else if (cq instanceof MatchAllDocsQuery) {
            return indexBounds;
        }

        return null;
    }

    /**
     * Helper function for checking if the time zone requested for date histogram
     * aggregation is utc or not
     */
    private static boolean isUTCTimeZone(final ZoneId zoneId) {
        return "Z".equals(zoneId.getDisplayName(TextStyle.FULL, Locale.ENGLISH));
    }

    /**
     * Creates the range query filters for aggregations using the interval, min/max
     * bounds and the rounding values
     */
    public static Weight[] createFilterForAggregations(
        final SearchContext context,
        final Rounding rounding,
        final Rounding.Prepared preparedRounding,
        final String field,
        final DateFieldMapper.DateFieldType fieldType,
        final long low,
        final long high
    ) throws IOException {
        long interval;
        if (rounding instanceof Rounding.TimeUnitRounding) {
            interval = (((Rounding.TimeUnitRounding) rounding).getUnit()).extraLocalOffsetLookup();
            if (!isUTCTimeZone(((Rounding.TimeUnitRounding) rounding).getTimeZone())) {
                // Fast filter aggregation cannot be used if it needs time zone rounding
                return null;
            }
        } else if (rounding instanceof Rounding.TimeIntervalRounding) {
            interval = ((Rounding.TimeIntervalRounding) rounding).getInterval();
            if (!isUTCTimeZone(((Rounding.TimeIntervalRounding) rounding).getTimeZone())) {
                // Fast filter aggregation cannot be used if it needs time zone rounding
                return null;
            }
        } else {
            // Unexpected scenario, exit and fall back to original
            return null;
        }

        // Calculate the number of buckets using range and interval
        long roundedLow = preparedRounding.round(fieldType.convertNanosToMillis(low));
        long prevRounded = roundedLow;
        int bucketCount = 0;
        while (roundedLow <= fieldType.convertNanosToMillis(high)) {
            bucketCount++;
            // Below rounding is needed as the interval could return in
            // non-rounded values for something like calendar month
            roundedLow = preparedRounding.round(roundedLow + interval);
            if (prevRounded == roundedLow) break;
            prevRounded = roundedLow;
        }

        Weight[] filters = null;
        if (bucketCount > 0 && bucketCount <= MAX_NUM_FILTER_BUCKETS) {
            int i = 0;
            filters = new Weight[bucketCount];
            roundedLow = preparedRounding.round(fieldType.convertNanosToMillis(low));
            while (i < bucketCount) {
                // Calculate the lower bucket bound
                final byte[] lower = new byte[8];
                NumericUtils.longToSortableBytes(i == 0 ? low : fieldType.convertRoundedMillisToNanos(roundedLow), lower, 0);
                // Calculate the upper bucket bound
                final byte[] upper = new byte[8];
                roundedLow = preparedRounding.round(roundedLow + interval);
                // Subtract -1 if the minimum is roundedLow as roundedLow itself
                // is included in the next bucket
                NumericUtils.longToSortableBytes(
                    i + 1 == bucketCount ? high : fieldType.convertRoundedMillisToNanos(roundedLow) - 1,
                    upper,
                    0
                );
                filters[i++] = context.searcher().createWeight(new PointRangeQuery(field, lower, upper, 1) {
                    @Override
                    protected String toString(int dimension, byte[] value) {
                        return null;
                    }
                }, ScoreMode.COMPLETE_NO_SCORES, 1);
            }
        }

        return filters;
    }
}

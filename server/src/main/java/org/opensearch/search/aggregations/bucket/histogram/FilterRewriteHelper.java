/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
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
     * and creates the weight filters using range queries within those bounds
     */
    public static void findBoundsAndCreateFilters(
        final SearchContext context,
        final Rounding rounding,
        final Rounding.Prepared preparedRounding,
        final String fieldName
    ) throws IOException {
        final List<LeafReaderContext> leaves = context.searcher().getIndexReader().leaves();
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        // Since the query does not specify bounds for aggregation, we can
        // build the global min/max from local min/max within each segment
        for (LeafReaderContext leaf : leaves) {
            min = Math.min(min, NumericUtils.sortableBytesToLong(leaf.reader().getPointValues(fieldName).getMinPackedValue(), 0));
            max = Math.max(max, NumericUtils.sortableBytesToLong(leaf.reader().getPointValues(fieldName).getMaxPackedValue(), 0));
        }
        createFilterForAggregations(context, rounding, preparedRounding, fieldName, min, max);
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
        final long low,
        final long high
    ) throws IOException {
        long interval;
        if (rounding instanceof Rounding.TimeUnitRounding) {
            interval = (((Rounding.TimeUnitRounding) rounding).unit).extraLocalOffsetLookup();
            if (!isUTCTimeZone(((Rounding.TimeUnitRounding) rounding).timeZone)) {
                // Fast filter aggregation cannot be used if it needs time zone rounding
                return null;
            }
        } else if (rounding instanceof Rounding.TimeIntervalRounding) {
            interval = ((Rounding.TimeIntervalRounding) rounding).interval;
            if (!isUTCTimeZone(((Rounding.TimeIntervalRounding) rounding).timeZone)) {
                // Fast filter aggregation cannot be used if it needs time zone rounding
                return null;
            }
        } else {
            // Unexpected scenario, exit and fall back to original
            return null;
        }

        // Calculate the number of buckets using range and interval
        long roundedLow = preparedRounding.round(low);
        int bucketCount = 0;
        while (roundedLow < high) {
            bucketCount++;
            // Below rounding is needed as the interval could return in
            // non-rounded values for something like calendar month
            roundedLow = preparedRounding.round(roundedLow + interval);
        }

        Weight[] filters = null;
        if (bucketCount > 0 && bucketCount <= MAX_NUM_FILTER_BUCKETS) {
            int i = 0;
            filters = new Weight[bucketCount];
            roundedLow = preparedRounding.round(low);
            while (i < bucketCount) {
                // Calculate the lower bucket bound
                final byte[] lower = new byte[8];
                NumericUtils.longToSortableBytes(Math.max(roundedLow, low), lower, 0);
                // Calculate the upper bucket bound
                final byte[] upper = new byte[8];
                roundedLow = preparedRounding.round(roundedLow + interval);
                // Subtract -1 if the minimum is roundedLow as roundedLow itself
                // is included in the next bucket
                NumericUtils.longToSortableBytes(Math.min(roundedLow - 1, high), upper, 0);
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

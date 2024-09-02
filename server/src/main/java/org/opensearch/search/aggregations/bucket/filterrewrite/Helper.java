/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.query.DateRangeIncludingNowQuery;
import org.opensearch.search.approximate.ApproximateIndexOrDocValuesQuery;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.LONG;

/**
 * Utility class to help range filters rewrite optimization
 *
 * @opensearch.internal
 */
final class Helper {

    private Helper() {}

    static final String loggerName = Helper.class.getPackageName();
    private static final Logger logger = LogManager.getLogger(loggerName);

    private static final Map<Class<?>, Function<Query, Query>> queryWrappers;

    // Initialize the wrapper map for unwrapping the query
    static {
        queryWrappers = new HashMap<>();
        queryWrappers.put(ConstantScoreQuery.class, q -> ((ConstantScoreQuery) q).getQuery());
        queryWrappers.put(FunctionScoreQuery.class, q -> ((FunctionScoreQuery) q).getSubQuery());
        queryWrappers.put(DateRangeIncludingNowQuery.class, q -> ((DateRangeIncludingNowQuery) q).getQuery());
        queryWrappers.put(IndexOrDocValuesQuery.class, q -> ((IndexOrDocValuesQuery) q).getIndexQuery());
        queryWrappers.put(ApproximateIndexOrDocValuesQuery.class, q -> ((ApproximateIndexOrDocValuesQuery) q).getOriginalQuery());
    }

    /**
     * Recursively unwraps query into the concrete form
     * for applying the optimization
     */
    private static Query unwrapIntoConcreteQuery(Query query) {
        while (queryWrappers.containsKey(query.getClass())) {
            query = queryWrappers.get(query.getClass()).apply(query);
        }

        return query;
    }

    /**
     * Finds the global min and max bounds of the field for the shard across all segments
     *
     * @return null if the field is empty or not indexed
     */
    private static long[] getShardBounds(final List<LeafReaderContext> leaves, final String fieldName) throws IOException {
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (LeafReaderContext leaf : leaves) {
            final PointValues values = leaf.reader().getPointValues(fieldName);
            if (values != null) {
                min = Math.min(min, NumericUtils.sortableBytesToLong(values.getMinPackedValue(), 0));
                max = Math.max(max, NumericUtils.sortableBytesToLong(values.getMaxPackedValue(), 0));
            }
        }

        if (min == Long.MAX_VALUE || max == Long.MIN_VALUE) {
            return null;
        }
        return new long[] { min, max };
    }

    /**
     * Finds the min and max bounds of the field for the segment
     *
     * @return null if the field is empty or not indexed
     */
    static long[] getSegmentBounds(final LeafReaderContext context, final String fieldName) throws IOException {
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        final PointValues values = context.reader().getPointValues(fieldName);
        if (values != null) {
            min = Math.min(min, NumericUtils.sortableBytesToLong(values.getMinPackedValue(), 0));
            max = Math.max(max, NumericUtils.sortableBytesToLong(values.getMaxPackedValue(), 0));
        }

        if (min == Long.MAX_VALUE || max == Long.MIN_VALUE) {
            return null;
        }
        return new long[] { min, max };
    }

    /**
     * Gets the min and max bounds of the field for the shard search
     * Depending on the query part, the bounds are computed differently
     *
     * @return null if the processed query not supported by the optimization
     */
    public static long[] getDateHistoAggBounds(final SearchContext context, final String fieldName) throws IOException {
        final Query cq = unwrapIntoConcreteQuery(context.query());
        final List<LeafReaderContext> leaves = context.searcher().getIndexReader().leaves();

        if (cq instanceof PointRangeQuery) {
            final PointRangeQuery prq = (PointRangeQuery) cq;
            final long[] indexBounds = getShardBounds(leaves, fieldName);
            if (indexBounds == null) return null;
            return getBoundsWithRangeQuery(prq, fieldName, indexBounds);
        } else if (cq instanceof MatchAllDocsQuery) {
            return getShardBounds(leaves, fieldName);
        } else if (cq instanceof FieldExistsQuery) {
            // when a range query covers all values of a shard, it will be rewrite field exists query
            if (((FieldExistsQuery) cq).getField().equals(fieldName)) {
                return getShardBounds(leaves, fieldName);
            }
        }

        return null;
    }

    private static long[] getBoundsWithRangeQuery(PointRangeQuery prq, String fieldName, long[] indexBounds) {
        // Ensure that the query and aggregation are on the same field
        if (prq.getField().equals(fieldName)) {
            // Minimum bound for aggregation is the max between query and global
            long lower = Math.max(NumericUtils.sortableBytesToLong(prq.getLowerPoint(), 0), indexBounds[0]);
            // Maximum bound for aggregation is the min between query and global
            long upper = Math.min(NumericUtils.sortableBytesToLong(prq.getUpperPoint(), 0), indexBounds[1]);
            if (lower > upper) {
                return null;
            }
            return new long[] { lower, upper };
        }

        return null;
    }

    /**
     * Creates the date ranges from date histo aggregations using its interval,
     * and min/max boundaries
     */
    static Ranges createRangesFromAgg(
        final DateFieldMapper.DateFieldType fieldType,
        final long interval,
        final Rounding.Prepared preparedRounding,
        long low,
        final long high,
        final int maxAggRewriteFilters
    ) {
        // Calculate the number of buckets using range and interval
        long roundedLow = preparedRounding.round(fieldType.convertNanosToMillis(low));
        long prevRounded = roundedLow;
        int bucketCount = 0;
        while (roundedLow <= fieldType.convertNanosToMillis(high)) {
            bucketCount++;
            if (bucketCount > maxAggRewriteFilters) {
                logger.debug("Max number of range filters reached [{}], skip the optimization", maxAggRewriteFilters);
                return null;
            }
            // Below rounding is needed as the interval could return in
            // non-rounded values for something like calendar month
            roundedLow = preparedRounding.round(roundedLow + interval);
            if (prevRounded == roundedLow) break; // prevents getting into an infinite loop
            prevRounded = roundedLow;
        }

        long[][] ranges = new long[bucketCount][2];
        if (bucketCount > 0) {
            roundedLow = preparedRounding.round(fieldType.convertNanosToMillis(low));

            int i = 0;
            while (i < bucketCount) {
                // Calculate the lower bucket bound
                long lower = i == 0 ? low : fieldType.convertRoundedMillisToNanos(roundedLow);
                roundedLow = preparedRounding.round(roundedLow + interval);

                // plus one on high value because upper bound is exclusive, but high value exists
                long upper = i + 1 == bucketCount ? high + 1 : fieldType.convertRoundedMillisToNanos(roundedLow);

                ranges[i][0] = lower;
                ranges[i][1] = upper;
                i++;
            }
        }

        byte[][] lowers = new byte[ranges.length][];
        byte[][] uppers = new byte[ranges.length][];
        for (int i = 0; i < ranges.length; i++) {
            byte[] lower = LONG.encodePoint(ranges[i][0]);
            byte[] max = LONG.encodePoint(ranges[i][1]);
            lowers[i] = lower;
            uppers[i] = max;
        }

        return new Ranges(lowers, uppers);
    }
}

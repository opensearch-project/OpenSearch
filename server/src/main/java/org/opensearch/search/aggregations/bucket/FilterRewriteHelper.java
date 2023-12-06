/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket;

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
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.DateRangeIncludingNowQuery;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Help rewrite and optimize aggregations using range filter queries
 * Currently supported types of aggregations are: DateHistogramAggregator, AutoDateHistogramAggregator, CompositeAggregator
 *
 * @opensearch.internal
 */
public class FilterRewriteHelper {

    public static class FilterContext {
        public final DateFieldMapper.DateFieldType fieldType;
        public final Weight[] filters;

        public FilterContext(DateFieldMapper.DateFieldType fieldType, Weight[] filters) {
            this.fieldType = fieldType;
            this.filters = filters;
        }
    }

    private static final int MAX_NUM_FILTER_BUCKETS = 1024;
    private static final Map<Class<?>, Function<Query, Query>> queryWrappers;

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
    private static Query unwrapIntoConcreteQuery(Query query) {
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
     * Creates the range query filters for aggregations using the interval, min/max
     * bounds and the rounding values
     */
    private static Weight[] createFilterForAggregations(
        final SearchContext context,
        final Rounding rounding,
        final Rounding.Prepared preparedRounding,
        final String field,
        final DateFieldMapper.DateFieldType fieldType,
        final long low,
        final long high
    ) throws IOException {
        final OptionalLong intervalOpt = Rounding.getInterval(rounding);
        if (intervalOpt.isEmpty()) {
            return null;
        }

        final long interval = intervalOpt.getAsLong();
        // Calculate the number of buckets using range and interval
        long roundedLow = preparedRounding.round(fieldType.convertNanosToMillis(low));
        long prevRounded = roundedLow;
        int bucketCount = 0;
        while (roundedLow <= fieldType.convertNanosToMillis(high)) {
            bucketCount++;
            // Below rounding is needed as the interval could return in
            // non-rounded values for something like calendar month
            roundedLow = preparedRounding.round(roundedLow + interval);
            if (prevRounded == roundedLow) break; // TODO reading prevents getting into an infinite loop?
            prevRounded = roundedLow;
        }

        Weight[] filters = null;
        if (bucketCount > 0 && bucketCount <= MAX_NUM_FILTER_BUCKETS) {
            filters = new Weight[bucketCount];
            roundedLow = preparedRounding.round(fieldType.convertNanosToMillis(low));

            int i = 0;
            while (i < bucketCount) {
                // Calculate the lower bucket bound
                final byte[] lower = new byte[8];
                NumericUtils.longToSortableBytes(
                    i == 0 ? low : fieldType.convertRoundedMillisToNanos(roundedLow),
                    lower, 0
                );

                // Calculate the upper bucket bound
                roundedLow = preparedRounding.round(roundedLow + interval);
                final byte[] upper = new byte[8];
                NumericUtils.longToSortableBytes(
                    i + 1 == bucketCount ? high :
                        // Subtract -1 if the minimum is roundedLow as roundedLow itself
                        // is included in the next bucket
                        fieldType.convertRoundedMillisToNanos(roundedLow) - 1,
                    upper,
                    0
                );

                filters[i++] = context.searcher().createWeight(
                    new PointRangeQuery(field, lower, upper, 1) {
                        @Override
                        protected String toString(int dimension, byte[] value) {
                            return null;
                        }
                    }, ScoreMode.COMPLETE_NO_SCORES, 1);
            }
        }

        return filters;
    }

    /**
     * The pre-conditions to initiate fast filter optimization on aggregations are:
     * 1. The query with aggregation has to be PointRangeQuery on the same date field
     * 2. No parent/sub aggregations
     * 3. No missing value/bucket
     * 4. No script
     *
     * @param computeBounds get the lower and upper bound of the field in a shard search
     * @param roundingFunction produce Rounding that will provide the interval
     * @param preparedRoundingSupplier produce PreparedRounding that will do the rounding
     */
    public static FilterContext buildFastFilterContext(
        final Object parent,
        final int subAggLength,
        SearchContext context,
        Function<long[], Rounding> roundingFunction,
        Supplier<Rounding.Prepared> preparedRoundingSupplier,
        ValueSourceContext valueSourceContext,
        CheckedFunction<ValueSourceContext, long[], IOException> computeBounds
    ) throws IOException {
        if (parent == null && subAggLength == 0 && !valueSourceContext.missing && !valueSourceContext.hasScript) {
            MappedFieldType fieldType = valueSourceContext.getFieldType();
            if (fieldType != null) {
                final String fieldName = valueSourceContext.getFieldType().name();
                final long[] bounds = computeBounds.apply(valueSourceContext);
                if (bounds != null) {
                    assert fieldType instanceof DateFieldMapper.DateFieldType;
                    final Rounding rounding = roundingFunction.apply(bounds);
                    final Weight[] filters = FilterRewriteHelper.createFilterForAggregations(
                        context,
                        rounding,
                        preparedRoundingSupplier.get(),
                        fieldName,
                        (DateFieldMapper.DateFieldType) fieldType,
                        bounds[0],
                        bounds[1]
                    );
                    return new FilterContext((DateFieldMapper.DateFieldType) fieldType, filters);
                }
            }
        }
        return null;
    }

    /**
     * Encapsulates metadata about a value source needed to rewrite
     */
    public static class ValueSourceContext {
        private final boolean missing;
        private final boolean hasScript;
        private final MappedFieldType fieldType;

        /**
         * @param missing   whether missing value/bucket is set
         * @param hasScript whether script is used
         * @param fieldType null if the field doesn't exist
         */
        public ValueSourceContext(boolean missing, boolean hasScript, MappedFieldType fieldType) {
            this.missing = missing;
            this.hasScript = hasScript;
            this.fieldType = fieldType;
        }

        // TODO reading why boolean doesn't need getter?
        public MappedFieldType getFieldType() {
            return fieldType;
        }
    }

    public static long getBucketOrd(long bucketOrd) {
        if (bucketOrd < 0) { // already seen
            bucketOrd = -1 - bucketOrd;
        }

        return bucketOrd;
    }

    /**
     * This should be executed for each segment
     *
     * @param size the maximum number of buckets needed
     */
    public static boolean tryFastFilterAggregation(
        final LeafReaderContext ctx,
        final Weight[] filters,
        final DateFieldMapper.DateFieldType fieldType,
        final BiConsumer<Long, Integer> incrementDocCount,
        final int size
    ) throws IOException {
        if (filters == null) return false;

        final int[] counts = new int[filters.length];
        int i;
        for (i = 0; i < filters.length; i++) {
            counts[i] = filters[i].count(ctx);
            if (counts[i] == -1) {
                // Cannot use the optimization if any of the counts
                // is -1 indicating the segment might have deleted documents
                return false;
            }
        }

        int s = 0;
        for (i = 0; i < filters.length; i++) {
            if (counts[i] > 0) {
                incrementDocCount.accept(
                    fieldType.convertNanosToMillis(
                        NumericUtils.sortableBytesToLong(
                            ((PointRangeQuery) filters[i].getQuery()).getLowerPoint(), 0)
                    ),
                    counts[i]
                );
                s++;
                if (s > size) return true;
            }
        }

        return true;
    }
}

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
 * Help rewrite aggregations into filters.
 * Instead of aggregation collects documents one by one, filter may count all documents that match in one pass.
 * <p>
 * Currently supported rewrite:
 * <ul>
 *  <li> date histogram -> date range filter.
 *   Applied: DateHistogramAggregator, AutoDateHistogramAggregator, CompositeAggregator </li>
 * </ul>
 * @opensearch.internal
 */
public class FastFilterRewriteHelper {

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

    /**
     * This method also acts as a pre-condition check for the optimization,
     * returns null if the optimization cannot be applied
     */
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
     * Creates the date range filters for aggregations using the interval, min/max
     * bounds and the rounding values
     */
    private static Weight[] createFilterForAggregations(
        final SearchContext context,
        final long interval,
        final Rounding.Prepared preparedRounding,
        final String field,
        final DateFieldMapper.DateFieldType fieldType,
        long low,
        final long high
    ) throws IOException {
        // Calculate the number of buckets using range and interval
        long roundedLow = preparedRounding.round(fieldType.convertNanosToMillis(low));
        long prevRounded = roundedLow;
        int bucketCount = 0;
        while (roundedLow <= fieldType.convertNanosToMillis(high)) {
            bucketCount++;
            // Below rounding is needed as the interval could return in
            // non-rounded values for something like calendar month
            roundedLow = preparedRounding.round(roundedLow + interval);
            if (prevRounded == roundedLow) break; // prevents getting into an infinite loop
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
                NumericUtils.longToSortableBytes(i == 0 ? low : fieldType.convertRoundedMillisToNanos(roundedLow), lower, 0);

                // Calculate the upper bucket bound
                roundedLow = preparedRounding.round(roundedLow + interval);
                final byte[] upper = new byte[8];
                NumericUtils.longToSortableBytes(i + 1 == bucketCount ? high :
                    // Subtract -1 if the minimum is roundedLow as roundedLow itself
                    // is included in the next bucket
                    fieldType.convertRoundedMillisToNanos(roundedLow) - 1, upper, 0);

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

    /**
     * @param computeBounds            get the lower and upper bound of the field in a shard search
     * @param roundingFunction         produce Rounding that contains interval of date range.
     *                                 Rounding is computed dynamically using the bounds in AutoDateHistogram
     * @param preparedRoundingSupplier produce PreparedRounding to round values at call-time
     */
    public static void buildFastFilter(
        SearchContext context,
        CheckedFunction<FastFilterContext, long[], IOException> computeBounds,
        Function<long[], Rounding> roundingFunction,
        Supplier<Rounding.Prepared> preparedRoundingSupplier,
        FastFilterContext fastFilterContext
    ) throws IOException {
        assert fastFilterContext.fieldType instanceof DateFieldMapper.DateFieldType;
        DateFieldMapper.DateFieldType fieldType = (DateFieldMapper.DateFieldType) fastFilterContext.fieldType;
        final long[] bounds = computeBounds.apply(fastFilterContext); // TODO b do we need to pass in the context? or specific things
        if (bounds != null) {
            final Rounding rounding = roundingFunction.apply(bounds);
            final OptionalLong intervalOpt = Rounding.getInterval(rounding);
            if (intervalOpt.isEmpty()) {
                return;
            }
            final long interval = intervalOpt.getAsLong();

            // afterKey is the last bucket key in previous response, while the bucket key
            // is the start of the bucket values, so add the interval
            if (fastFilterContext.afterKey != -1) {
                bounds[0] = fastFilterContext.afterKey + interval;
            }

            final Weight[] filters = FastFilterRewriteHelper.createFilterForAggregations(
                context,
                interval,
                preparedRoundingSupplier.get(),
                fieldType.name(),
                fieldType,
                bounds[0],
                bounds[1]
            );
            fastFilterContext.setFilters(filters);
        }
    }

    /**
     * Encapsulates metadata about a value source needed to rewrite
     */
    public static class FastFilterContext {
        private boolean missing = false; // TODO b confirm UT that can catch this
        private boolean hasScript = false;
        private boolean showOtherBucket = false;

        private final MappedFieldType fieldType;

        private long afterKey = -1L;
        private int size = Integer.MAX_VALUE; // only used by composite aggregation for pagination
        private Weight[] filters = null;

        private final Type type;

        public FastFilterContext(MappedFieldType fieldType) {
            this.fieldType = fieldType;
            this.type = Type.DATE_HISTO;
        }

        public FastFilterContext(Type type) {
            this.fieldType = null;
            this.type = type;
        }

        public DateFieldMapper.DateFieldType getFieldType() {
            assert fieldType instanceof DateFieldMapper.DateFieldType;
            return (DateFieldMapper.DateFieldType) fieldType;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public void setFilters(Weight[] filters) {
            this.filters = filters;
        }

        public void setAfterKey(long afterKey) {
            this.afterKey = afterKey;
        }

        public void setMissing(boolean missing) {
            this.missing = missing;
        }

        public void setHasScript(boolean hasScript) {
            this.hasScript = hasScript;
        }

        public void setShowOtherBucket(boolean showOtherBucket) {
            this.showOtherBucket = showOtherBucket;
        }

        public boolean isRewriteable(Object parent, int subAggLength) {
            if (parent == null && subAggLength == 0 && !missing && !hasScript) {
                if (type == Type.FILTERS) {
                    return !showOtherBucket;
                } else if (type == Type.DATE_HISTO) {
                    return fieldType != null && fieldType instanceof DateFieldMapper.DateFieldType;
                }
            }
            return false;
        }

        public enum Type {
            FILTERS, DATE_HISTO
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
     * @param incrementDocCount takes in the bucket key value and the bucket count
     */
    public static boolean tryFastFilterAggregation(
        final LeafReaderContext ctx,
        FastFilterContext fastFilterContext,
        final BiConsumer<Long, Integer> incrementDocCount
    ) throws IOException {
        if (fastFilterContext == null) return false;
        if (fastFilterContext.filters == null) return false;

        final Weight[] filters = fastFilterContext.filters;
        // TODO b refactor the type conversion to the context
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
                long bucketKey = i; // the index of filters is the key for filters aggregation
                if (fastFilterContext.type == FastFilterContext.Type.DATE_HISTO) {
                    final DateFieldMapper.DateFieldType fieldType = (DateFieldMapper.DateFieldType) fastFilterContext.fieldType;
                    bucketKey = fieldType.convertNanosToMillis(
                        NumericUtils.sortableBytesToLong(((PointRangeQuery) filters[i].getQuery()).getLowerPoint(), 0)
                    );
                }
                incrementDocCount.accept(bucketKey, counts[i]);
                s++;
                if (s > fastFilterContext.size) return true;
            }
        }

        return true;
    }
}

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
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.DateRangeIncludingNowQuery;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregator;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceConfig;
import org.opensearch.search.aggregations.bucket.composite.RoundingValuesSource;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Utility class to help rewrite aggregations into filters.
 * Instead of aggregation collects documents one by one, filter may count all documents that match in one pass.
 * <p>
 * Currently supported rewrite:
 * <ul>
 *  <li> date histogram : date range filter.
 *   Applied: DateHistogramAggregator, AutoDateHistogramAggregator, CompositeAggregator </li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class FastFilterRewriteHelper {

    private FastFilterRewriteHelper() {}

    private static final int MAX_NUM_FILTER_BUCKETS = 1024;
    private static final Map<Class<?>, Function<Query, Query>> queryWrappers;

    // Initialize the wrapper map for unwrapping the query
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
     * Finds the min and max bounds of field values for the shard
     */
    private static long[] getIndexBounds(final SearchContext context, final String fieldName) throws IOException {
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
        final long[] indexBounds = getIndexBounds(context, fieldName);
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
        // Check if the top-level query (which may be a PRQ on another field) is functionally match-all
        Weight weight = context.searcher().createWeight(context.query(), ScoreMode.COMPLETE_NO_SCORES, 1f);
        for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
            if (weight.count(ctx) != ctx.reader().numDocs()) {
                return null;
            }
        }
        return indexBounds;
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
            if (bucketCount > MAX_NUM_FILTER_BUCKETS) return null;
            // Below rounding is needed as the interval could return in
            // non-rounded values for something like calendar month
            roundedLow = preparedRounding.round(roundedLow + interval);
            if (prevRounded == roundedLow) break; // prevents getting into an infinite loop
            prevRounded = roundedLow;
        }

        Weight[] filters = null;
        if (bucketCount > 0) {
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
     * Context object to do fast filter optimization
     */
    public static class FastFilterContext {
        private Weight[] filters = null;
        public AggregationType aggregationType;

        private void setFilters(Weight[] filters) {
            this.filters = filters;
        }

        public void setAggregationType(AggregationType aggregationType) {
            this.aggregationType = aggregationType;
        }

        public boolean isRewriteable(final Object parent, final int subAggLength) {
            return aggregationType.isRewriteable(parent, subAggLength);
        }

        /**
         * This filter build method is for date histogram aggregation type
         *
         */
        // public void buildFastFilter(
        //     SearchContext context,
        //     CheckedFunction<DateHistogramAggregationType, long[], IOException> computeBounds,
        //     Function<long[], Rounding> roundingFunction,
        //     Supplier<Rounding.Prepared> preparedRoundingSupplier
        // ) throws IOException {
        //     assert this.aggregationType instanceof DateHistogramAggregationType;
        //     DateHistogramAggregationType aggregationType = (DateHistogramAggregationType) this.aggregationType;
        //     DateFieldMapper.DateFieldType fieldType = aggregationType.getFieldType();
        //     final long[] bounds = computeBounds.apply(aggregationType);
        //     if (bounds == null) return;
        //
        //     final Rounding rounding = roundingFunction.apply(bounds);
        //     final OptionalLong intervalOpt = Rounding.getInterval(rounding);
        //     if (intervalOpt.isEmpty()) return;
        //     final long interval = intervalOpt.getAsLong();
        //
        //     // afterKey is the last bucket key in previous response, while the bucket key
        //     // is the start of the bucket values, so add the interval
        //     if (aggregationType instanceof CompositeAggregationType && ((CompositeAggregationType) aggregationType).afterKey != -1) {
        //         bounds[0] = ((CompositeAggregationType) aggregationType).afterKey + interval;
        //     }
        //
        //     final Weight[] filters = FastFilterRewriteHelper.createFilterForAggregations(
        //         context,
        //         interval,
        //         preparedRoundingSupplier.get(),
        //         fieldType.name(),
        //         fieldType,
        //         bounds[0],
        //         bounds[1]
        //     );
        //     this.setFilters(filters);
        // }

        public void buildFastFilter(SearchContext context) throws IOException {
            Weight[] filters = this.aggregationType.buildFastFilter(context);
            this.setFilters(filters);
        }

        // this method should delegate to specific aggregation type

        // can this all be triggered in aggregation type?
    }

    /**
     * Different types have different pre-conditions, filter building logic, etc.
     */
    public interface AggregationType {
        boolean isRewriteable(Object parent, int subAggLength);
        Weight[] buildFastFilter(SearchContext context) throws IOException;
    }

    /**
     * For date histogram aggregation
     */
    public static abstract class AbstractDateHistogramAggregationType implements AggregationType {
        private final MappedFieldType fieldType;
        private final boolean missing;
        private final boolean hasScript;
        private LongBounds hardBounds = null;

        public AbstractDateHistogramAggregationType(MappedFieldType fieldType, boolean missing, boolean hasScript) {
            this.fieldType = fieldType;
            this.missing = missing;
            this.hasScript = hasScript;
        }

        public AbstractDateHistogramAggregationType(MappedFieldType fieldType, boolean missing, boolean hasScript, LongBounds hardBounds) {
            this(fieldType, missing, hasScript);
            this.hardBounds = hardBounds;
        }

        @Override
        public boolean isRewriteable(Object parent, int subAggLength) {
            if (parent == null && subAggLength == 0 && !missing && !hasScript) {
                return fieldType != null && fieldType instanceof DateFieldMapper.DateFieldType;
            }
            return false;
        }

        @Override
        public Weight[] buildFastFilter(SearchContext context) throws IOException {
            // the procedure in this method can be separated
            // 1. compute bound
            // 2. get rounding, interval
            // 3. get prepared rounding, better combined with step 2
            Weight[] result = {};

            long[] bounds = computeBounds(context);
            if (bounds == null) return result;

            final Rounding rounding = getRounding(bounds[0], bounds[1]);
            final OptionalLong intervalOpt = Rounding.getInterval(rounding);
            if (intervalOpt.isEmpty()) return result;
            final long interval = intervalOpt.getAsLong();

            // afterKey is the last bucket key in previous response, while the bucket key
            // is the start of the bucket values, so add the interval
            // if (aggregationType instanceof CompositeAggregationType && ((CompositeAggregationType) aggregationType).afterKey != -1) {
            //     bounds[0] = ((CompositeAggregationType) aggregationType).afterKey + interval;
            // }
            processAfterKey(bounds, interval);

            return FastFilterRewriteHelper.createFilterForAggregations(
                context,
                interval,
                getRoundingPrepared(),
                fieldType.name(),
                (DateFieldMapper.DateFieldType) fieldType,
                bounds[0],
                bounds[1]
            );
        }

        protected long[] computeBounds(SearchContext context) throws IOException {
            final long[] bounds = getAggregationBounds(context, fieldType.name());
            if (bounds != null) {
                // Update min/max limit if user specified any hard bounds
                if (hardBounds != null) {
                    bounds[0] = Math.max(bounds[0], hardBounds.getMin());
                    bounds[1] = Math.min(bounds[1], hardBounds.getMax() - 1); // hard bounds max is exclusive
                }
            }
            return bounds;
        }

        protected abstract Rounding getRounding(final long low, final long high);
        protected abstract Rounding.Prepared getRoundingPrepared();

        protected void processAfterKey(long[] bound, long interval) {};

        public DateFieldMapper.DateFieldType getFieldType() {
            assert fieldType instanceof DateFieldMapper.DateFieldType;
            return (DateFieldMapper.DateFieldType) fieldType;
        }
    }

    /**
     * For composite aggregation with date histogram as a source
     */
    // public static class CompositeAggregationType extends DateHistogramAggregationType {
    //     private final RoundingValuesSource valuesSource;
    //     private long afterKey = -1L;
    //     private final int size;
    //
    //     public CompositeAggregationType(
    //         CompositeValuesSourceConfig[] sourceConfigs,
    //         CompositeKey rawAfterKey,
    //         List<DocValueFormat> formats,
    //         int size
    //     ) {
    //         super(sourceConfigs[0].fieldType(), sourceConfigs[0].missingBucket(), sourceConfigs[0].hasScript());
    //         this.valuesSource = (RoundingValuesSource) sourceConfigs[0].valuesSource();
    //         this.size = size;
    //         if (rawAfterKey != null) {
    //             assert rawAfterKey.size() == 1 && formats.size() == 1;
    //             this.afterKey = formats.get(0).parseLong(rawAfterKey.get(0).toString(), false, () -> {
    //                 throw new IllegalArgumentException("now() is not supported in [after] key");
    //             });
    //         }
    //     }
    //
    //     public Rounding getRounding() {
    //         return valuesSource.getRounding();
    //     }
    //
    //     public Rounding.Prepared getRoundingPreparer() {
    //         return valuesSource.getPreparedRounding();
    //     }
    // }

    public static boolean isCompositeAggRewriteable(CompositeValuesSourceConfig[] sourceConfigs) {
        return sourceConfigs.length == 1 && sourceConfigs[0].valuesSource() instanceof RoundingValuesSource;
    }

    public static long getBucketOrd(long bucketOrd) {
        if (bucketOrd < 0) { // already seen
            bucketOrd = -1 - bucketOrd;
        }

        return bucketOrd;
    }

    /**
     * This is executed for each segment by passing the leaf reader context
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
        int size = Integer.MAX_VALUE;
        for (i = 0; i < filters.length; i++) {
            if (counts[i] > 0) {
                long bucketKey = i; // the index of filters is the key for filters aggregation
                if (fastFilterContext.aggregationType instanceof AbstractDateHistogramAggregationType) {
                    final DateFieldMapper.DateFieldType fieldType = ((AbstractDateHistogramAggregationType) fastFilterContext.aggregationType)
                        .getFieldType();
                    bucketKey = fieldType.convertNanosToMillis(
                        NumericUtils.sortableBytesToLong(((PointRangeQuery) filters[i].getQuery()).getLowerPoint(), 0)
                    );
                    if (fastFilterContext.aggregationType instanceof CompositeAggregator.CompositeAggregationType) {
                        size = ((CompositeAggregator.CompositeAggregationType) fastFilterContext.aggregationType).getSize();
                    }
                }
                incrementDocCount.accept(bucketKey, counts[i]);
                s++;
                if (s > size) return true;
            }
        }

        return true;
    }
}

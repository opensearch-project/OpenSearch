/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
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
import org.opensearch.index.mapper.DocCountFieldMapper;
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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

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

    private static final Logger logger = LogManager.getLogger(FastFilterRewriteHelper.class);

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
     * Finds the global min and max bounds of the field for the shard from each segment
     *
     * @return null if the field is empty or not indexed
     */
    private static long[] getIndexBounds(final SearchContext context, final String fieldName) throws IOException {
        final List<LeafReaderContext> leaves = context.searcher().getIndexReader().leaves();
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (LeafReaderContext leaf : leaves) {
            final PointValues values = leaf.reader().getPointValues(fieldName);
            if (values != null) {
                min = Math.min(min, NumericUtils.sortableBytesToLong(values.getMinPackedValue(), 0));
                max = Math.max(max, NumericUtils.sortableBytesToLong(values.getMaxPackedValue(), 0));
            } else {
                // "values" is null if the field is not indexed
                return null;
            }
        }

        if (min == Long.MAX_VALUE || max == Long.MIN_VALUE) {
            return null;
        }
        return new long[] { min, max };
    }

    /**
     * This method also acts as a pre-condition check for the optimization
     *
     * @return null if the processed query not as expected
     */
    public static long[] getDateHistoAggBounds(final SearchContext context, final String fieldName) throws IOException {
        final Query cq = unwrapIntoConcreteQuery(context.query());
        if (cq instanceof PointRangeQuery) {
            final PointRangeQuery prq = (PointRangeQuery) cq;
            // Ensure that the query and aggregation are on the same field
            if (prq.getField().equals(fieldName)) {
                final long[] indexBounds = getIndexBounds(context, fieldName);
                return new long[] {
                    // Minimum bound for aggregation is the max between query and global
                    Math.max(NumericUtils.sortableBytesToLong(prq.getLowerPoint(), 0), indexBounds[0]),
                    // Maximum bound for aggregation is the min between query and global
                    Math.min(NumericUtils.sortableBytesToLong(prq.getUpperPoint(), 0), indexBounds[1]) };
            }
        } else if (cq instanceof MatchAllDocsQuery) {
            return getIndexBounds(context, fieldName);
        } else if (cq instanceof FieldExistsQuery) {
            // when a range query covers all values of a shard, it will be rewrite field exists query
            if (((FieldExistsQuery) cq).getField().equals(fieldName)) {
                return getIndexBounds(context, fieldName);
            }
        }

        return null;
    }

    /**
     * Creates the date range filters for aggregations using the interval, min/max
     * bounds and prepared rounding
     */
    private static Weight[] createFilterForAggregations(
        final SearchContext context,
        final DateFieldMapper.DateFieldType fieldType,
        final long interval,
        final Rounding.Prepared preparedRounding,
        long low,
        final long high
    ) throws IOException {
        // Calculate the number of buckets using range and interval
        long roundedLow = preparedRounding.round(fieldType.convertNanosToMillis(low));
        long prevRounded = roundedLow;
        int bucketCount = 0;
        while (roundedLow <= fieldType.convertNanosToMillis(high)) {
            bucketCount++;
            if (bucketCount > MAX_NUM_FILTER_BUCKETS) {
                logger.debug("Max number of filters reached [{}], skip the fast filter optimization", MAX_NUM_FILTER_BUCKETS);
                return null;
            }
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

                filters[i++] = context.searcher().createWeight(new PointRangeQuery(fieldType.name(), lower, upper, 1) {
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
     * Context object for fast filter optimization
     * <p>
     * Usage: first set aggregation type, then check isRewriteable, then buildFastFilter
     */
    public static class FastFilterContext {
        private boolean rewriteable = false;
        private Weight[] filters = null;
        private boolean filtersBuiltAtShardLevel = false;

        public AggregationType aggregationType;
        private final SearchContext context;

        public FastFilterContext(SearchContext context) {
            this.context = context;
        }

        public void setAggregationType(AggregationType aggregationType) {
            this.aggregationType = aggregationType;
        }

        public boolean isRewriteable(final Object parent, final int subAggLength) {
            boolean rewriteable = aggregationType.isRewriteable(parent, subAggLength);
            logger.debug("Fast filter rewriteable: {} for shard {}", rewriteable, context.indexShard().shardId());
            this.rewriteable = rewriteable;
            return rewriteable;
        }

        public void buildFastFilter() throws IOException {
            this.buildFastFilter(FastFilterRewriteHelper::getDateHistoAggBounds);
        }

        private void buildFastFilter(GetBounds<SearchContext, String, long[]> getBounds) throws IOException {
            assert filters == null : "Filters should only be built once, but they are already built";
            Weight[] filters = this.aggregationType.buildFastFilter(context, getBounds);
            if (filters != null) {
                logger.debug("Fast filter built for shard {}", context.indexShard().shardId());
                filtersBuiltAtShardLevel = true;
                this.filters = filters;
            }
        }

        private boolean filterBuilt() {
            return filters != null;
        }
    }

    /**
     * Different types have different pre-conditions, filter building logic, etc.
     */
    public interface AggregationType {

        boolean isRewriteable(Object parent, int subAggLength);

        Weight[] buildFastFilter(SearchContext ctx, GetBounds<SearchContext, String, long[]> getBounds) throws IOException;
    }

    /**
     * Functional interface for getting bounds for date histogram aggregation
     */
    @FunctionalInterface
    public interface GetBounds<T, U, R> {
        R apply(T t, U u) throws IOException;
    }

    /**
     * For date histogram aggregation
     */
    public static abstract class AbstractDateHistogramAggregationType implements AggregationType {
        private final MappedFieldType fieldType;
        private final boolean missing;
        private final boolean hasScript;
        private LongBounds hardBounds;

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
        public Weight[] buildFastFilter(SearchContext context, GetBounds<SearchContext, String, long[]> getBounds) throws IOException {
            long[] bounds = getBounds.apply(context, fieldType.name());
            bounds = processHardBounds(bounds);
            logger.debug("Bounds are {} for shard {}", bounds, context.indexShard().shardId());
            if (bounds == null) {
                return null;
            }
            assert bounds[0] <= bounds[1] : "Low bound should be less than high bound";

            final Rounding rounding = getRounding(bounds[0], bounds[1]);
            final OptionalLong intervalOpt = Rounding.getInterval(rounding);
            if (intervalOpt.isEmpty()) {
                return null;
            }
            final long interval = intervalOpt.getAsLong();

            // process the after key of composite agg
            processAfterKey(bounds, interval);

            return FastFilterRewriteHelper.createFilterForAggregations(
                context,
                (DateFieldMapper.DateFieldType) fieldType,
                interval,
                getRoundingPrepared(),
                bounds[0],
                bounds[1]
            );
        }

        protected abstract Rounding getRounding(final long low, final long high);

        protected abstract Rounding.Prepared getRoundingPrepared();

        protected void processAfterKey(long[] bound, long interval) {}

        protected long[] processHardBounds(long[] bounds) {
            if (bounds != null) {
                // Update min/max limit if user specified any hard bounds
                if (hardBounds != null) {
                    if (hardBounds.getMin() > bounds[0]) {
                        bounds[0] = hardBounds.getMin();
                    }
                    if (hardBounds.getMax() - 1 < bounds[1]) {
                        bounds[1] = hardBounds.getMax() - 1; // hard bounds max is exclusive
                    }
                    if (bounds[0] > bounds[1]) {
                        return null;
                    }
                }
            }
            return bounds;
        }

        public DateFieldMapper.DateFieldType getFieldType() {
            assert fieldType instanceof DateFieldMapper.DateFieldType;
            return (DateFieldMapper.DateFieldType) fieldType;
        }
    }

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
     * Try to get the bucket doc counts from the fast filters for the aggregation
     *
     * @param incrementDocCount takes in the bucket key value and the bucket count
     */
    public static boolean tryFastFilterAggregation(
        final LeafReaderContext ctx,
        FastFilterContext fastFilterContext,
        final BiConsumer<Long, Integer> incrementDocCount
    ) throws IOException {
        if (fastFilterContext == null) return false;
        if (!fastFilterContext.rewriteable) return false;

        NumericDocValues docCountValues = DocValues.getNumeric(ctx.reader(), DocCountFieldMapper.NAME);
        if (docCountValues.nextDoc() != NO_MORE_DOCS) {
            logger.debug(
                "Shard {} segment {} has at least one document with _doc_count field, skip fast filter optimization",
                fastFilterContext.context.indexShard().shardId(),
                ctx.ord
            );
            return false;
        }

        // if no filters built at shard level (see getDateHistoAggBounds method for possible reasons)
        // check if the query is functionally match-all at segment level
        if (!fastFilterContext.filtersBuiltAtShardLevel && !segmentMatchAll(fastFilterContext.context, ctx)) return false;
        if (!fastFilterContext.filterBuilt()) {
            logger.debug(
                "Shard {} segment {} functionally match all documents. Build the fast filter",
                fastFilterContext.context.indexShard().shardId(),
                ctx.ord
            );
            fastFilterContext.buildFastFilter(FastFilterRewriteHelper::getIndexBounds);
        }
        if (!fastFilterContext.filterBuilt()) return false;

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
                    final DateFieldMapper.DateFieldType fieldType =
                        ((AbstractDateHistogramAggregationType) fastFilterContext.aggregationType).getFieldType();
                    bucketKey = fieldType.convertNanosToMillis(
                        NumericUtils.sortableBytesToLong(((PointRangeQuery) filters[i].getQuery()).getLowerPoint(), 0)
                    );
                    if (fastFilterContext.aggregationType instanceof CompositeAggregator.CompositeAggregationType) {
                        size = ((CompositeAggregator.CompositeAggregationType) fastFilterContext.aggregationType).getSize();
                    }
                }
                incrementDocCount.accept(bucketKey, counts[i]);
                s++;
                if (s > size) {
                    logger.debug("Fast filter optimization applied to composite aggregation with size {}", size);
                    return true;
                }
            }
        }

        logger.debug("Fast filter optimization applied");
        return true;
    }

    private static boolean segmentMatchAll(SearchContext ctx, LeafReaderContext leafCtx) throws IOException {
        Weight weight = ctx.searcher().createWeight(ctx.query(), ScoreMode.COMPLETE_NO_SCORES, 1f);
        assert weight != null;
        int count = weight.count(leafCtx);
        return count > 0 && count == leafCtx.reader().numDocs();
    }
}

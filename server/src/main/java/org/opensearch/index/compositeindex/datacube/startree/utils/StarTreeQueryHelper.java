/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.StarTreeMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.metrics.MetricAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeFilter;
import org.opensearch.search.startree.StarTreeQueryContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Helper class for building star-tree query
 *
 * @opensearch.internal
 * @opensearch.experimental
 */
public class StarTreeQueryHelper {

    /**
     * Checks if the search context can be supported by star-tree
     */
    public static boolean isStarTreeSupported(SearchContext context) {
        return context.aggregations() != null
            && context.mapperService().isCompositeIndexPresent()
            && context.parsedPostFilter() == null
            && context.innerHits().getInnerHits().isEmpty()
            && context.sort() == null
            && context.trackScores() == false
            && context.minimumScore() == null;
    }

    /**
     * Gets StarTreeQueryContext from the search context and source builder.
     * Returns null if the query & aggregation cannot be supported.
     */
    public static StarTreeQueryContext getStarTreeQueryContext(SearchContext context, SearchSourceBuilder source) throws IOException {
        // Current implementation assumes only single star-tree is supported
        CompositeDataCubeFieldType compositeMappedFieldType = (StarTreeMapper.StarTreeFieldType) context.mapperService()
            .getCompositeFieldTypes()
            .iterator()
            .next();
        CompositeIndexFieldInfo starTree = new CompositeIndexFieldInfo(
            compositeMappedFieldType.name(),
            compositeMappedFieldType.getCompositeIndexType()
        );

        StarTreeQueryContext starTreeQueryContext = StarTreeQueryHelper.toStarTreeQueryContext(
            starTree,
            compositeMappedFieldType,
            source.query()
        );
        if (starTreeQueryContext == null) {
            return null;
        }

        for (AggregatorFactory aggregatorFactory : context.aggregations().factories().getFactories()) {
            MetricStat metricStat = validateStarTreeMetricSupport(compositeMappedFieldType, aggregatorFactory);
            if (metricStat == null) {
                return null;
            }
        }

        if (context.aggregations().factories().getFactories().length > 1) {
            context.initializeStarTreeValuesMap();
        }
        return starTreeQueryContext;
    }

    /**
     * Uses query builder & composite index info to form star-tree query context
     */
    private static StarTreeQueryContext toStarTreeQueryContext(
        CompositeIndexFieldInfo compositeIndexFieldInfo,
        CompositeDataCubeFieldType compositeFieldType,
        QueryBuilder queryBuilder
    ) {
        Map<String, Long> queryMap;
        if (queryBuilder == null || queryBuilder instanceof MatchAllQueryBuilder) {
            queryMap = null;
        } else if (queryBuilder instanceof TermQueryBuilder) {
            List<String> supportedDimensions = compositeFieldType.getDimensions()
                .stream()
                .map(Dimension::getField)
                .collect(Collectors.toList());
            queryMap = getStarTreePredicates(queryBuilder, supportedDimensions);
            if (queryMap == null) {
                return null;
            }
        } else {
            return null;
        }
        return new StarTreeQueryContext(compositeIndexFieldInfo, queryMap);
    }

    /**
     * Parse query body to star-tree predicates
     * @param queryBuilder to match star-tree supported query shape
     * @return predicates to match
     */
    private static Map<String, Long> getStarTreePredicates(QueryBuilder queryBuilder, List<String> supportedDimensions) {
        TermQueryBuilder tq = (TermQueryBuilder) queryBuilder;
        String field = tq.fieldName();
        if (!supportedDimensions.contains(field)) {
            return null;
        }
        long inputQueryVal = Long.parseLong(tq.value().toString());

        // Create a map with the field and the value
        Map<String, Long> predicateMap = new HashMap<>();
        predicateMap.put(field, inputQueryVal);
        return predicateMap;
    }

    private static MetricStat validateStarTreeMetricSupport(
        CompositeDataCubeFieldType compositeIndexFieldInfo,
        AggregatorFactory aggregatorFactory
    ) {
        if (aggregatorFactory instanceof MetricAggregatorFactory && aggregatorFactory.getSubFactories().getFactories().length == 0) {
            String field;
            Map<String, List<MetricStat>> supportedMetrics = compositeIndexFieldInfo.getMetrics()
                .stream()
                .collect(Collectors.toMap(Metric::getField, Metric::getMetrics));

            MetricStat metricStat = ((MetricAggregatorFactory) aggregatorFactory).getMetricStat();
            field = ((MetricAggregatorFactory) aggregatorFactory).getField();

            if (supportedMetrics.containsKey(field) && supportedMetrics.get(field).contains(metricStat)) {
                return metricStat;
            }
        }
        return null;
    }

    public static CompositeIndexFieldInfo getSupportedStarTree(SearchContext context) {
        StarTreeQueryContext starTreeQueryContext = context.getStarTreeQueryContext();
        return (starTreeQueryContext != null) ? starTreeQueryContext.getStarTree() : null;
    }

    public static StarTreeValues getStarTreeValues(LeafReaderContext context, CompositeIndexFieldInfo starTree) throws IOException {
        SegmentReader reader = Lucene.segmentReader(context.reader());
        if (!(reader.getDocValuesReader() instanceof CompositeIndexReader)) {
            return null;
        }
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
        return (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(starTree);
    }

    /**
     * Get the star-tree leaf collector
     * This collector computes the aggregation prematurely and invokes an early termination collector
     */
    public static LeafBucketCollector getStarTreeLeafCollector(
        SearchContext context,
        ValuesSource.Numeric valuesSource,
        LeafReaderContext ctx,
        LeafBucketCollector sub,
        CompositeIndexFieldInfo starTree,
        String metric,
        Consumer<Long> valueConsumer,
        Runnable finalConsumer
    ) throws IOException {
        StarTreeValues starTreeValues = getStarTreeValues(ctx, starTree);
        assert starTreeValues != null;
        String fieldName = ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName();
        String metricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(starTree.getField(), fieldName, metric);

        assert starTreeValues != null;
        SortedNumericStarTreeValuesIterator valuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues.getMetricValuesIterator(
            metricName
        );
        // Obtain a FixedBitSet of matched document IDs
        FixedBitSet matchedDocIds = getStarTreeFilteredValues(context, ctx, starTreeValues);  // Assuming this method gives a FixedBitSet
        assert matchedDocIds != null;

        int numBits = matchedDocIds.length();  // Get the length of the FixedBitSet
        if (numBits > 0) {
            // Iterate over the FixedBitSet
            for (int bit = matchedDocIds.nextSetBit(0); bit != -1; bit = (bit + 1 < numBits) ? matchedDocIds.nextSetBit(bit + 1) : -1) {
                // Advance to the bit (entryId) in the valuesIterator
                if (valuesIterator.advance(bit) == StarTreeValuesIterator.NO_MORE_ENTRIES) {
                    continue;  // Skip if no more entries
                }

                // Iterate over the values for the current entryId
                for (int i = 0, count = valuesIterator.valuesCount(); i < count; i++) {
                    long value = valuesIterator.nextValue();
                    valueConsumer.accept(value); // Apply the consumer operation (e.g., max, sum)
                }
            }
        }

        // Call the final consumer after processing all entries
        finalConsumer.run();

        // Return a LeafBucketCollector that terminates collection
        return new LeafBucketCollectorBase(sub, valuesSource.doubleValues(ctx)) {
            @Override
            public void collect(int doc, long bucket) {
                throw new CollectionTerminatedException();
            }
        };
    }

    /**
     * Get the filtered values for the star-tree query
     */
    public static FixedBitSet getStarTreeFilteredValues(SearchContext context, LeafReaderContext ctx, StarTreeValues starTreeValues)
        throws IOException {
        if (context.getStarTreeValuesMap() != null && context.getStarTreeValuesMap().containsKey(ctx)) {
            return context.getStarTreeValuesMap().get(ctx);
        }

        StarTreeFilter filter = new StarTreeFilter(starTreeValues, context.getStarTreeQueryContext().getQueryMap());
        FixedBitSet result = filter.getStarTreeResult();

        context.getStarTreeValuesMap().put(ctx, result);
        return result;
    }
}

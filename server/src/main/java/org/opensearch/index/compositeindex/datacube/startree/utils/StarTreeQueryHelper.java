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

    private static Map<LeafReaderContext, StarTreeValues> starTreeValuesMap = new HashMap<>();

    /**
     * Checks if the search context can be supported by star-tree
     */
    public static boolean isStarTreeSupported(SearchContext context, boolean trackTotalHits) {
        boolean canUseStarTree = context.aggregations() != null
            && context.size() == 0
            && context.mapperService().isCompositeIndexPresent()
            && context.parsedPostFilter() == null
            && context.innerHits().getInnerHits().isEmpty()
            && context.sort() == null
            && (!trackTotalHits || context.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_DISABLED)
            && context.trackScores() == false
            && context.minimumScore() == null
            && context.terminateAfter() == 0;
        return canUseStarTree;
    }

    /**
     * Gets a parsed OriginalOrStarTreeQuery from the search context and source builder.
     * Returns null if the query cannot be supported.
     */

    /**
     * Gets a parsed OriginalOrStarTreeQuery from the search context and source builder.
     * Returns null if the query cannot be supported.
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
            if (validateStarTreeMetricSuport(compositeMappedFieldType, aggregatorFactory) == false) {
                return null;
            }
        }

        return starTreeQueryContext;
    }

    private static StarTreeQueryContext toStarTreeQueryContext(
        CompositeIndexFieldInfo starTree,
        CompositeDataCubeFieldType compositeIndexFieldInfo,
        QueryBuilder queryBuilder
    ) {
        Map<String, Long> queryMap;
        if (queryBuilder == null || queryBuilder instanceof MatchAllQueryBuilder) {
            queryMap = null;
        } else if (queryBuilder instanceof TermQueryBuilder) {
            List<String> supportedDimensions = compositeIndexFieldInfo.getDimensions()
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

        return new StarTreeQueryContext(starTree, queryMap);
    }

    /**
     * Parse query body to star-tree predicates
     * @param queryBuilder
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

    private static boolean validateStarTreeMetricSuport(
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
            return supportedMetrics.containsKey(field) && supportedMetrics.get(field).contains(metricStat);
        } else {
            return false;
        }
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
        String fieldName = ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName();
        String metricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(starTree.getField(), fieldName, metric);

        assert starTreeValues != null;
        SortedNumericStarTreeValuesIterator valuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues.getMetricValuesIterator(
            metricName
        );
        StarTreeValuesIterator result = context.getStarTreeFilteredValues(ctx, starTreeValues);

        int entryId;
        while ((entryId = result.nextEntry()) != StarTreeValuesIterator.NO_MORE_ENTRIES) {
            if (valuesIterator.advance(entryId) != StarTreeValuesIterator.NO_MORE_ENTRIES) {
                int count = valuesIterator.valuesCount();
                for (int i = 0; i < count; i++) {
                    long value = valuesIterator.nextValue();
                    valueConsumer.accept(value); // Apply the operation (max, sum, etc.)
                }
            }
        }
        finalConsumer.run();
        return new LeafBucketCollectorBase(sub, valuesSource.doubleValues(ctx)) {
            @Override
            public void collect(int doc, long bucket) {
                throw new CollectionTerminatedException();
            }
        };
    }
}

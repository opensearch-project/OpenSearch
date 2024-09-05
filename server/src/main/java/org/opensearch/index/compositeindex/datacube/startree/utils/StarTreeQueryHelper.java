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
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.StarTreeMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.metrics.MetricAggregatorFactory;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.OriginalOrStarTreeQuery;
import org.opensearch.search.startree.StarTreeQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public static OriginalOrStarTreeQuery getOriginalOrStarTreeQuery(SearchContext context, SearchSourceBuilder source) throws IOException {
        // Current implementation assumes only single star-tree is supported
        CompositeDataCubeFieldType compositeMappedFieldType = (StarTreeMapper.StarTreeFieldType) context.mapperService()
            .getCompositeFieldTypes()
            .iterator()
            .next();
        CompositeIndexFieldInfo starTree = new CompositeIndexFieldInfo(
            compositeMappedFieldType.name(),
            compositeMappedFieldType.getCompositeIndexType()
        );

        StarTreeQuery starTreeQuery = StarTreeQueryHelper.toStarTreeQuery(starTree, compositeMappedFieldType, source.query());
        if (starTreeQuery == null) {
            return null;
        }

        for (AggregatorFactory aggregatorFactory : context.aggregations().factories().getFactories()) {
            if (validateStarTreeMetricSuport(compositeMappedFieldType, aggregatorFactory) == false) {
                return null;
            }
        }

        return new OriginalOrStarTreeQuery(starTreeQuery, context.query());
    }

    private static StarTreeQuery toStarTreeQuery(
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

        return new StarTreeQuery(starTree, queryMap);
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
        if (context.query() instanceof StarTreeQuery) {
            return ((StarTreeQuery) context.query()).getStarTree();
        }
        return null;
    }

    public static StarTreeValues getStarTreeValues(LeafReaderContext context, CompositeIndexFieldInfo starTree) throws IOException {
        SegmentReader reader = Lucene.segmentReader(context.reader());
        if (!(reader.getDocValuesReader() instanceof CompositeIndexReader)) {
            return null;
        }
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
        return (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(starTree);
    }
}

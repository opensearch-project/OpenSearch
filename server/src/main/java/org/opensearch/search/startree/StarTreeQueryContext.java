/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregatorFactory;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.opensearch.search.aggregations.metrics.MetricAggregatorFactory;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.filter.StarTreeFilter;
import org.opensearch.search.startree.filter.provider.StarTreeFilterProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Stores the star tree related context of a search request.
 */
@ExperimentalApi
public class StarTreeQueryContext {

    private final CompositeDataCubeFieldType compositeMappedFieldType;

    /**
     * Cache for leaf results
     * This is used to cache the results for each leaf reader context
     * to avoid reading the filtered values from the leaf reader context multiple times
     */
    // TODO : Change caching to be based on aggregation specific filters.
    private final FixedBitSet[] perSegmentNodeIdsCache;

    private final QueryBuilder baseQueryBuilder;
    private StarTreeFilter baseStarTreeFilter;

    // TODO : Implement storing and aggregating aggregation specific filters.

    public StarTreeQueryContext(SearchContext context, QueryBuilder baseQueryBuilder) {
        this.baseQueryBuilder = baseQueryBuilder;
        // TODO : We need to select the most appropriate one from multiple star tree field types.
        compositeMappedFieldType = (CompositeDataCubeFieldType) context.mapperService().getCompositeFieldTypes().iterator().next();
        // need to cache star tree values only for multiple aggregations
        boolean cacheStarTreeValues = context.aggregations().factories().getFactories().length > 1;
        int cacheSize = cacheStarTreeValues ? context.indexShard().segments(false).size() : -1;
        if (cacheSize > -1) {
            perSegmentNodeIdsCache = new FixedBitSet[cacheSize];
        } else {
            perSegmentNodeIdsCache = null;
        }
    }

    // TODO : Make changes to change visibility into package private. Handle the same in @org.opensearch.search.SearchServiceStarTreeTests
    public StarTreeQueryContext(CompositeDataCubeFieldType compositeMappedFieldType, QueryBuilder baseQueryBuilder, int cacheSize) {
        this.compositeMappedFieldType = compositeMappedFieldType;
        this.baseQueryBuilder = baseQueryBuilder;
        if (cacheSize > -1) {
            perSegmentNodeIdsCache = new FixedBitSet[cacheSize];
        } else {
            perSegmentNodeIdsCache = null;
        }
    }

    public CompositeIndexFieldInfo getStarTree() {
        return new CompositeIndexFieldInfo(compositeMappedFieldType.name(), compositeMappedFieldType.getCompositeIndexType());
    }

    public FixedBitSet maybeGetCachedNodeIdsForSegment(int ordinal) {
        return perSegmentNodeIdsCache != null ? perSegmentNodeIdsCache[ordinal] : null;
    }

    public FixedBitSet[] getAllCachedValues() {
        return perSegmentNodeIdsCache;
    }

    public void maybeSetCachedNodeIdsForSegment(int key, FixedBitSet values) {
        if (perSegmentNodeIdsCache != null) {
            perSegmentNodeIdsCache[key] = values;
        }
    }

    /**
     * Generates the Base StarTreeFilter and then recursively merges
     * any aggregation specific STF.
     * @return true if recursively all filters were consolidated, else false.
     */
    public boolean consolidateAllFilters(SearchContext context) {
        // Validate the fields and metrics required by aggregations are supported in star tree
        for (AggregatorFactory aggregatorFactory : context.aggregations().factories().getFactories()) {
            // first check for aggregation is a metric aggregation
            if (validateStarTreeMetricSupport(compositeMappedFieldType, aggregatorFactory)) {
                continue;
            }

            // if not a metric aggregation, check for applicable date histogram shape
            if (validateDateHistogramSupport(compositeMappedFieldType, aggregatorFactory)) {
                continue;
            }

            // validation for terms aggregation
            if (validateKeywordTermsAggregationSupport(compositeMappedFieldType, aggregatorFactory)) {
                continue;
            }

            // invalid query shape
            return false;
        }

        // Generate the base Star Tree Filter
        if (baseQueryBuilder != null) {
            baseStarTreeFilter = getStarTreeFilter(context, baseQueryBuilder, compositeMappedFieldType);
            return baseStarTreeFilter != null; // Base Query is not supported by star tree filter.
        }
        // TODO : Generate StarTreeFilter specific to aggregations by merging base and their parents.
        return true;
    }

    public StarTreeFilter getBaseQueryStarTreeFilter() {
        if (baseStarTreeFilter == null) {
            return new StarTreeFilter(Collections.emptyMap());
        }
        return baseStarTreeFilter;
    }

    // TODO : Push this validation down to a common method in AggregatorFactory or an equivalent place.
    private static boolean validateStarTreeMetricSupport(
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

            return field != null && supportedMetrics.containsKey(field) && supportedMetrics.get(field).contains(metricStat);
        }
        return false;
    }

    private static boolean validateKeywordTermsAggregationSupport(
        CompositeDataCubeFieldType compositeIndexFieldInfo,
        AggregatorFactory aggregatorFactory
    ) {
        if (!(aggregatorFactory instanceof TermsAggregatorFactory termsAggregatorFactory)) {
            return false;
        }

        // Validate request field is part of dimensions
        if (compositeIndexFieldInfo.getDimensions()
            .stream()
            .map(Dimension::getField)
            .noneMatch(termsAggregatorFactory.getField()::equals)) {
            return false;
        }

        // Validate all sub-factories
        for (AggregatorFactory subFactory : aggregatorFactory.getSubFactories().getFactories()) {
            if (!validateStarTreeMetricSupport(compositeIndexFieldInfo, subFactory)) {
                return false;
            }
        }
        return true;
    }

    private StarTreeFilter getStarTreeFilter(
        SearchContext context,
        QueryBuilder queryBuilder,
        CompositeDataCubeFieldType compositeMappedFieldType
    ) {
        StarTreeFilterProvider starTreeFilterProvider = StarTreeFilterProvider.SingletonFactory.getProvider(queryBuilder);
        // The query builder's support is not implemented.
        if (starTreeFilterProvider == null) {
            return null;
        }
        try {
            return starTreeFilterProvider.getFilter(context, queryBuilder, compositeMappedFieldType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean validateDateHistogramSupport(
        CompositeDataCubeFieldType compositeIndexFieldInfo,
        AggregatorFactory aggregatorFactory
    ) {
        if (!(aggregatorFactory instanceof DateHistogramAggregatorFactory dateHistogramAggregatorFactory)
            || aggregatorFactory.getSubFactories().getFactories().length < 1) {
            return false;
        }

        // Find the DateDimension in the dimensions list
        DateDimension starTreeDateDimension = null;
        for (Dimension dimension : compositeIndexFieldInfo.getDimensions()) {
            if (dimension instanceof DateDimension) {
                starTreeDateDimension = (DateDimension) dimension;
                break;
            }
        }

        // If no DateDimension is found, validation fails
        if (starTreeDateDimension == null) {
            return false;
        }

        // Ensure the rounding is not null
        if (dateHistogramAggregatorFactory.getRounding() == null) {
            return false;
        }

        // Find the closest valid interval in the DateTimeUnitRounding class associated with star tree
        DateTimeUnitRounding rounding = starTreeDateDimension.findClosestValidInterval(
            new DateTimeUnitAdapter(dateHistogramAggregatorFactory.getRounding())
        );
        if (rounding == null) {
            return false;
        }

        // Validate all sub-factories
        for (AggregatorFactory subFactory : aggregatorFactory.getSubFactories().getFactories()) {
            if (!validateStarTreeMetricSupport(compositeIndexFieldInfo, subFactory)) {
                return false;
            }
        }
        return true;
    }

}

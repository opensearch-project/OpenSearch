/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactory;
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
    private final FixedBitSet[] starTreeValues;

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
            starTreeValues = new FixedBitSet[cacheSize];
        } else {
            starTreeValues = null;
        }
    }

    public StarTreeQueryContext(CompositeDataCubeFieldType compositeMappedFieldType, QueryBuilder baseQueryBuilder, int cacheSize) {
        this.compositeMappedFieldType = compositeMappedFieldType;
        this.baseQueryBuilder = baseQueryBuilder;
        if (cacheSize > -1) {
            starTreeValues = new FixedBitSet[cacheSize];
        } else {
            starTreeValues = null;
        }
    }

    public CompositeIndexFieldInfo getStarTree() {
        return new CompositeIndexFieldInfo(compositeMappedFieldType.name(), compositeMappedFieldType.getCompositeIndexType());
    }

    public FixedBitSet getStarTreeValue(LeafReaderContext ctx) {
        return starTreeValues != null ? starTreeValues[ctx.ord] : null;
    }

    public FixedBitSet[] getStarTreeValues() {
        return starTreeValues;
    }

    public void setStarTreeValues(LeafReaderContext ctx, FixedBitSet values) {
        if (starTreeValues != null) {
            starTreeValues[ctx.ord] = values;
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
            MetricStat metricStat = validateStarTreeMetricSupport(compositeMappedFieldType, aggregatorFactory);
            if (metricStat == null) {
                return false;
            }
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
    private MetricStat validateStarTreeMetricSupport(
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

            if (field != null && supportedMetrics.containsKey(field) && supportedMetrics.get(field).contains(metricStat)) {
                return metricStat;
            }
        }
        return null;
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

}

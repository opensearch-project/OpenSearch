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
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.metrics.MetricAggregatorFactory;
import org.opensearch.search.internal.SearchContext;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ExperimentalApi
public class StarTreeQueryContext {

    private final CompositeDataCubeFieldType compositeMappedFieldType;

    /**
     * Cache for leaf results
     * This is used to cache the results for each leaf reader context
     * to avoid reading the filtered values from the leaf reader context multiple times
     */
    private final FixedBitSet[] starTreeValues;

    private final QueryBuilder baseQueryBuilder;

    private final Map<String, QueryBuilder> aggSpecificQueryBuilders = new HashMap<>();

    private final Map<String, StarTreeFilter> aggSpecificConsolidatedStarTreeFilters = new HashMap<>();

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

    public CompositeIndexFieldInfo getStarTree() {
        return new CompositeIndexFieldInfo(compositeMappedFieldType.name(), compositeMappedFieldType.getCompositeIndexType());
    }

    public FixedBitSet getStarTreeValues(LeafReaderContext ctx) {
        return starTreeValues != null ? starTreeValues[ctx.ord] : null;
    }

    public void setStarTreeValues(LeafReaderContext ctx, FixedBitSet values) {
        if (starTreeValues != null) {
            starTreeValues[ctx.ord] = values;
        }
    }

    public void registerQuery(QueryBuilder query, AggregatorFactory aggregatorFactory) {
        aggSpecificQueryBuilders.put(getAggregatorFactoryKey(aggregatorFactory), query);
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
        StarTreeFilter baseStarTreeFilter;
        if (baseQueryBuilder == null) {
            baseStarTreeFilter = new StarTreeFilter(Collections.emptyMap());
        } else {
            baseStarTreeFilter = getStarTreeFilter(baseQueryBuilder, compositeMappedFieldType);
            if (baseStarTreeFilter == null) {
                return false; // Base Query is not supported by star tree filter.
            }
        }

        // Generate StarTreeFilter specific to aggregations by merging base and their parents.
        for (String fullAggKey : aggSpecificQueryBuilders.keySet()) {
            StringBuilder currentAggKeyBuilder = new StringBuilder();
            StarTreeFilter mergedStarTreeFilter = baseStarTreeFilter;
            String[] splits = fullAggKey.split(",");
            // Ensure all the parents have their STF consolidated.
            for (int i = 0; i < splits.length - 1; i++) {
                currentAggKeyBuilder.append(splits[i]);
                String currentAggKey = currentAggKeyBuilder.toString();
                if (!aggSpecificConsolidatedStarTreeFilters.containsKey(currentAggKey)) {
                    StarTreeFilter currentStarTreeFilter = getStarTreeFilter(
                        aggSpecificQueryBuilders.get(currentAggKey),
                        compositeMappedFieldType
                    );
                    if (currentStarTreeFilter != null) {
                        boolean res = mergedStarTreeFilter.mergeStarTreeFilter(currentStarTreeFilter);
                        if (res == false) {
                            return false;
                        } // Merging of query builders failed.
                    } else {
                        return false; // Query Builder not supported by star tree.
                    }
                } else {
                    mergedStarTreeFilter = aggSpecificConsolidatedStarTreeFilters.get(currentAggKey);
                }
                currentAggKeyBuilder.append(",");
            }
            StarTreeFilter currentStarTreeFilter = getStarTreeFilter(aggSpecificQueryBuilders.get(fullAggKey), compositeMappedFieldType);
            if (currentStarTreeFilter != null) {
                boolean res = mergedStarTreeFilter.mergeStarTreeFilter(currentStarTreeFilter);
                if (res == false) {
                    return false;
                } // Merging of query builders failed.
            } else {
                return false; // Query Builder not supported by star tree.
            }
            aggSpecificConsolidatedStarTreeFilters.put(fullAggKey, mergedStarTreeFilter);
        }

        return true;
    }

    public StarTreeFilter getBaseQueryStarTreeFilter() {
        if (baseQueryBuilder == null) {
            return new StarTreeFilter(Collections.emptyMap());
        } else {
            return getStarTreeFilter(baseQueryBuilder, compositeMappedFieldType);
        }
    }

    // TODO : Push this validation down to a common method in AggregatorFactory or equivalent place.
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

    private String getAggregatorFactoryKey(AggregatorFactory aggregatorFactory) {
        ArrayDeque<String> orderedAggNames = new ArrayDeque<>();
        AggregatorFactory currentAggregatorFactory = aggregatorFactory;
        while (currentAggregatorFactory != null) {
            orderedAggNames.addFirst(currentAggregatorFactory.name());
            currentAggregatorFactory = currentAggregatorFactory.getParent();
        }
        return String.join(",", orderedAggNames);
    }

    private String getAggregatorKey(Aggregator aggregator) {
        ArrayDeque<String> orderedAggNames = new ArrayDeque<>();
        Aggregator currentAggregator = aggregator;
        while (currentAggregator != null) {
            orderedAggNames.addFirst(currentAggregator.name());
            currentAggregator = currentAggregator.parent();
        }
        return String.join(",", orderedAggNames);
    }

    private StarTreeFilter getStarTreeFilter(QueryBuilder queryBuilder, CompositeDataCubeFieldType compositeMappedFieldType) {
        StarTreeFilterProvider starTreeFilterProvider = StarTreeFilterProvider.SingletonFactory.getProvider(queryBuilder);
        // A query builder's support is not implemented.
        if (starTreeFilterProvider == null) {
            return null;
        }
        return starTreeFilterProvider.getFilter(queryBuilder, compositeMappedFieldType);
    }

}

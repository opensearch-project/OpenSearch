/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class contains all the Counters related to search query types.
 */
public final class SearchQueryCounters {
    private static final String LEVEL_TAG = "level";
    private static final String UNIT = "1";
    private final MetricsRegistry metricsRegistry;
    /**
     * Aggregation counter
     */
    private final Counter aggCounter;
    /**
     * Counter for all other query types (catch all)
     */
    private final Counter otherQueryCounter;
    /**
     * Counter for sort
     */
    private final Counter sortCounter;
    private final Map<Class<? extends QueryBuilder>, Counter> queryHandlers;
    /**
     * Counter name to Counter object map
     */
    private final ConcurrentHashMap<String, Counter> nameToQueryTypeCounters;

    /**
     * Constructor
     * @param metricsRegistry opentelemetry metrics registry
     */
    public SearchQueryCounters(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.nameToQueryTypeCounters = new ConcurrentHashMap<>();
        this.aggCounter = metricsRegistry.createCounter(
            "search.query.type.agg.count",
            "Counter for the number of top level agg search queries",
            UNIT
        );
        this.otherQueryCounter = metricsRegistry.createCounter(
            "search.query.type.other.count",
            "Counter for the number of top level and nested search queries that do not match any other categories",
            UNIT
        );
        this.sortCounter = metricsRegistry.createCounter(
            "search.query.type.sort.count",
            "Counter for the number of top level sort search queries",
            UNIT
        );
        this.queryHandlers = new HashMap<>();

    }

    /**
     * Increment counter
     * @param queryBuilder query builder
     * @param level level of query builder, 0 being highest level
     */
    public void incrementCounter(QueryBuilder queryBuilder, int level) {
        String uniqueQueryCounterName = queryBuilder.getName();

        Counter counter = nameToQueryTypeCounters.computeIfAbsent(uniqueQueryCounterName, k -> createQueryCounter(k));
        counter.add(1, Tags.create().addTag(LEVEL_TAG, level));
    }

    /**
     * Increment aggregate counter
     * @param value value to increment
     * @param tags tags
     */
    public void incrementAggCounter(double value, Tags tags) {
        aggCounter.add(value, tags);
    }

    /**
     * Increment sort counter
     * @param value value to increment
     * @param tags tags
     */
    public void incrementSortCounter(double value, Tags tags) {
        sortCounter.add(value, tags);
    }

    /**
     * Get aggregation counter
     * @return aggregation counter
     */
    public Counter getAggCounter() {
        return aggCounter;
    }

    /**
     * Get sort counter
     * @return sort counter
     */
    public Counter getSortCounter() {
        return sortCounter;
    }

    /**
     * Get counter based on the query builder name
     * @param queryBuilderName query builder name
     * @return counter
     */
    public Counter getCounterByQueryBuilderName(String queryBuilderName) {
        return nameToQueryTypeCounters.get(queryBuilderName);
    }

    private Counter createQueryCounter(String counterName) {
        Counter counter = metricsRegistry.createCounter(
            "search.query.type." + counterName + ".count",
            "Counter for the number of top level and nested " + counterName + " search queries",
            UNIT
        );
        return counter;
    }
}

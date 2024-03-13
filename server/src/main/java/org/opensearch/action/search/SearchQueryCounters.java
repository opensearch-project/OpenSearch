/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

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
final class SearchQueryCounters {
    private static final String LEVEL_TAG = "level";
    private static final String UNIT = "1";
    private final MetricsRegistry metricsRegistry;
    public final Counter aggCounter;
    public final Counter otherQueryCounter;
    public final Counter sortCounter;
    private final Map<Class<? extends QueryBuilder>, Counter> queryHandlers;
    public final ConcurrentHashMap<String, Counter> nameToQueryTypeCounters;

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

    public void incrementCounter(QueryBuilder queryBuilder, int level) {
        String uniqueQueryCounterName = queryBuilder.getName();

        Counter counter = nameToQueryTypeCounters.computeIfAbsent(uniqueQueryCounterName, k -> createQueryCounter(k));
        counter.add(1, Tags.create().addTag(LEVEL_TAG, level));
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;

/**
 * Class contains all the Counters related to search query types.
 */
public class SearchQueryCounters {
    private final MetricsRegistry metricsRegistry;

    // Counters related to Query types
    public final Counter aggCounter;
    public final Counter boolCounter;
    public final Counter functionScoreCounter;
    public final Counter matchCounter;
    public final Counter matchPhrasePrefixCounter;
    public final Counter multiMatchCounter;
    public final Counter otherQueryCounter;
    public final Counter queryStringQueryCounter;
    public final Counter rangeCounter;
    public final Counter regexCounter;

    public final Counter sortCounter;
    public final Counter skippedCounter;
    public final Counter termCounter;
    public final Counter totalCounter;
    public final Counter wildcardCounter;

    public SearchQueryCounters(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.aggCounter = metricsRegistry.createCounter(
            "search.query.type.agg.count",
            "Counter for the number of top level agg search queries",
            "1"
        );
        this.boolCounter = metricsRegistry.createCounter(
            "search.query.type.bool.count",
            "Counter for the number of top level and nested bool search queries",
            "1"
        );
        this.functionScoreCounter = metricsRegistry.createCounter(
            "search.query.type.functionscore.count",
            "Counter for the number of top level and nested function score search queries",
            "1"
        );
        this.matchCounter = metricsRegistry.createCounter(
            "search.query.type.match.count",
            "Counter for the number of top level and nested match search queries",
            "1"
        );
        this.matchPhrasePrefixCounter = metricsRegistry.createCounter(
            "search.query.type.matchphrase.count",
            "Counter for the number of top level and nested match phrase prefix search queries",
            "1"
        );
        this.multiMatchCounter = metricsRegistry.createCounter(
            "search.query.type.multimatch.count",
            "Counter for the number of top level and nested multi match search queries",
            "1"
        );
        this.otherQueryCounter = metricsRegistry.createCounter(
            "search.query.type.other.count",
            "Counter for the number of top level and nested search queries that do not match any other categories",
            "1"
        );
        this.queryStringQueryCounter = metricsRegistry.createCounter(
            "search.query.type.querystringquery.count",
            "Counter for the number of top level and nested queryStringQuery search queries",
            "1"
        );
        this.rangeCounter = metricsRegistry.createCounter(
            "search.query.type.range.count",
            "Counter for the number of top level and nested range search queries",
            "1"
        );
        this.regexCounter = metricsRegistry.createCounter(
            "search.query.type.regex.count",
            "Counter for the number of top level and nested regex search queries",
            "1"
        );
        this.skippedCounter = metricsRegistry.createCounter(
            "search.query.type.skipped.count",
            "Counter for the number queries skipped due to error",
            "1"
        );
        this.sortCounter = metricsRegistry.createCounter(
            "search.query.type.sort.count",
            "Counter for the number of top level sort search queries",
            "1"
        );
        this.termCounter = metricsRegistry.createCounter(
            "search.query.type.term.count",
            "Counter for the number of top level and nested term search queries",
            "1"
        );
        this.totalCounter = metricsRegistry.createCounter(
            "search.query.type.total.count",
            "Counter for the number of top level and nested search queries",
            "1"
        );
        this.wildcardCounter = metricsRegistry.createCounter(
            "search.query.type.wildcard.count",
            "Counter for the number of top level and nested wildcard search queries",
            "1"
        );
    }
}

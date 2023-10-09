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
    public final Counter termCounter;
    public final Counter totalCounter;
    public final Counter wildcardCounter;



    public SearchQueryCounters(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.aggCounter = metricsRegistry.createCounter("aggSearchQueryCounter",
            "Counter for the number of top level and nested agg search queries", "0");
        this.boolCounter = metricsRegistry.createCounter("boolSearchQueryCounter",
            "Counter for the number of top level and nested bool search queries", "0");
        this.functionScoreCounter = metricsRegistry.createCounter("functionScoreSearchQueryCounter",
            "Counter for the number of top level and nested function score search queries", "0");
        this.matchCounter = metricsRegistry.createCounter("matchSearchQueryCounter",
            "Counter for the number of top level and nested match search queries", "0");
        this.matchPhrasePrefixCounter = metricsRegistry.createCounter("matchPhrasePrefixSearchQueryCounter",
            "Counter for the number of top level and nested match phrase prefix search queries", "0");
        this.multiMatchCounter = metricsRegistry.createCounter("multiMatchSearchQueryCounter",
            "Counter for the number of top level and nested multi match search queries", "0");
        this.otherQueryCounter = metricsRegistry.createCounter("otherSearchQueryCounter",
            "Counter for the number of top level and nested search queries that do not match any other categories", "0");
        this.queryStringQueryCounter = metricsRegistry.createCounter("queryStringQuerySearchQueryCounter",
            "Counter for the number of top level and nested queryStringQuery search queries", "0");
        this.rangeCounter = metricsRegistry.createCounter("rangeSearchQueryCounter",
            "Counter for the number of top level and nested range search queries", "0");
        this.regexCounter = metricsRegistry.createCounter("regexSearchQueryCounter",
            "Counter for the number of top level and nested regex search queries", "0");
        this.termCounter = metricsRegistry.createCounter("termSearchQueryCounter",
            "Counter for the number of top level and nested term search queries", "0");
        this.totalCounter = metricsRegistry.createCounter("totalSearchQueryCounter",
            "Counter for the number of top level and nested search queries", "0");
        this.wildcardCounter = metricsRegistry.createCounter("wildcardSearchQueryCounter",
            "Counter for the number of top level and nested wildcard search queries", "0");
    }
}

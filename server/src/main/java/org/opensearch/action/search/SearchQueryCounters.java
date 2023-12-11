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
final class SearchQueryCounters {
    private static final String UNIT = "1";
    private final MetricsRegistry metricsRegistry;

    // Counters related to Query types
    public final Counter aggCounter;
    public final Counter boolCounter;
    public final Counter boostCounter;
    public final Counter constantScoreCounter;
    public final Counter disMaxCounter;
    public final Counter distanceFeatureCounter;
    public final Counter existsCounter;
    public final Counter fieldMaskingSpanCounter;
    public final Counter functionScoreCounter;
    public final Counter fuzzyCounter;
    public final Counter geoBoundingBoxCounter;
    public final Counter geoDistanceCounter;
    public final Counter geoPolygonCounter;
    public final Counter geoShapeCounter;
    public final Counter intervalCounter;
    public final Counter matchCounter;
    public final Counter matchallCounter;
    public final Counter matchPhrasePrefixCounter;
    public final Counter multiMatchCounter;
    public final Counter otherQueryCounter;
    public final Counter prefixCounter;
    public final Counter queryStringQueryCounter;
    public final Counter rangeCounter;
    public final Counter regexCounter;
    public final Counter scriptCounter;
    public final Counter simpleQueryStringCounter;
    public final Counter sortCounter;
    public final Counter skippedCounter;
    public final Counter termCounter;
    public final Counter totalCounter;
    public final Counter wildcardCounter;
    public final Counter numberOfInputFieldsCounter;

    public SearchQueryCounters(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.aggCounter = metricsRegistry.createCounter(
            "search.query.type.agg.count",
            "Counter for the number of top level agg search queries",
            UNIT
        );
        this.boolCounter = metricsRegistry.createCounter(
            "search.query.type.bool.count",
            "Counter for the number of top level and nested bool search queries",
            UNIT
        );
        this.boostCounter = metricsRegistry.createCounter(
            "search.query.type.boast.count",
            "Counter for the number of top level and nested boast search queries",
            UNIT
        );
        this.constantScoreCounter = metricsRegistry.createCounter(
            "search.query.type.counstantscore.count",
            "Counter for the number of top level and nested constant score search queries",
            UNIT
        );
        this.disMaxCounter = metricsRegistry.createCounter(
            "search.query.type.disMax.count",
            "Counter for the number of top level and nested disjuntion max search queries",
            UNIT
        );
        this.distanceFeatureCounter = metricsRegistry.createCounter(
            "search.query.type.distanceFeature.count",
            "Counter for the number of top level and nested distance feature search queries",
            UNIT
        );
        this.existsCounter = metricsRegistry.createCounter(
            "search.query.type.exists.count",
            "Counter for the number of top level and nested exists search queries",
            UNIT
        );
        this.fieldMaskingSpanCounter = metricsRegistry.createCounter(
            "search.query.type.fieldmaskingspan.count",
            "Counter for the number of top level and nested field masking span search queries",
            UNIT
        );
        this.functionScoreCounter = metricsRegistry.createCounter(
            "search.query.type.functionscore.count",
            "Counter for the number of top level and nested function score search queries",
            UNIT
        );
        this.fuzzyCounter = metricsRegistry.createCounter(
            "search.query.type.fuzzy.count",
            "Counter for the number of top level and nested fuzzy search queries",
            UNIT
        );
        this.geoBoundingBoxCounter = metricsRegistry.createCounter(
            "search.query.type.geoboundingbox.count",
            "Counter for the number of top level and nested geo bounding box queries",
            UNIT
        );
        this.geoDistanceCounter = metricsRegistry.createCounter(
            "search.query.type.geodistance.count",
            "Counter for the number of top level and nested geo distance queries",
            UNIT
        );
        this.geoPolygonCounter = metricsRegistry.createCounter(
            "search.query.type.geopolygon.count",
            "Counter for the number of top level and nested geo polygon queries",
            UNIT
        );
        this.geoShapeCounter = metricsRegistry.createCounter(
            "search.query.type.geoshape.count",
            "Counter for the number of top level and nested geo shape queries",
            UNIT
        );
        this.intervalCounter = metricsRegistry.createCounter(
            "search.query.type.interval.count",
            "Counter for the number of top level and nested interval queries",
            UNIT
        );
        this.matchCounter = metricsRegistry.createCounter(
            "search.query.type.match.count",
            "Counter for the number of top level and nested match search queries",
            UNIT
        );
        this.matchallCounter = metricsRegistry.createCounter(
            "search.query.type.matchall.count",
            "Counter for the number of top level and nested match all search queries",
            UNIT
        );
        this.matchPhrasePrefixCounter = metricsRegistry.createCounter(
            "search.query.type.matchphrase.count",
            "Counter for the number of top level and nested match phrase prefix search queries",
            UNIT
        );
        this.multiMatchCounter = metricsRegistry.createCounter(
            "search.query.type.multimatch.count",
            "Counter for the number of top level and nested multi match search queries",
            UNIT
        );
        this.otherQueryCounter = metricsRegistry.createCounter(
            "search.query.type.other.count",
            "Counter for the number of top level and nested search queries that do not match any other categories",
            UNIT
        );
        this.prefixCounter = metricsRegistry.createCounter(
            "search.query.type.prefix.count",
            "Counter for the number of top level and nested search queries that match prefix queries",
            UNIT
        );
        this.queryStringQueryCounter = metricsRegistry.createCounter(
            "search.query.type.querystringquery.count",
            "Counter for the number of top level and nested queryStringQuery search queries",
            UNIT
        );
        this.rangeCounter = metricsRegistry.createCounter(
            "search.query.type.range.count",
            "Counter for the number of top level and nested range search queries",
            UNIT
        );
        this.regexCounter = metricsRegistry.createCounter(
            "search.query.type.regex.count",
            "Counter for the number of top level and nested regex search queries",
            UNIT
        );
        this.scriptCounter = metricsRegistry.createCounter(
            "search.query.type.script.count",
            "Counter for the number of top level and nested script search queries",
            UNIT
        );
        this.simpleQueryStringCounter = metricsRegistry.createCounter(
            "search.query.type.simplequerystring.count",
            "Counter for the number of top level and nested script simple query string search queries",
            UNIT
        );
        this.skippedCounter = metricsRegistry.createCounter(
            "search.query.type.skipped.count",
            "Counter for the number queries skipped due to error",
            UNIT
        );
        this.sortCounter = metricsRegistry.createCounter(
            "search.query.type.sort.count",
            "Counter for the number of top level sort search queries",
            UNIT
        );
        this.termCounter = metricsRegistry.createCounter(
            "search.query.type.term.count",
            "Counter for the number of top level and nested term search queries",
            UNIT
        );
        this.totalCounter = metricsRegistry.createCounter(
            "search.query.type.total.count",
            "Counter for the number of top level and nested search queries",
            UNIT
        );
        this.wildcardCounter = metricsRegistry.createCounter(
            "search.query.type.wildcard.count",
            "Counter for the number of top level and nested wildcard search queries",
            UNIT
        );
        this.numberOfInputFieldsCounter = metricsRegistry.createCounter(
            "search.query.type.numberofinputfields.count",
            "Counter for the number of input fields in the search queries",
            UNIT
        );
    }
}

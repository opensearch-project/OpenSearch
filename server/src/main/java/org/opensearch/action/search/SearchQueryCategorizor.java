/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.QueryShapeVisitor;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.telemetry.metrics.MetricsRegistry;

import java.util.List;

/**
 * Class to categorize the search queries based on the type and increment the relevant counters.
 * Class also logs the query shape.
 */
public class SearchQueryCategorizor {

    private static final Logger log = LogManager.getLogger(SearchQueryCategorizor.class);

    public static SearchQueryCounters searchQueryCounters;

    public SearchQueryCategorizor(MetricsRegistry metricsRegistry) {
        searchQueryCounters = new SearchQueryCounters(metricsRegistry);
    }

    public void categorize(SearchSourceBuilder source) {
        QueryBuilder topLevelQueryBuilder = source.query();

        logQueryShape(topLevelQueryBuilder);
        incrementQueryTypeCounters(topLevelQueryBuilder);
        incrementQueryAggregationCounters(source.aggregations());
        incrementQuerySortCounters(source.sorts());
    }

    private void incrementQuerySortCounters(List<SortBuilder<?>> sorts) {
        if (sorts != null && sorts.size() > 0) {
            searchQueryCounters.sortCounter.add(1);
        }
    }

    private void incrementQueryAggregationCounters(AggregatorFactories.Builder aggregations) {
        if (aggregations != null) {
            searchQueryCounters.aggCounter.add(1);
        }
    }

    private static void incrementQueryTypeCounters(QueryBuilder topLevelQueryBuilder) {
        QueryBuilderVisitor searhQueryVisitor = new SearchQueryCategorizingVisitor(searchQueryCounters);
        topLevelQueryBuilder.visit(searhQueryVisitor);
    }

    private static void logQueryShape(QueryBuilder topLevelQueryBuilder) {
        QueryShapeVisitor shapeVisitor = new QueryShapeVisitor();
        topLevelQueryBuilder.visit(shapeVisitor);
        String queryShapeJson = shapeVisitor.prettyPrintTree("  ");
        log.debug("Query shape : " + queryShapeJson);
    }

}

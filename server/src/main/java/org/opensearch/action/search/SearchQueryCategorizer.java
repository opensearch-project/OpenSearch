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
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.List;
import java.util.ListIterator;

/**
 * Class to categorize the search queries based on the type and increment the relevant counters.
 * Class also logs the query shape.
 */
final class SearchQueryCategorizer {

    private static final Logger log = LogManager.getLogger(SearchQueryCategorizer.class);

    final SearchQueryCounters searchQueryCounters;

    public SearchQueryCategorizer(MetricsRegistry metricsRegistry) {
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
            for (ListIterator<SortBuilder<?>> it = sorts.listIterator(); it.hasNext();) {
                SortBuilder sortBuilder = it.next();
                String sortOrder = sortBuilder.order().toString();
                searchQueryCounters.sortCounter.add(1, Tags.create().addTag("sort_order", sortOrder));
            }
        }
    }

    private void incrementQueryAggregationCounters(AggregatorFactories.Builder aggregations) {
        if (aggregations != null) {
            searchQueryCounters.aggCounter.add(1);
        }
    }

    private void incrementQueryTypeCounters(QueryBuilder topLevelQueryBuilder) {
        if (topLevelQueryBuilder == null) {
            return;
        }
        QueryBuilderVisitor searchQueryVisitor = new SearchQueryCategorizingVisitor(searchQueryCounters);
        topLevelQueryBuilder.visit(searchQueryVisitor);
    }

    private void logQueryShape(QueryBuilder topLevelQueryBuilder) {
        if (topLevelQueryBuilder == null) {
            return;
        }
        QueryShapeVisitor shapeVisitor = new QueryShapeVisitor();
        topLevelQueryBuilder.visit(shapeVisitor);
        log.trace("Query shape : {}", shapeVisitor.prettyPrintTree("  "));
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.List;

/**
 * Class to categorize the search queries based on the type and increment the relevant counters.
 * Class also logs the query shape.
 */
public final class SearchQueryCategorizer {

    private static final Logger log = LogManager.getLogger(SearchQueryCategorizer.class);

    /**
     * Contains all the search query counters
     */
    private final SearchQueryCounters searchQueryCounters;

    final SearchQueryAggregationCategorizer searchQueryAggregationCategorizer;
    private static SearchQueryCategorizer instance;

    /**
     * Constructor for SearchQueryCategorizor
     * @param metricsRegistry opentelemetry metrics registry
     */
    private SearchQueryCategorizer(MetricsRegistry metricsRegistry) {
        searchQueryCounters = new SearchQueryCounters(metricsRegistry);
        searchQueryAggregationCategorizer = new SearchQueryAggregationCategorizer(searchQueryCounters);
    }

    /**
     * Get singleton instance of SearchQueryCategorizer
     * @param metricsRegistry metric registry
     * @return singleton instance
     */
    public static SearchQueryCategorizer getInstance(MetricsRegistry metricsRegistry) {
        if (instance == null) {
            synchronized (SearchQueryCategorizer.class) {
                if (instance == null) {
                    instance = new SearchQueryCategorizer(metricsRegistry);
                }
            }
        }
        return instance;
    }

    /**
     * Consume records and increment counters for the records
     * @param records records to consume
     */
    public void consumeRecords(List<SearchQueryRecord> records) {
        for (SearchQueryRecord record : records) {
            SearchSourceBuilder source = (SearchSourceBuilder) record.getAttributes().get(Attribute.SOURCE);
            categorize(source);
        }
    }

    /**
     * Increment categorizations counters for the given source search query
     * @param source search query source
     */
    public void categorize(SearchSourceBuilder source) {
        QueryBuilder topLevelQueryBuilder = source.query();
        logQueryShape(topLevelQueryBuilder);
        incrementQueryTypeCounters(topLevelQueryBuilder);
        incrementQueryAggregationCounters(source.aggregations());
        incrementQuerySortCounters(source.sorts());
    }

    private void incrementQuerySortCounters(List<SortBuilder<?>> sorts) {
        if (sorts != null && sorts.size() > 0) {
            for (SortBuilder<?> sortBuilder : sorts) {
                String sortOrder = sortBuilder.order().toString();
                searchQueryCounters.incrementSortCounter(1, Tags.create().addTag("sort_order", sortOrder));
            }
        }
    }

    private void incrementQueryAggregationCounters(AggregatorFactories.Builder aggregations) {
        if (aggregations == null) {
            return;
        }

        searchQueryAggregationCategorizer.incrementSearchQueryAggregationCounters(aggregations.getAggregatorFactories());
    }

    private void incrementQueryTypeCounters(QueryBuilder topLevelQueryBuilder) {
        if (topLevelQueryBuilder == null) {
            return;
        }
        QueryBuilderVisitor searchQueryVisitor = new SearchQueryCategorizingVisitor(searchQueryCounters);
        topLevelQueryBuilder.visit(searchQueryVisitor);
    }

    private void logQueryShape(QueryBuilder topLevelQueryBuilder) {
        if (log.isTraceEnabled()) {
            if (topLevelQueryBuilder == null) {
                return;
            }
            QueryShapeVisitor shapeVisitor = new QueryShapeVisitor();
            topLevelQueryBuilder.visit(shapeVisitor);
            log.trace("Query shape : {}", shapeVisitor.prettyPrintTree("  "));
        }
    }

    /**
     * Get search query counters
     * @return search query counters
     */
    public SearchQueryCounters getSearchQueryCounters() {
        return this.searchQueryCounters;
    }
}

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
import org.apache.lucene.search.BooleanClause;
import org.opensearch.identity.IdentityService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.IntervalsSourceProvider;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.QueryShapeVisitor;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.telemetry.tracing.attributes.Attributes;

public class SearchQueryCategorizor {

    private static final Logger log = LogManager.getLogger(SearchQueryCategorizor.class);

    public static SearchQueryCounters searchQueryCounters = new SearchQueryCounters(); // What metrics registry to use here?

    public static void categorize(SearchSourceBuilder source) {
        QueryBuilder topLevelQueryBuilder = source.query();

        // Get and log the query shape
        QueryShapeVisitor shapeVisitor = new QueryShapeVisitor();
        topLevelQueryBuilder.visit(shapeVisitor, 0);
        String queryShapeJson = shapeVisitor.prettyPrintTree("  ");
        log.info("Query shape : " + queryShapeJson);

        // Increment the query counters using Metric Framework
        QueryBuilderVisitor queryBuilderVisitor = new QueryBuilderVisitor() {
            @Override
            public void accept(QueryBuilder qb, int level) {
                // This method will be called for every QueryBuilder node in the tree.
                // The tree referred to here is the tree of querybuilders for the incoming search
                // query with the topLevelQueryBuilder as the root.

                // Increment counter for current QueryBuilder using Metric Framework.
                if (qb instanceof AggregationQ) {
                    searchQueryCounters.aggCounter.add(1, Attributes.create().addAttribute("level", level));
                } else if (qb instanceof BoolQueryBuilder) {
                    searchQueryCounters.boolCounter.add(1, Attributes.create().addAttribute("level", level));
                } else if (qb instanceof FunctionScoreQueryBuilder) {
                    searchQueryCounters.functionScoreCounter.add(1, Attributes.create().addAttribute("level", level));
                } else if (qb instanceof MatchQueryBuilder) {
                    searchQueryCounters.matchCounter.add(1, Attributes.create().addAttribute("level", level));
                } else if (qb instanceof MatchPhraseQueryBuilder) {
                    searchQueryCounters.matchPhrasePrefixCounter.add(1, Attributes.create().addAttribute("level", level));
                } else if (qb instanceof MultiMatchQueryBuilder) {
                    searchQueryCounters.multiMatchCounter.add(1, Attributes.create().addAttribute("level", level));
                } else if (qb instanceof QueryStringQueryBuilder) {
                    searchQueryCounters.queryStringQueryCounter.add(1, Attributes.create().addAttribute("level", level));
                } else if (qb instanceof RangeQueryBuilder) {
                    searchQueryCounters.rangeCounter.add(1, Attributes.create().addAttribute("level", level));
                } else if (qb instanceof RegexpQueryBuilder) {
                    searchQueryCounters.regexCounter.add(1, Attributes.create().addAttribute("level", level));
                } else if (qb instanceof TermQueryBuilder) {
                    searchQueryCounters.termCounter.add(1, Attributes.create().addAttribute("level", level));
                } else if (qb instanceof WildcardQueryBuilder) {
                    searchQueryCounters.wildcardCounter.add(1, Attributes.create().addAttribute("level", level));
                } else {
                    searchQueryCounters.otherQueryCounter.add(1, Attributes.create().addAttribute("level", level));
                }
            }

            @Override
            public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
                return this;
            }
        };
        topLevelQueryBuilder.visit(queryBuilderVisitor, 0);
    }

}

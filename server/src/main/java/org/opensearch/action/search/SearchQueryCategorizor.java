/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

public class SearchQueryCategorizor {

    public static void categorize(SearchSourceBuilder source) {
        QueryBuilder topLevelQueryBuilder = source.query();
        QueryBuilderVisitor queryBuilderVisitor = new QueryBuilderVisitor() {
            @Override
            public void accept(QueryBuilder qb) {
                // This method will be called for every QueryBuilder node in the tree.
                // The tree referred to here is the tree of querybuilders for the incoming search
                // query with the topLevelQueryBuilder as the root.

                // Increment counter for current QueryBuilder using Metric Framework.
                if (qb instanceof BoolQueryBuilder) {
                    // Increment counter for Bool using Metric Framework.
                } else if (qb instanceof MatchQueryBuilder) {
                    // Increment counter for Match using Metric Framework.
                } else if (qb instanceof QueryStringQueryBuilder) {
                    // Increment counter for QueryStringQuery using Metric Framework.
                }
                // Similar for other builders
            }

            @Override
            public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
                return this;
            }
        };
        topLevelQueryBuilder.visit(queryBuilderVisitor);
    }

}

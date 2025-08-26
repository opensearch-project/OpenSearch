/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.query.rewriters.BooleanFlatteningRewriter;
import org.opensearch.search.query.rewriters.MatchAllRemovalRewriter;
import org.opensearch.search.query.rewriters.MustNotToShouldRewriter;
import org.opensearch.search.query.rewriters.MustToFilterRewriter;
import org.opensearch.search.query.rewriters.TermsMergingRewriter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Registry for query rewriters.
 *
 * @opensearch.internal
 */
public final class QueryRewriterRegistry {

    private static final Logger logger = LogManager.getLogger(QueryRewriterRegistry.class);

    private QueryRewriterRegistry() {}

    public static QueryBuilder rewrite(QueryBuilder query, QueryShardContext context, boolean enabled) {
        return rewrite(query, context, enabled, 16);
    }

    public static QueryBuilder rewrite(QueryBuilder query, QueryShardContext context, boolean enabled, int termsThreshold) {
        if (!enabled || query == null) {
            return query;
        }

        // Create rewriters with the current threshold
        List<QueryRewriter> currentRewriters = new ArrayList<>();
        currentRewriters.add(new BooleanFlatteningRewriter());
        currentRewriters.add(new MustToFilterRewriter());
        currentRewriters.add(new MustNotToShouldRewriter());
        currentRewriters.add(new TermsMergingRewriter(termsThreshold));
        currentRewriters.add(new MatchAllRemovalRewriter());
        currentRewriters.sort(Comparator.comparingInt(QueryRewriter::priority));

        QueryBuilder current = query;
        for (QueryRewriter rewriter : currentRewriters) {
            try {
                QueryBuilder rewritten = rewriter.rewrite(current, context);
                if (rewritten != current) {
                    current = rewritten;
                }
            } catch (Exception e) {
                logger.warn("Query rewriter {} failed: {}", rewriter.name(), e.getMessage());
            }
        }

        return current;
    }
}

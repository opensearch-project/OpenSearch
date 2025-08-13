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
    private static final List<QueryRewriter> REWRITERS = new ArrayList<>();

    static {
        REWRITERS.add(new BooleanFlatteningRewriter());
        REWRITERS.add(new TermsMergingRewriter());
        REWRITERS.add(new MatchAllRemovalRewriter());
        REWRITERS.sort(Comparator.comparingInt(QueryRewriter::priority));
    }

    private QueryRewriterRegistry() {}

    public static QueryBuilder rewrite(QueryBuilder query, QueryShardContext context, boolean enabled) {
        if (!enabled || query == null) {
            return query;
        }

        QueryBuilder current = query;
        for (QueryRewriter rewriter : REWRITERS) {
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

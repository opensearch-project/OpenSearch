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
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry for query rewriters
 *
 * @opensearch.internal
 */
public final class QueryRewriterRegistry {

    private static final Logger logger = LogManager.getLogger(QueryRewriterRegistry.class);

    private static final QueryRewriterRegistry INSTANCE = new QueryRewriterRegistry();

    /**
     * Default rewriters.
     * CopyOnWriteArrayList is used for thread-safety during registration.
     */
    private final CopyOnWriteArrayList<QueryRewriter> rewriters;

    private QueryRewriterRegistry() {
        this.rewriters = new CopyOnWriteArrayList<>();

        // Register default rewriters
        // Note: TermsMergingRewriter is special - it needs threshold at runtime
        registerRewriter(new BooleanFlatteningRewriter());
        registerRewriter(new MustToFilterRewriter());
        registerRewriter(new MustNotToShouldRewriter());
        registerRewriter(new MatchAllRemovalRewriter());
    }

    /**
     * Get the singleton instance of the registry.
     */
    public static QueryRewriterRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Register a custom query rewriter.
     *
     * @param rewriter The rewriter to register
     */
    public void registerRewriter(QueryRewriter rewriter) {
        if (rewriter != null) {
            rewriters.add(rewriter);
            logger.info("Registered query rewriter: {}", rewriter.name());
        }
    }

    /**
     * Get a list of all rewriters with the given terms threshold.
     */
    private List<QueryRewriter> getRewritersWithThreshold(int termsThreshold) {
        List<QueryRewriter> allRewriters = new ArrayList<>(rewriters);
        // Add TermsMergingRewriter with the current threshold
        // This is added dynamically because it needs the threshold parameter
        allRewriters.add(new TermsMergingRewriter(termsThreshold));
        allRewriters.sort(Comparator.comparingInt(QueryRewriter::priority));
        return allRewriters;
    }

    public static QueryBuilder rewrite(QueryBuilder query, QueryShardContext context, boolean enabled) {
        return rewrite(query, context, enabled, 16);
    }

    public static QueryBuilder rewrite(QueryBuilder query, QueryShardContext context, boolean enabled, int termsThreshold) {
        if (!enabled || query == null) {
            return query;
        }

        QueryRewriterRegistry registry = getInstance();
        List<QueryRewriter> sortedRewriters = registry.getRewritersWithThreshold(termsThreshold);

        QueryBuilder current = query;
        for (QueryRewriter rewriter : sortedRewriters) {
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

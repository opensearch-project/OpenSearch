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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
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

    /**
     * TermsMergingRewriter instance that needs settings initialization.
     */
    private final TermsMergingRewriter termsMergingRewriter;

    private QueryRewriterRegistry() {
        this.rewriters = new CopyOnWriteArrayList<>();

        // Register default rewriters
        registerRewriter(new BooleanFlatteningRewriter());
        registerRewriter(new MustToFilterRewriter());
        registerRewriter(new MustNotToShouldRewriter());
        registerRewriter(new MatchAllRemovalRewriter());

        // TermsMergingRewriter is registered as singleton
        this.termsMergingRewriter = new TermsMergingRewriter();
        registerRewriter(termsMergingRewriter);
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
     * Initialize the registry with cluster settings.
     * This must be called once during system startup to properly configure
     * the TermsMergingRewriter with settings and update consumers.
     *
     * @param settings Initial cluster settings
     * @param clusterSettings Cluster settings for registering update consumers
     */
    public static void initialize(Settings settings, ClusterSettings clusterSettings) {
        getInstance().termsMergingRewriter.initialize(settings, clusterSettings);
    }

    public static QueryBuilder rewrite(QueryBuilder query, QueryShardContext context, boolean enabled) {
        if (!enabled || query == null) {
            return query;
        }

        QueryRewriterRegistry registry = getInstance();
        List<QueryRewriter> sortedRewriters = new ArrayList<>(registry.rewriters);
        sortedRewriters.sort(Comparator.comparingInt(QueryRewriter::priority));

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

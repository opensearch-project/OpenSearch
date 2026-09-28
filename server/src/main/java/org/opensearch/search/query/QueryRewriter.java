/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

/**
 * Interface for query rewriting implementations that optimize query structure
 * before conversion to Lucene queries.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface QueryRewriter {

    /**
     * Rewrites the given query builder to a more optimal form.
     *
     * @param query The query to rewrite
     * @param context The search execution context
     * @return The rewritten query (may be the same instance if no rewrite needed)
     */
    QueryBuilder rewrite(QueryBuilder query, QueryShardContext context);

    /**
     * Returns the priority of this rewriter. Lower values execute first.
     * This allows control over rewrite ordering when multiple rewriters
     * may interact.
     *
     * @return The priority value
     */
    default int priority() {
        return 1000;
    }

    /**
     * Returns the name of this rewriter for debugging and profiling.
     *
     * @return The rewriter name
     */
    String name();
}

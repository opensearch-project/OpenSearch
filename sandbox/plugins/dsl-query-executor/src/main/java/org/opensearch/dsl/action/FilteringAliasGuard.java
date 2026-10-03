/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;

import java.util.List;
import java.util.Set;

/**
 * Coordinator-level guard that rejects DSL requests whose named expressions resolve to a filtering
 * alias.
 *
 * <p>The analytics engine cannot honor alias filters, and vanilla resolution silently drops the
 * filter for hidden aliases reached via {@code expand_wildcards=open,hidden} — a confirmed tenant
 * leak. This guard fails loud (HTTP 400) instead, an intentional documented divergence. It is
 * shared by both {@link TransportExecuteAction} and {@link TransportValidateAction} so the two
 * paths reject identically.
 */
final class FilteringAliasGuard {

    private FilteringAliasGuard() {}

    /**
     * Rejects the request if any resolved concrete index is reached through a filtering alias.
     *
     * @param indicesOptions forwarded so hidden filtered aliases (expand_wildcards=open,hidden) are visible
     * @throws IllegalArgumentException if a filtering alias is detected (surfaces as HTTP 400)
     */
    static void check(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState state,
        String[] requestIndices,
        IndicesOptions indicesOptions,
        List<IndexMetadata> concreteIndices
    ) {
        // Build the resolved-expression set with the request's real options so hidden filtered aliases
        // reached via expand_wildcards=open,hidden are included — the lenient default would drop them.
        Set<String> resolvedExpressions = indexNameExpressionResolver.resolveExpressions(state, indicesOptions, requestIndices);
        for (IndexMetadata concreteIndex : concreteIndices) {
            String concreteIndexName = concreteIndex.getIndex().getName();
            String[] filteringAliases = indexNameExpressionResolver.filteringAliases(state, concreteIndexName, resolvedExpressions);
            if (filteringAliases != null && filteringAliases.length > 0) {
                throw new IllegalArgumentException(
                    "Alias ["
                        + filteringAliases[0]
                        + "] declares a filter on index ["
                        + concreteIndexName
                        + "]; filter aliases are not yet supported by analytics queries"
                );
            }
        }
    }
}

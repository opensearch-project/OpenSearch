/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.IndicesRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;

import java.util.List;

/**
 * Resolves an indices-bearing request (which may carry aliases or wildcards) to the concrete indices
 * the DSL Calcite path will operate on. The implementation owns the cardinality policy:
 * {@link SingleIndexResolutionStrategy} requires exactly one concrete index (the constraint today),
 * while a future multi-index strategy may return several. Transport actions call a single
 * {@link #resolve} and leave the policy to the strategy.
 */
interface IndexResolutionStrategy {

    /**
     * Resolves the request's indices to the concrete indices this strategy permits.
     *
     * @param indexNameExpressionResolver resolves aliases and wildcards to concrete indices
     * @param state cluster-state snapshot to resolve against
     * @param request the indices-bearing request (e.g. a {@code SearchRequest} or {@code ValidateQueryRequest})
     * @return the resolved indices' metadata (never empty)
     * @throws IllegalArgumentException if the resolved indices violate the strategy's cardinality policy
     */
    List<IndexMetadata> resolve(IndexNameExpressionResolver indexNameExpressionResolver, ClusterState state, IndicesRequest request);
}

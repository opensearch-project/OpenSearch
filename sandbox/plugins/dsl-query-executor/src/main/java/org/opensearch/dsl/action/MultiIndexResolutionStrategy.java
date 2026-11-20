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
import org.opensearch.core.index.Index;

import java.util.Arrays;
import java.util.List;

/**
 * {@link IndexResolutionStrategy} that accepts one or more concrete indices. The analytics
 * engine resolves aliases and index expressions as one logical table and executes the query
 * across all backing shards.
 */
final class MultiIndexResolutionStrategy implements IndexResolutionStrategy {

    @Override
    public List<IndexMetadata> resolve(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState state,
        IndicesRequest request
    ) {
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices.length == 0) {
            throw new IllegalArgumentException("DSL search requires at least one concrete index");
        }
        return Arrays.stream(concreteIndices).map(state.metadata()::getIndexSafe).toList();
    }
}

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

import java.util.List;

/**
 * {@link IndexResolutionStrategy} that requires the request to resolve to exactly one concrete
 * index — the single-index constraint of the DSL Calcite path today. Resolving to zero or more
 * than one concrete index is rejected.
 */
final class SingleIndexResolutionStrategy implements IndexResolutionStrategy {

    @Override
    public List<IndexMetadata> resolve(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState state,
        IndicesRequest request
    ) {
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices.length != 1) {
            throw new IllegalArgumentException(
                "DSL currently supports exactly one concrete index, but resolved to " + concreteIndices.length + " indices"
            );
        }
        return List.of(state.metadata().getIndexSafe(concreteIndices[0]));
    }
}

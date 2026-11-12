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

import java.util.ArrayList;
import java.util.List;

/**
 * {@link IndexResolutionStrategy} that returns every concrete index the request resolves to,
 * preserving resolution order. At one concrete index it behaves identically to
 * {@link SingleIndexResolutionStrategy}; it imposes no upper bound on cardinality but keeps the
 * never-empty contract — an empty resolution still surfaces as {@code IndexNotFoundException} from
 * {@link IndexNameExpressionResolver#concreteIndices}.
 */
final class MultiIndexResolutionStrategy implements IndexResolutionStrategy {

    @Override
    public List<IndexMetadata> resolve(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState state,
        IndicesRequest request
    ) {
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        List<IndexMetadata> resolved = new ArrayList<>(concreteIndices.length);
        for (Index concreteIndex : concreteIndices) {
            resolved.add(state.metadata().getIndexSafe(concreteIndex));
        }
        return resolved;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.List;
import java.util.Optional;

/**
 * Factory for creating {@link InstructionNode}s at the coordinator and
 * {@link FragmentInstructionHandler}s at the data node. One factory per backend,
 * accessed via {@code AnalyticsSearchBackendPlugin.getInstructionHandlerFactory()}.
 *
 * <p>Coordinator-side creation methods return {@link Optional#empty()} if the backend
 * does not support the instruction type. Core logs and skips unsupported instructions.
 *
 * @opensearch.internal
 */
public interface FragmentInstructionHandlerFactory {

    // ── Coordinator-side: create instruction nodes ──

    /** Creates a shard scan instruction node. */
    Optional<InstructionNode> createShardScanNode();

    /** Creates a filter delegation instruction node with the given delegation metadata. */
    Optional<InstructionNode> createFilterDelegationNode(
        FilterTreeShape treeShape,
        int delegatedPredicateCount,
        List<DelegatedExpression> delegatedQueries
    );

    /** Creates a shard scan with delegation instruction node — combines scan setup with delegation config. */
    Optional<InstructionNode> createShardScanWithDelegationNode(FilterTreeShape treeShape, int delegatedPredicateCount);

    /** Creates a partial aggregate instruction node. */
    Optional<InstructionNode> createPartialAggregateNode();

    /** Creates a final aggregate instruction node for coordinator reduce. */
    Optional<InstructionNode> createFinalAggregateNode();

    // ── Data-node-side: create handler for an instruction node ──

    /**
     * Creates a handler for the given instruction node. The handler's
     * {@link FragmentInstructionHandler#apply} will be called with the node
     * and the execution context.
     */
    FragmentInstructionHandler<?> createHandler(InstructionNode node);
}

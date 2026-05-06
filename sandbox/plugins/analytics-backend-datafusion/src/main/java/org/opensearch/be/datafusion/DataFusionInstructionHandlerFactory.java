/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.FilterDelegationInstructionNode;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FinalAggregateInstructionNode;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;

import java.util.List;
import java.util.Optional;

/**
 * DataFusion backend's instruction handler factory.
 *
 * <p>Coordinator side: creates typed instruction nodes for wire transport.
 * <p>Data node side: creates handlers that call into Rust via FFM to configure the SessionContext.
 */
public class DataFusionInstructionHandlerFactory implements FragmentInstructionHandlerFactory {

    private final DataFusionPlugin plugin;

    public DataFusionInstructionHandlerFactory(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    // ── Coordinator: create instruction nodes ──

    @Override
    public Optional<InstructionNode> createShardScanNode() {
        return Optional.of(new ShardScanInstructionNode());
    }

    @Override
    public Optional<InstructionNode> createFilterDelegationNode(
        FilterTreeShape treeShape,
        int delegatedPredicateCount,
        List<DelegatedExpression> delegatedExpressions
    ) {
        return Optional.of(new FilterDelegationInstructionNode(treeShape, delegatedPredicateCount, delegatedExpressions));
    }

    @Override
    public Optional<InstructionNode> createPartialAggregateNode() {
        // TODO: return Optional.of(...) once PartialAggregateInstructionHandler is implemented
        return Optional.empty();
    }

    @Override
    public Optional<InstructionNode> createFinalAggregateNode() {
        return Optional.of(new FinalAggregateInstructionNode());
    }

    // ── Data node: create handlers ──

    @SuppressWarnings("unchecked")
    @Override
    public FragmentInstructionHandler<?> createHandler(InstructionNode node) {
        if (node instanceof ShardScanWithDelegationInstructionNode) {
            return new ShardScanWithDelegationHandler(plugin);
        }
        if (node instanceof ShardScanInstructionNode) {
            return new ShardScanInstructionHandler(plugin);
        }
        if (node instanceof FinalAggregateInstructionNode) {
            return new FinalAggregateInstructionHandler();
        }
        // TODO: FilterDelegationInstructionHandler, PartialAggregateInstructionHandler
        throw new UnsupportedOperationException("No handler for instruction type: " + node.type());
    }
}

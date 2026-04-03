/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeWriter;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleReader;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Builds a {@link QueryDAG} from the CBO output by cutting at exchange boundaries.
 *
 * <p>SINGLETON: {@link OpenSearchExchangeReducer} is the boundary. Parent
 * fragment is everything above the reducer (may be null for a pure gather).
 * Child fragment is the reducer's input subtree.
 *
 * <p>HASH/RANGE: {@link OpenSearchShuffleReader} → {@link OpenSearchExchangeWriter}.
 * ShuffleReader stays in parent as leaf (input severed). Writer + subtree
 * below becomes the child fragment.
 *
 * <p>Stage IDs assigned bottom-up (leaf stages get lower IDs).
 *
 * @opensearch.internal
 */
public class DAGBuilder {

    private DAGBuilder() {}

    public static QueryDAG build(RelNode cboOutput) {
        int[] counter = { 0 };
        List<Stage> childStages = new ArrayList<>();

        RelNode fragment;
        if (cboOutput instanceof OpenSearchExchangeReducer reducer) {
            // Root is an ExchangeReducer (e.g., shuffle case where coordinator
            // is a pure gather). ExchangeReducer becomes the root stage fragment
            // with its input severed. Child stage is the subtree below.
            fragment = cutSingleton(reducer, counter, childStages);
        } else {
            fragment = sever(cboOutput, counter, childStages);
        }

        Stage rootStage = new Stage(counter[0]++, fragment, childStages, null);
        return new QueryDAG(UUID.randomUUID().toString(), rootStage);
    }

    /**
     * Walks top-down collecting operators into the current stage. When an
     * exchange boundary is hit, severs the link: subtree below becomes a
     * child stage, current stage continues with the boundary removed or
     * replaced by a detached leaf.
     *
     * <p>Exchange boundaries are detected at the input level (not the node
     * level) so the parent node never receives a null input. For SINGLETON
     * cuts the input is simply dropped; for shuffle cuts the ShuffleReader
     * replaces the input as a detached leaf.
     *
     * @param node        current node being visited
     * @param counter     stage ID counter (bottom-up assignment)
     * @param childStages accumulator for child stages found below exchanges
     * @return the rewritten fragment root for the current stage
     */
    private static RelNode sever(RelNode node, int[] counter, List<Stage> childStages) {
        // Check each input for exchange boundaries before recursing
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            if (input instanceof OpenSearchExchangeReducer reducer) {
                // SINGLETON cut: ExchangeReducer stays in parent as leaf (input severed).
                // Child stage is the reducer's input subtree. Analytics Core streams
                // results from data nodes — no exchange operator needed in child fragment.
                newInputs.add(cutSingleton(reducer, counter, childStages));
            } else if (input instanceof OpenSearchShuffleReader reader) {
                // Shuffle cut: ShuffleReader stays in parent as leaf (input severed).
                // Child stage is ExchangeWriter + subtree below.
                newInputs.add(cutShuffle(reader, counter, childStages));
            } else {
                newInputs.add(sever(input, counter, childStages));
            }
        }

        // Leaf node (e.g., TableScan) — no inputs to process
        if (node.getInputs().isEmpty()) {
            return node;
        }

        // Rebuild only if inputs changed
        boolean changed = false;
        for (int idx = 0; idx < newInputs.size(); idx++) {
            if (newInputs.get(idx) != node.getInputs().get(idx)) {
                changed = true;
                break;
            }
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static RelNode cutSingleton(OpenSearchExchangeReducer reducer,
                                        int[] counter, List<Stage> parentChildStages) {
        List<Stage> grandchildren = new ArrayList<>();
        RelNode childFragment = sever(reducer.getInput(), counter, grandchildren);

        int childStageId = counter[0]++;
        parentChildStages.add(new Stage(
            childStageId, childFragment, grandchildren,
            new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of())
        ));

        // Replace child subtree with StageInputScan, keep ExchangeReducer in parent
        RelDataType childRowType = reducer.getInput().getRowType();
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            reducer.getCluster(), reducer.getTraitSet(), childStageId, childRowType
        );
        return new OpenSearchExchangeReducer(
            reducer.getCluster(), reducer.getTraitSet(), stageInput, reducer.getViableBackends()
        );
    }

    private static RelNode cutShuffle(OpenSearchShuffleReader reader,
                                     int[] counter, List<Stage> parentChildStages) {
        if (!(reader.getInput() instanceof OpenSearchExchangeWriter writer)) {
            throw new IllegalStateException(
                "ShuffleReader input must be ExchangeWriter, got: "
                    + reader.getInput().getClass().getSimpleName());
        }

        List<Stage> grandchildren = new ArrayList<>();
        RelNode belowWriter = sever(writer.getInput(), counter, grandchildren);
        RelNode childFragment = writer.copy(writer.getTraitSet(), List.of(belowWriter));

        int childStageId = counter[0]++;
        parentChildStages.add(new Stage(
            childStageId, childFragment, grandchildren,
            new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, writer.getShuffleImpl(), writer.getKeys())
        ));

        // Replace child subtree with StageInputScan, keep ShuffleReader in parent
        RelDataType childRowType = writer.getInput().getRowType();
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            reader.getCluster(), reader.getTraitSet(), childStageId, childRowType
        );
        return new OpenSearchShuffleReader(
            reader.getCluster(), reader.getTraitSet(), stageInput,
            reader.getViableBackends(), reader.getShuffleImpl()
        );
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Detects the N-input coordinator-fragment shape where every child of an N-input
 * operator is an {@link OpenSearchExchangeReducer} wrapping an
 * {@link OpenSearchStageInputScan}. This is the shape produced when a rule wraps
 * each branch of a multi-input operator (Union, coord-side Join, future set-ops)
 * in an ExchangeReducer so {@code DAGBuilder} cuts a separate child stage per branch.
 *
 * <p>Used by {@link FragmentConversionDriver} to distinguish a multi-input
 * coordinator fragment (Union / Join above per-branch child stages) from a
 * single-input fragment (final aggregate / sort above a partial-agg child stage).
 *
 * @opensearch.internal
 */
public final class MultiInputShape {

    private final List<OpenSearchStageInputScan> branchScans;

    private MultiInputShape(List<OpenSearchStageInputScan> branchScans) {
        this.branchScans = branchScans;
    }

    /**
     * Returns {@code Optional.of(shape)} if {@code node} has at least two inputs and
     * every input is an {@link OpenSearchExchangeReducer} whose own input is an
     * {@link OpenSearchStageInputScan}. Returns {@code Optional.empty()} otherwise.
     *
     * <p>The branch {@link OpenSearchStageInputScan}s are returned in the same order
     * as the parent's inputs — callers that care about column ordering (Union, Join)
     * can rely on positional correspondence with the parent operator's input list.
     */
    public static Optional<MultiInputShape> detect(RelNode node) {
        List<RelNode> inputs = node.getInputs();
        if (inputs.size() < 2) {
            return Optional.empty();
        }
        List<OpenSearchStageInputScan> scans = new ArrayList<>(inputs.size());
        for (RelNode input : inputs) {
            if (!(input instanceof OpenSearchExchangeReducer reducer)) {
                return Optional.empty();
            }
            if (!(reducer.getInput() instanceof OpenSearchStageInputScan scan)) {
                return Optional.empty();
            }
            scans.add(scan);
        }
        return Optional.of(new MultiInputShape(scans));
    }

    /**
     * The {@link OpenSearchStageInputScan} under each branch's ExchangeReducer, in input order.
     */
    public List<OpenSearchStageInputScan> getBranchScans() {
        return branchScans;
    }
}

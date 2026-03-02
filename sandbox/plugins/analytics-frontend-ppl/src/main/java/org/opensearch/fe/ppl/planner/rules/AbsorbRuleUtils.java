/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.ppl.planner.rules;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.opensearch.fe.ppl.planner.rel.OpenSearchBoundaryTableScan;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared utilities for absorb rules that need to walk down a subtree
 * to find an {@link OpenSearchBoundaryTableScan} and replace it with
 * its logical fragment.
 */
final class AbsorbRuleUtils {

    private AbsorbRuleUtils() {}

    /** Unwraps HepRelVertex wrappers that HepPlanner uses internally. */
    static RelNode unwrap(RelNode node) {
        if (node instanceof HepRelVertex) {
            return ((HepRelVertex) node).getCurrentRel();
        }
        return node;
    }

    /**
     * Walks down single-input chains to find an OpenSearchBoundaryTableScan.
     */
    static OpenSearchBoundaryTableScan findBoundary(RelNode node) {
        for (RelNode rawInput : node.getInputs()) {
            RelNode input = unwrap(rawInput);
            if (input instanceof OpenSearchBoundaryTableScan) {
                return (OpenSearchBoundaryTableScan) input;
            }
            if (input.getInputs().size() == 1) {
                OpenSearchBoundaryTableScan found = findBoundary(input);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    /**
     * Recursively copies the subtree from {@code node} down, replacing any
     * boundary node with its logical fragment.
     */
    static RelNode replaceWithFragment(RelNode node) {
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode rawInput : node.getInputs()) {
            RelNode input = unwrap(rawInput);
            if (input instanceof OpenSearchBoundaryTableScan) {
                newInputs.add(((OpenSearchBoundaryTableScan) input).getLogicalFragment());
            } else {
                newInputs.add(replaceWithFragment(input));
            }
        }
        return node.copy(node.getTraitSet(), newInputs);
    }

    /**
     * Absorbs the operator (and all intermediate nodes) into the boundary,
     * returning a new boundary node with the expanded fragment.
     */
    static OpenSearchBoundaryTableScan absorbIntoBoundary(RelNode operator, OpenSearchBoundaryTableScan boundary) {
        RelNode absorbed = replaceWithFragment(operator);
        return new OpenSearchBoundaryTableScan(
            boundary.getCluster(),
            boundary.getTraitSet(),
            boundary.getTable(),
            absorbed,
            boundary.getEngineExecutor()
        );
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.InstructionType;

import java.util.ArrayList;
import java.util.List;

/**
 * Ordering invariant for a stage's instruction chain.
 *
 * <p>Instructions split into two kinds. Most only REGISTER something on the backend session — a scanned
 * shard, an injected broadcast payload, a shuffle partition's streaming table. An aggregate-preparing
 * instruction is different: it BUILDS A PHYSICAL PLAN from the fragment bytes, which resolves every table
 * the fragment names. So it can only run once all registration is done.
 *
 * <p>Several independent steps append to the chain (fragment conversion, broadcast injection, shuffle
 * enrichment), and each one appending at the end is enough to land a preparation ahead of a registration.
 * The failure is a planning error naming the table that had not been registered yet, which reads like a
 * missing-input bug rather than an ordering one. Routing every chain through
 * {@link #aggregatePreparationLast} keeps the invariant in one place instead of relying on each appender to
 * pick the right index.
 *
 * @opensearch.internal
 */
public final class InstructionOrdering {

    private InstructionOrdering() {}

    /**
     * Returns {@code instructions} with any aggregate-preparing instruction moved to the end, preserving the
     * relative order within both groups. Returns the input unchanged when there is nothing to move, so
     * callers can apply it unconditionally.
     */
    public static List<InstructionNode> aggregatePreparationLast(List<InstructionNode> instructions) {
        int lastPrepares = -1;
        boolean needsMove = false;
        for (int i = 0; i < instructions.size(); i++) {
            if (prepares(instructions.get(i))) {
                lastPrepares = i;
            } else if (lastPrepares >= 0) {
                // A registration sits after a preparation — the chain is out of order.
                needsMove = true;
            }
        }
        if (!needsMove) {
            return instructions;
        }
        List<InstructionNode> registers = new ArrayList<>(instructions.size());
        List<InstructionNode> prepares = new ArrayList<>(2);
        for (InstructionNode node : instructions) {
            if (prepares(node)) {
                prepares.add(node);
            } else {
                registers.add(node);
            }
        }
        registers.addAll(prepares);
        return registers;
    }

    /** True when {@code node} builds a physical plan from the fragment bytes and so needs its tables present. */
    private static boolean prepares(InstructionNode node) {
        return node.type() == InstructionType.SETUP_PARTIAL_AGGREGATE || node.type() == InstructionType.SETUP_FINAL_AGGREGATE;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.InstructionNode;

import java.util.List;

/**
 * A single plan alternative for a {@link Stage}. Contains a resolved fragment
 * where every operator's viableBackends and every annotation's viableBackends
 * are narrowed to exactly one backend, plus the converted bytes produced by
 * the backend's {@link FragmentConvertor}.
 *
 * @param resolvedFragment      fragment with all viableBackends narrowed to single choices
 * @param backendId             the primary backend for this plan
 * @param convertedBytes        backend-specific serialized plan bytes (null before conversion)
 * @param delegatedExpressions  serialized delegated expressions (empty if no delegation)
 * @param instructions          ordered instruction nodes for data-node execution (empty before resolution)
 * @opensearch.internal
 */
public record StagePlan(RelNode resolvedFragment, String backendId, byte[] convertedBytes, List<DelegatedExpression> delegatedExpressions,
    List<InstructionNode> instructions) {

    /** Creates a StagePlan before conversion (bytes not yet available). */
    public StagePlan(RelNode resolvedFragment, String backendId) {
        this(resolvedFragment, backendId, null, List.of(), List.of());
    }

    /** Returns a copy with converted bytes and delegated expressions populated. */
    public StagePlan withConvertedBytes(byte[] bytes, List<DelegatedExpression> delegatedExpressions) {
        return new StagePlan(resolvedFragment, backendId, bytes, delegatedExpressions, List.of());
    }

    /** Returns a copy with instructions populated. */
    public StagePlan withInstructions(List<InstructionNode> instructions) {
        return new StagePlan(resolvedFragment, backendId, convertedBytes, delegatedExpressions, instructions);
    }
}

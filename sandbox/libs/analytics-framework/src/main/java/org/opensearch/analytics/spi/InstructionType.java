/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Types of instructions that the planner can produce for backend execution.
 * Each type corresponds to a specific execution concern that the backend
 * must handle during the prepare phase on the data node.
 *
 * @opensearch.internal
 */
public enum InstructionType {
    /** Base scan setup — reader acquisition, SessionContext creation, default table provider. */
    SHARD_SCAN,
    /**
     * Filter delegation to an index backend — bridge setup, UDF registration, IndexedTableProvider.
     *
     * <p>TODO: add a DelegationStrategy field (BACKEND_DRIVEN vs CENTRALLY_DRIVEN) to the
     * instruction node when centrally-driven delegation is implemented. Currently only
     * BACKEND_DRIVEN exists — derived from the backend declaring
     * {@code supportedDelegations(DelegationType.FILTER)}.
     */
    FILTER_DELEGATION_FOR_INDEX,
    /** Partial aggregate mode — disable combine optimizer, cut plan to partial-only. */
    PARTIAL_AGGREGATE,
    /** Final aggregate for coordinator reduce — ExchangeSink path, final-only agg. */
    FINAL_AGGREGATE;

    /** Deserializes an {@link InstructionNode} from the stream based on this type. */
    public InstructionNode readNode(StreamInput in) throws IOException {
        return switch (this) {
            case SHARD_SCAN -> new ShardScanInstructionNode(in);
            case FILTER_DELEGATION_FOR_INDEX -> new FilterDelegationInstructionNode(in);
            case PARTIAL_AGGREGATE -> new PartialAggregateInstructionNode(in);
            case FINAL_AGGREGATE -> new FinalAggregateInstructionNode(in);
        };
    }
}

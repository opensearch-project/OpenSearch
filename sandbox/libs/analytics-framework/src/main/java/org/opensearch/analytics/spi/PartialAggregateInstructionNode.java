/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Instruction node for partial aggregate mode — disable combine optimizer, cut plan to partial-only.
 *
 * <p>When {@code hasTopK} is true, the shard fragment also contains a TopK sort (Sort with a
 * non-null fetch/limit). In that case the shard execution must run with a single partition so
 * that CSS does not split the data across multiple partitions, each independently truncating to
 * the TopK limit before the coordinator merge sees all groups.
 *
 * @opensearch.internal
 */
public class PartialAggregateInstructionNode implements InstructionNode {

    private final boolean hasTopK;

    public PartialAggregateInstructionNode() {
        this.hasTopK = false;
    }

    public PartialAggregateInstructionNode(boolean hasTopK) {
        this.hasTopK = hasTopK;
    }

    public PartialAggregateInstructionNode(StreamInput in) throws IOException {
        this.hasTopK = in.readBoolean();
    }

    /** Whether the shard fragment contains a TopK sort (Sort with a non-null fetch/limit). */
    public boolean hasTopK() {
        return hasTopK;
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_PARTIAL_AGGREGATE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(hasTopK);
    }
}

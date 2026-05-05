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
 * <p>TODO: add backend-specific config fields as partial aggregate implementation is built out.
 *
 * @opensearch.internal
 */
public class PartialAggregateInstructionNode implements InstructionNode {

    public PartialAggregateInstructionNode() {}

    public PartialAggregateInstructionNode(StreamInput in) throws IOException {
        // TODO: read config fields when added
    }

    @Override
    public InstructionType type() {
        return InstructionType.PARTIAL_AGGREGATE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: write config fields when added
    }
}

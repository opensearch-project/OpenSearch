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
 * Instruction node for base shard scan setup — reader acquisition, SessionContext creation,
 * default table provider registration.
 *
 * @opensearch.internal
 */
public class ShardScanInstructionNode implements InstructionNode {

    public ShardScanInstructionNode() {}

    public ShardScanInstructionNode(StreamInput in) throws IOException {
        // No fields to read
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_SHARD_SCAN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No fields to write
    }
}

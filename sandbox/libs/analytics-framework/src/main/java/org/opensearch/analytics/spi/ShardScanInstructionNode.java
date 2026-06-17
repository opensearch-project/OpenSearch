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
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Instruction node for base shard scan setup — reader acquisition, SessionContext creation,
 * table provider registration. {@code requestsRowIds} signals that the shard scan needs to
 * emit shard-global {@code __row_id__} values (QTF query phase). Inherited by
 * {@link ShardScanWithDelegationInstructionNode} so the same flag applies whether or not
 * filter delegation is in play — QTF and delegation are orthogonal concerns.
 *
 * @opensearch.internal
 */
public class ShardScanInstructionNode implements InstructionNode, Writeable {

    private final boolean requestsRowIds;
    private final boolean hasPartialAggregate;

    public ShardScanInstructionNode() {
        this(false, false);
    }

    public ShardScanInstructionNode(boolean requestsRowIds) {
        this(requestsRowIds, false);
    }

    public ShardScanInstructionNode(boolean requestsRowIds, boolean hasPartialAggregate) {
        this.requestsRowIds = requestsRowIds;
        this.hasPartialAggregate = hasPartialAggregate;
    }

    public ShardScanInstructionNode(StreamInput in) throws IOException {
        this.requestsRowIds = in.readBoolean();
        this.hasPartialAggregate = in.readBoolean();
    }

    public boolean requestsRowIds() {
        return requestsRowIds;
    }

    public boolean hasPartialAggregate() {
        return hasPartialAggregate;
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_SHARD_SCAN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(requestsRowIds);
        out.writeBoolean(hasPartialAggregate);
    }
}

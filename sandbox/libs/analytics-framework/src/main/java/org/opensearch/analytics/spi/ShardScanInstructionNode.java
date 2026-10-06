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
 * <p>Carries the planner's <em>logical</em> table name (the alias / index-pattern / index the
 * query referenced), captured on the coordinator from the {@code OpenSearchTableScan} leaf. The
 * backend registers the per-shard table under this name so the Substrait plan's {@code NamedTable}
 * reference binds, regardless of which concrete shard index is actually being scanned. May be
 * {@code null} when unknown (e.g. backend self-constructed nodes in tests); the data-node handler
 * falls back to the concrete shard index name in that case.
 *
 * @opensearch.internal
 */
public class ShardScanInstructionNode implements InstructionNode, Writeable {

    private final String logicalTableName;
    private final boolean requestsRowIds;

    public ShardScanInstructionNode() {
        this(false, null);
    }

    public ShardScanInstructionNode(boolean requestsRowIds) {
        this(requestsRowIds, null);
    }

    // requestsRowIds is upstream's param; logicalTableName is our feature-branch addition, appended last.
    public ShardScanInstructionNode(boolean requestsRowIds, String logicalTableName) {
        this.logicalTableName = logicalTableName;
        this.requestsRowIds = requestsRowIds;
    }

    public ShardScanInstructionNode(StreamInput in) throws IOException {
        this.logicalTableName = in.readOptionalString();
        this.requestsRowIds = in.readBoolean();
    }

    /**
     * The logical table name (alias / index pattern / index) the query referenced, or {@code null}
     * if unknown. The backend registers the scanned shard's table under this name so the Substrait
     * plan's {@code NamedTable} binds.
     */
    public String getLogicalTableName() {
        return logicalTableName;
    }

    public boolean requestsRowIds() {
        return requestsRowIds;
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_SHARD_SCAN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(logicalTableName);
        out.writeBoolean(requestsRowIds);
    }
}

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
 * Instruction node for shard scan with filter delegation — extends base shard scan
 * with {@link FilterTreeShape} and delegated predicate count so the driving backend
 * can configure its indexed execution path (UDF registration, IndexedTableProvider)
 * in a single FFM call.
 *
 * @opensearch.internal
 */
public class ShardScanWithDelegationInstructionNode extends ShardScanInstructionNode {

    private final FilterTreeShape treeShape;
    private final int delegatedPredicateCount;

    public ShardScanWithDelegationInstructionNode(FilterTreeShape treeShape, int delegatedPredicateCount) {
        this.treeShape = treeShape;
        this.delegatedPredicateCount = delegatedPredicateCount;
    }

    public ShardScanWithDelegationInstructionNode(StreamInput in) throws IOException {
        super(in);
        this.treeShape = in.readEnum(FilterTreeShape.class);
        this.delegatedPredicateCount = in.readVInt();
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(treeShape);
        out.writeVInt(delegatedPredicateCount);
    }

    public FilterTreeShape getTreeShape() {
        return treeShape;
    }

    public int getDelegatedPredicateCount() {
        return delegatedPredicateCount;
    }
}

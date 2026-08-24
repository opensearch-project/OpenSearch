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
    private final boolean requiresDeletedDocFiltering;

    public ShardScanWithDelegationInstructionNode(FilterTreeShape treeShape, int delegatedPredicateCount) {
        this(treeShape, delegatedPredicateCount, false, null, true);
    }

    public ShardScanWithDelegationInstructionNode(FilterTreeShape treeShape, int delegatedPredicateCount, boolean requestsRowIds) {
        this(treeShape, delegatedPredicateCount, requestsRowIds, null, true);
    }

    // treeShape/delegatedPredicateCount/requestsRowIds are upstream's params; logicalTableName is main's
    // addition; requiresDeletedDocFiltering is our feature-branch addition, appended last.
    public ShardScanWithDelegationInstructionNode(
        FilterTreeShape treeShape,
        int delegatedPredicateCount,
        boolean requestsRowIds,
        String logicalTableName,
        boolean requiresDeletedDocFiltering
    ) {
        super(requestsRowIds, logicalTableName);
        this.treeShape = treeShape;
        this.delegatedPredicateCount = delegatedPredicateCount;
        this.requiresDeletedDocFiltering = requiresDeletedDocFiltering;
    }

    public ShardScanWithDelegationInstructionNode(StreamInput in) throws IOException {
        super(in);
        this.treeShape = in.readEnum(FilterTreeShape.class);
        this.delegatedPredicateCount = in.readVInt();
        this.requiresDeletedDocFiltering = in.readBoolean();
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
        out.writeBoolean(requiresDeletedDocFiltering);
    }

    public FilterTreeShape getTreeShape() {
        return treeShape;
    }

    public int getDelegatedPredicateCount() {
        return delegatedPredicateCount;
    }

    /**
     * Whether the data node should inject a MatchAll delegation when deleted docs are present.
     * Computed at the coordinator via the tree coverage algorithm: {@code true} when the filter
     * tree does not guarantee that every result row passes through a correctness Collector
     * (which respects liveDocs).
     */
    public boolean requiresDeletedDocFiltering() {
        return requiresDeletedDocFiltering;
    }
}

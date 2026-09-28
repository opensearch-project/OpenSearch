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
import java.util.List;

/**
 * Instruction node for filter delegation to an index backend.
 * Carries the tree shape, predicate count, and serialized delegated queries.
 *
 * @opensearch.internal
 */
public class FilterDelegationInstructionNode implements InstructionNode {

    private final FilterTreeShape treeShape;
    private final int delegatedPredicateCount;
    private final List<DelegatedExpression> delegatedQueries;

    public FilterDelegationInstructionNode(
        FilterTreeShape treeShape,
        int delegatedPredicateCount,
        List<DelegatedExpression> delegatedQueries
    ) {
        this.treeShape = treeShape;
        this.delegatedPredicateCount = delegatedPredicateCount;
        this.delegatedQueries = delegatedQueries;
    }

    public FilterDelegationInstructionNode(StreamInput in) throws IOException {
        this.treeShape = in.readEnum(FilterTreeShape.class);
        this.delegatedPredicateCount = in.readInt();
        this.delegatedQueries = in.readList(DelegatedExpression::new);
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(treeShape);
        out.writeInt(delegatedPredicateCount);
        out.writeCollection(delegatedQueries);
    }

    public FilterTreeShape getTreeShape() {
        return treeShape;
    }

    public int getDelegatedPredicateCount() {
        return delegatedPredicateCount;
    }

    public List<DelegatedExpression> getDelegatedQueries() {
        return delegatedQueries;
    }
}

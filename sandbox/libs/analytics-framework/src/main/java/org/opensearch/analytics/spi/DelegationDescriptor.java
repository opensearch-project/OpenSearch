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
import java.util.ArrayList;
import java.util.List;

/**
 * Describes the delegation metadata for a plan alternative. Carried on the wire
 * alongside the instruction list so that Core can orchestrate the handle exchange
 * between accepting and driving backends at the data node.
 *
 * @opensearch.internal
 */
public record DelegationDescriptor(FilterTreeShape treeShape, int delegatedPredicateCount, List<DelegatedExpression> delegatedExpressions)
    implements
        Writeable {

    public DelegationDescriptor(StreamInput in) throws IOException {
        this(in.readEnum(FilterTreeShape.class), in.readVInt(), readExpressions(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(treeShape);
        out.writeVInt(delegatedPredicateCount);
        out.writeVInt(delegatedExpressions.size());
        for (DelegatedExpression expr : delegatedExpressions) {
            expr.writeTo(out);
        }
    }

    private static List<DelegatedExpression> readExpressions(StreamInput in) throws IOException {
        int count = in.readVInt();
        List<DelegatedExpression> expressions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            expressions.add(new DelegatedExpression(in));
        }
        return expressions;
    }
}

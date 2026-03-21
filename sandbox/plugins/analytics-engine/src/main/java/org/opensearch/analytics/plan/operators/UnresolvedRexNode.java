/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan.operators;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

import java.util.Arrays;

/**
 * A {@link RexNode} wrapping an opaque backend-specific payload (e.g., a serialized Lucene
 * FuzzyQuery blob) that has not yet been validated against a backend.
 * Created by frontend plugins (DSL, PPL) for backend-specific query constructs.
 *
 * <p>Requirements: 1.4, 3.6
 */
public final class UnresolvedRexNode extends RexNode {

    private final byte[] payload;

    public UnresolvedRexNode(byte[] payload) {
        this.payload = payload.clone();
    }

    public byte[] getPayload() {
        return payload.clone();
    }

    @Override
    public RelDataType getType() {
        throw new UnsupportedOperationException("UnresolvedRexNode has no type until resolved");
    }

    @Override
    public <R> R accept(RexVisitor<R> visitor) {
        throw new UnsupportedOperationException("UnresolvedRexNode cannot be visited until resolved");
    }

    @Override
    public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
        throw new UnsupportedOperationException("UnresolvedRexNode cannot be visited until resolved");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof UnresolvedRexNode)) return false;
        return Arrays.equals(payload, ((UnresolvedRexNode) obj).payload);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        return "UnresolvedRexNode[" + payload.length + " bytes]";
    }
}

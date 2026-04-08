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
 * An {@link UnresolvedRexNode} that has been accepted and validated by exactly one backend.
 * Carries the resolved backend name alongside the original opaque payload.
 *
 * <p>Requirements: 5.5
 */
public final class BackendSpecificRexNode extends RexNode {

    private final String backendName;
    private final byte[] payload;

    public BackendSpecificRexNode(String backendName, byte[] payload) {
        this.backendName = backendName;
        this.payload = payload.clone();
    }

    public String getBackendName() {
        return backendName;
    }

    public byte[] getPayload() {
        return payload.clone();
    }

    @Override
    public RelDataType getType() {
        throw new UnsupportedOperationException("BackendSpecificRexNode has no generic type");
    }

    @Override
    public <R> R accept(RexVisitor<R> visitor) {
        throw new UnsupportedOperationException("BackendSpecificRexNode cannot be visited generically");
    }

    @Override
    public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
        throw new UnsupportedOperationException("BackendSpecificRexNode cannot be visited generically");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof BackendSpecificRexNode)) return false;
        BackendSpecificRexNode other = (BackendSpecificRexNode) obj;
        return backendName.equals(other.backendName) && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        return 31 * backendName.hashCode() + Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        return "BackendSpecificRexNode[backend=" + backendName + ", " + payload.length + " bytes]";
    }
}

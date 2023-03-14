/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Response for onIndexModule extension point
 *
 * @opensearch.internal
 */
public class IndicesModuleResponse extends TransportResponse {
    private boolean supportsIndexEventListener;
    private boolean addIndexOperationListener;
    private boolean addSearchOperationListener;

    public IndicesModuleResponse(
        boolean supportsIndexEventListener,
        boolean addIndexOperationListener,
        boolean addSearchOperationListener
    ) {
        this.supportsIndexEventListener = supportsIndexEventListener;
        this.addIndexOperationListener = addIndexOperationListener;
        this.addSearchOperationListener = addSearchOperationListener;
    }

    public IndicesModuleResponse(StreamInput in) throws IOException {
        this.supportsIndexEventListener = in.readBoolean();
        this.addIndexOperationListener = in.readBoolean();
        this.addSearchOperationListener = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(supportsIndexEventListener);
        out.writeBoolean(addIndexOperationListener);
        out.writeBoolean(addSearchOperationListener);
    }

    public boolean getIndexEventListener() {
        return this.supportsIndexEventListener;
    }

    public boolean getIndexOperationListener() {
        return this.addIndexOperationListener;
    }

    public boolean getSearchOperationListener() {
        return this.addSearchOperationListener;
    }

    @Override
    public String toString() {
        return "IndicesModuleResponse{"
            + "supportsIndexEventListener"
            + supportsIndexEventListener
            + " addIndexOperationListener"
            + addIndexOperationListener
            + " addSearchOperationListener"
            + addSearchOperationListener
            + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndicesModuleResponse that = (IndicesModuleResponse) o;
        return Objects.equals(supportsIndexEventListener, that.supportsIndexEventListener)
            && Objects.equals(addIndexOperationListener, that.addIndexOperationListener)
            && Objects.equals(addSearchOperationListener, that.addSearchOperationListener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(supportsIndexEventListener, addIndexOperationListener, addSearchOperationListener);
    }
}

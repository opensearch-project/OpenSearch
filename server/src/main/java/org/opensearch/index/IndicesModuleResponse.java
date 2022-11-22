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
    private boolean addIndexEventListener;
    private boolean addIndexOperationListener;
    private boolean addSearchOperationListener;

    public IndicesModuleResponse(boolean addIndexEventListener, boolean addIndexOperationListener, boolean addSearchOperationListener) {
        this.addIndexEventListener = addIndexEventListener;
        this.addIndexOperationListener = addIndexOperationListener;
        this.addSearchOperationListener = addSearchOperationListener;
    }

    public IndicesModuleResponse(StreamInput in) throws IOException {
        this.addIndexEventListener = in.readBoolean();
        this.addIndexOperationListener = in.readBoolean();
        this.addSearchOperationListener = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(addIndexEventListener);
        out.writeBoolean(addIndexOperationListener);
        out.writeBoolean(addSearchOperationListener);
    }

    public boolean getIndexEventListener() {
        return this.addIndexEventListener;
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
            + "addIndexEventListener"
            + addIndexEventListener
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
        return Objects.equals(addIndexEventListener, that.addIndexEventListener)
            && Objects.equals(addIndexOperationListener, that.addIndexOperationListener)
            && Objects.equals(addSearchOperationListener, that.addSearchOperationListener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(addIndexEventListener, addIndexOperationListener, addSearchOperationListener);
    }
}

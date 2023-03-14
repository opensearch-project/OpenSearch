/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Request to register extension Transport actions
 *
 * @opensearch.internal
 */
public class RegisterTransportActionsRequest extends TransportRequest {
    // The uniqueId defining the extension which runs this action
    private String uniqueId;
    // The action names to register
    private Set<String> transportActions;

    public RegisterTransportActionsRequest(String uniqueId, Set<String> transportActions) {
        this.uniqueId = uniqueId;
        this.transportActions = transportActions;
    }

    public RegisterTransportActionsRequest(StreamInput in) throws IOException {
        super(in);
        this.uniqueId = in.readString();
        this.transportActions = new HashSet<>(in.readStringList());
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public Set<String> getTransportActions() {
        return transportActions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(uniqueId);
        out.writeStringCollection(transportActions);
    }

    @Override
    public String toString() {
        return "TransportActionsRequest{uniqueId=" + uniqueId + ", actions=" + transportActions + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RegisterTransportActionsRequest that = (RegisterTransportActionsRequest) obj;
        return Objects.equals(uniqueId, that.uniqueId) && Objects.equals(transportActions, that.transportActions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueId, transportActions);
    }
}

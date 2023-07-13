/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.extensions.proto.RegisterTransportActionsProto.RegisterTransportActions;
import org.opensearch.extensions.proto.ExtensionIdentityProto.ExtensionIdentity;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Request to register extension Transport actions
 *
 * @opensearch.internal
 */
public class RegisterTransportActionsRequest extends TransportRequest {
    private final RegisterTransportActions request;

    public RegisterTransportActionsRequest(String uniqueId, Set<String> transportActions) {
        ExtensionIdentity identity = ExtensionIdentity.newBuilder().setUniqueId(uniqueId).build();
        this.request = RegisterTransportActions.newBuilder().setIdentity(identity).addAllTransportActions(transportActions).build();
    }

    public RegisterTransportActionsRequest(StreamInput in) throws IOException {
        super(in);
        this.request = RegisterTransportActions.parseFrom(in.readByteArray());
    }

    public String getUniqueId() {
        return request.getIdentity().getUniqueId();
    }

    public List<String> getTransportActions() {
        return request.getTransportActionsList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByteArray(request.toByteArray());
    }

    @Override
    public String toString() {
        return "TransportActionsRequest{Identity=" + request.getIdentity() + ", actions=" + request.getTransportActionsList() + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RegisterTransportActionsRequest that = (RegisterTransportActionsRequest) obj;
        return Objects.equals(request.getIdentity().getUniqueId(), that.request.getIdentity().getUniqueId())
            && Objects.equals(request.getTransportActionsList(), that.request.getTransportActionsList());
    }

    @Override
    public int hashCode() {
        return Objects.hash(request.getIdentity(), request.getTransportActionsList());
    }
}

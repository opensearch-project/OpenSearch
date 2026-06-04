/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.extensions.proto.ExtensionIdentityProto.ExtensionIdentity;
import org.opensearch.extensions.proto.RegisterRestActionsProto.RegisterRestActions;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Request to register extension REST actions
 *
 * @opensearch.internal
 */
public class RegisterRestActionsRequest extends TransportRequest {
    private final RegisterRestActions request;

    public RegisterRestActionsRequest(String uniqueId, List<String> restActions, List<String> deprecatedRestActions) {
        ExtensionIdentity identity = ExtensionIdentity.newBuilder().setUniqueId(uniqueId).build();
        this.request = RegisterRestActions.newBuilder()
            .setIdentity(identity)
            .addAllRestActions(restActions)
            .addAllDeprecatedRestActions(deprecatedRestActions)
            .build();
    }

    public RegisterRestActionsRequest(StreamInput in) throws IOException {
        super(in);
        request = RegisterRestActions.parseFrom(in.readByteArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByteArray(request.toByteArray());
    }

    public String getUniqueId() {
        return request.getIdentity().getUniqueId();
    }

    public List<String> getRestActions() {
        return List.copyOf(request.getRestActionsList());
    }

    public List<String> getDeprecatedRestActions() {
        return List.copyOf(request.getDeprecatedRestActionsList());
    }

    @Override
    public String toString() {
        return "RestActionsRequest{Identity="
            + request.getIdentity()
            + ", restActions="
            + request.getRestActionsList()
            + ", deprecatedRestActions="
            + request.getDeprecatedRestActionsList()
            + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RegisterRestActionsRequest that = (RegisterRestActionsRequest) obj;
        return Objects.equals(request.getIdentity().getUniqueId(), that.request.getIdentity().getUniqueId())
            && Objects.equals(request.getRestActionsList(), that.request.getRestActionsList())
            && Objects.equals(request.getDeprecatedRestActionsList(), that.request.getDeprecatedRestActionsList());
    }

    @Override
    public int hashCode() {
        return Objects.hash(request.getIdentity(), request.getRestActionsList(), request.getDeprecatedRestActionsList());
    }
}

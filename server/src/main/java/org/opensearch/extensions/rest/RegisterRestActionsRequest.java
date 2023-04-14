/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Request to register extension REST actions
 *
 * @opensearch.internal
 */
public class RegisterRestActionsRequest extends TransportRequest {
    private String uniqueId;
    private List<String> restActions;
    private List<String> deprecatedRestActions;

    public RegisterRestActionsRequest(String uniqueId, List<String> restActions, List<String> deprecatedRestActions) {
        this.uniqueId = uniqueId;
        this.restActions = new ArrayList<>(restActions);
        this.deprecatedRestActions = new ArrayList<>(deprecatedRestActions);
    }

    public RegisterRestActionsRequest(StreamInput in) throws IOException {
        super(in);
        uniqueId = in.readString();
        restActions = in.readStringList();
        deprecatedRestActions = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(uniqueId);
        out.writeStringCollection(restActions);
        out.writeStringCollection(deprecatedRestActions);
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public List<String> getRestActions() {
        return List.copyOf(restActions);
    }

    public List<String> getDeprecatedRestActions() {
        return List.copyOf(deprecatedRestActions);
    }

    @Override
    public String toString() {
        return "RestActionsRequest{uniqueId="
            + uniqueId
            + ", restActions="
            + restActions
            + ", deprecatedRestActions="
            + deprecatedRestActions
            + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RegisterRestActionsRequest that = (RegisterRestActionsRequest) obj;
        return Objects.equals(uniqueId, that.uniqueId)
            && Objects.equals(restActions, that.restActions)
            && Objects.equals(deprecatedRestActions, that.deprecatedRestActions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueId, restActions, deprecatedRestActions);
    }
}

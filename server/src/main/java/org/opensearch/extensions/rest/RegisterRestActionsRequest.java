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
    private String nodeId;
    private String uniqueId;
    private List<String> restActions;

    public RegisterRestActionsRequest(String nodeId, String uniqueId, List<String> restActions) {
        this.nodeId = nodeId;
        this.uniqueId = uniqueId;
        this.restActions = new ArrayList<>(restActions);
    }

    public RegisterRestActionsRequest(StreamInput in) throws IOException {
        super(in);
        nodeId = in.readString();
        uniqueId = in.readString();
        restActions = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeString(uniqueId);
        out.writeStringCollection(restActions);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public List<String> getRestActions() {
        return new ArrayList<>(restActions);
    }

    @Override
    public String toString() {
        return "RestActionsRequest{nodeId=" + nodeId + ", uniqueId=" + uniqueId + ", restActions=" + restActions + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RegisterRestActionsRequest that = (RegisterRestActionsRequest) obj;
        return Objects.equals(nodeId, that.nodeId)
            && Objects.equals(uniqueId, that.uniqueId)
            && Objects.equals(restActions, that.restActions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, uniqueId, restActions);
    }
}

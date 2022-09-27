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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Request to register extension Transport actions
 *
 * @opensearch.internal
 */
public class RegisterTransportActionsRequest extends TransportRequest {
    private String uniqueId;
    private Map<String, Class> transportActions;

    public RegisterTransportActionsRequest(String uniqueId, Map<String, Class> transportActions) {
        this.uniqueId = uniqueId;
        this.transportActions = new HashMap<>(transportActions);
    }

    public RegisterTransportActionsRequest(StreamInput in) throws IOException {
        super(in);
        this.uniqueId = in.readString();
        Map<String, Class> actions = new HashMap<>();
        int actionCount = in.readVInt();
        for (int i = 0; i < actionCount; i++) {
            try {
                String actionName = in.readString();
                Class transportAction = Class.forName(in.readString());
                actions.put(actionName, transportAction);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not read transport action");
            }
        }
        this.transportActions = actions;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public Map<String, Class> getTransportActions() {
        return transportActions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(uniqueId);
        out.writeVInt(this.transportActions.size());
        for (Map.Entry<String, Class> action : transportActions.entrySet()) {
            out.writeString(action.getKey());
            out.writeString(action.getValue().getName());
        }
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

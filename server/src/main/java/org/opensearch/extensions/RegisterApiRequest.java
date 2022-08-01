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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * ApiRequest to register extension API
 *
 * @opensearch.internal
 */
public class RegisterApiRequest extends TransportRequest {
    private String nodeId;
    private List<String> api;

    public RegisterApiRequest(String nodeId, List<String> api) {
        this.nodeId = nodeId;
        this.api = new ArrayList<>(api);
    }

    public RegisterApiRequest(StreamInput in) throws IOException {
        super(in);
        nodeId = in.readString();
        api = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeStringCollection(api);
    }

    public String getNodeId() {
        return nodeId;
    }

    public List<String> getApi() {
        return new ArrayList<>(api);
    }

    @Override
    public String toString() {
        return "ApiRequest{nodeId=" + nodeId + ", api=" + api + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RegisterApiRequest that = (RegisterApiRequest) obj;
        return Objects.equals(nodeId, that.nodeId) && Objects.equals(api, that.api);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, api);
    }
}

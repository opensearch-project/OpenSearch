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
 * ApiRequest to register extension REST API
 *
 * @opensearch.internal
 */
public class RegisterRestApiRequest extends TransportRequest {
    private String nodeId;
    private List<String> restApi;

    public RegisterRestApiRequest(String nodeId, List<String> api) {
        this.nodeId = nodeId;
        this.restApi = new ArrayList<>(api);
    }

    public RegisterRestApiRequest(StreamInput in) throws IOException {
        super(in);
        nodeId = in.readString();
        restApi = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeStringCollection(restApi);
    }

    public String getNodeId() {
        return nodeId;
    }

    public List<String> getRestApi() {
        return new ArrayList<>(restApi);
    }

    @Override
    public String toString() {
        return "RestApiRequest{nodeId=" + nodeId + ", restApi=" + restApi + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RegisterRestApiRequest that = (RegisterRestApiRequest) obj;
        return Objects.equals(nodeId, that.nodeId) && Objects.equals(restApi, that.restApi);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, restApi);
    }
}

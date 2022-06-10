/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * clusterState Response for Extensibility
 *
 * @opensearch.internal
 */
public class ExtensionClusterStateResponse extends TransportResponse {
    private final ClusterState clusterState;

    public ExtensionClusterStateResponse(ClusterService clusterService) {
        this.clusterState = clusterService.state();
    }

    public ExtensionClusterStateResponse(StreamInput in) throws IOException {
        super(in);
        this.clusterState = ClusterState.readFrom(in, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.clusterState.writeTo(out);
    }

    @Override
    public String toString() {
        return "ClusterStateResponse{" + "clusterState=" + clusterState + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtensionClusterStateResponse that = (ExtensionClusterStateResponse) o;
        return Objects.equals(clusterState, that.clusterState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterState);
    }

}

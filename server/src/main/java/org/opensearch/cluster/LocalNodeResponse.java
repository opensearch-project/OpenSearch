/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * LocalNode Response for Extensibility
 *
 * @opensearch.internal
 */
public class LocalNodeResponse extends TransportResponse {
    private final DiscoveryNode localNode;

    public LocalNodeResponse(ClusterService clusterService) {
        this.localNode = clusterService.localNode();
    }

    public LocalNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.localNode = new DiscoveryNode(in);
    }

    /**
     * Get the local node
     */
    public DiscoveryNode getLocalNode() {
        return this.localNode;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.localNode.writeTo(out);
    }

    @Override
    public String toString() {
        return "LocalNodeResponse{" + "localNode=" + localNode + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalNodeResponse that = (LocalNodeResponse) o;
        return Objects.equals(localNode, that.localNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(localNode);
    }
}

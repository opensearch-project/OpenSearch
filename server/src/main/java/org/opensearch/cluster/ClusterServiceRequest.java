/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

public class ClusterServiceRequest extends TransportRequest {
    private static final Logger logger = LogManager.getLogger(ClusterServiceRequest.class);
    private boolean clusterState;

    public ClusterServiceRequest(Boolean clusterState) {
        this.clusterState = clusterState;
    }

    public ClusterServiceRequest(StreamInput in) throws IOException {
        super(in);
        this.clusterState = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(clusterState);
    }

    public String toString() {
        return "ClusterServiceRequest{" + "clusterstate=" + clusterState + '}';
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterServiceRequest that = (ClusterServiceRequest) o;
        return Objects.equals(clusterState, that.clusterState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterState);
    }

}

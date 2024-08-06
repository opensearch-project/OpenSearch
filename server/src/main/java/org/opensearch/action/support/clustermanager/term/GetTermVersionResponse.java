/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.clustermanager.term;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response object of cluster term
 *
 * @opensearch.internal
 */
public class GetTermVersionResponse extends ActionResponse {

    private final ClusterStateTermVersion clusterStateTermVersion;

    public GetTermVersionResponse(ClusterStateTermVersion clusterStateTermVersion) {
        this.clusterStateTermVersion = clusterStateTermVersion;
    }

    public GetTermVersionResponse(StreamInput in) throws IOException {
        super(in);
        this.clusterStateTermVersion = new ClusterStateTermVersion(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterStateTermVersion.writeTo(out);
    }

    public ClusterStateTermVersion getClusterStateTermVersion() {
        return clusterStateTermVersion;
    }

    public boolean matches(ClusterState clusterState) {
        return clusterStateTermVersion != null && clusterStateTermVersion.equals(new ClusterStateTermVersion(clusterState));
    }

}

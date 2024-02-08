/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.state.term;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
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

    private final ClusterName clusterName;
    private final String stateUUID;
    private final long term;
    private final long version;

    public GetTermVersionResponse(ClusterName clusterName, String stateUUID, long term, long version) {
        this.clusterName = clusterName;
        this.stateUUID = stateUUID;
        this.term = term;
        this.version = version;
    }

    public GetTermVersionResponse(StreamInput in) throws IOException {
        super(in);
        this.clusterName = new ClusterName(in);
        this.stateUUID = in.readString();
        this.term = in.readLong();
        this.version = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        out.writeString(stateUUID);
        out.writeLong(term);
        out.writeLong(version);
    }

    public long getTerm() {
        return term;
    }

    public long getVersion() {
        return version;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public String getStateUUID() {
        return stateUUID;
    }

    public boolean matches(ClusterState clusterState) {
        return clusterName.equals(clusterState.getClusterName())
            && stateUUID.equals(clusterState.stateUUID())
            && term == clusterState.term()
            && version == clusterState.version();
    }

}

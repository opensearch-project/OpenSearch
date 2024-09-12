/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.clustermanager.term;

import org.opensearch.Version;
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

    private final boolean isStatePresentInRemote;

    public GetTermVersionResponse(ClusterStateTermVersion clusterStateTermVersion) {
        this.clusterStateTermVersion = clusterStateTermVersion;
        this.isStatePresentInRemote = false;
    }

    public GetTermVersionResponse(ClusterStateTermVersion clusterStateTermVersion, boolean canDownloadFromRemote) {
        this.clusterStateTermVersion = clusterStateTermVersion;
        this.isStatePresentInRemote = canDownloadFromRemote;
    }

    public GetTermVersionResponse(StreamInput in) throws IOException {
        super(in);
        this.clusterStateTermVersion = new ClusterStateTermVersion(in);
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.isStatePresentInRemote = in.readOptionalBoolean();
        } else {
            this.isStatePresentInRemote = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterStateTermVersion.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeOptionalBoolean(isStatePresentInRemote);
        }
    }

    public ClusterStateTermVersion getClusterStateTermVersion() {
        return clusterStateTermVersion;
    }

    public boolean matches(ClusterState clusterState) {
        return clusterStateTermVersion != null && clusterStateTermVersion.equals(new ClusterStateTermVersion(clusterState));
    }

    public boolean isStatePresentInRemote() {
        return isStatePresentInRemote;
    }
}

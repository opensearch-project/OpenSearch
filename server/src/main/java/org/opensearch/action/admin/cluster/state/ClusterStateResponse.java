/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.state;

import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * The response for getting the cluster state.
 *
 * @opensearch.internal
 */
public class ClusterStateResponse extends ActionResponse {

    private ClusterName clusterName;
    private ClusterState clusterState;
    private boolean waitForTimedOut = false;

    public ClusterStateResponse(StreamInput in) throws IOException {
        super(in);
        clusterName = new ClusterName(in);
        clusterState = in.readOptionalWriteable(innerIn -> ClusterState.readFrom(innerIn, null));
        waitForTimedOut = in.readBoolean();
    }

    public ClusterStateResponse(ClusterName clusterName, ClusterState clusterState, boolean waitForTimedOut) {
        this.clusterName = clusterName;
        this.clusterState = clusterState;
        this.waitForTimedOut = waitForTimedOut;
    }

    /**
     * The requested cluster state.  Only the parts of the cluster state that were
     * requested are included in the returned {@link ClusterState} instance.
     */
    public ClusterState getState() {
        return this.clusterState;
    }

    /**
     * The name of the cluster.
     */
    public ClusterName getClusterName() {
        return this.clusterName;
    }

    /**
     * Returns whether the request timed out waiting for a cluster state with a metadata version equal or
     * higher than the specified metadata.
     */
    public boolean isWaitForTimedOut() {
        return waitForTimedOut;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        out.writeOptionalWriteable(clusterState);
        out.writeBoolean(waitForTimedOut);
    }

    @Override
    public String toString() {
        return "ClusterStateResponse{" + "clusterState=" + clusterState + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateResponse response = (ClusterStateResponse) o;
        return waitForTimedOut == response.waitForTimedOut && Objects.equals(clusterName, response.clusterName) &&
        // Best effort. Only compare cluster state version and cluster-manager node id,
        // because cluster state doesn't implement equals()
            Objects.equals(getVersion(clusterState), getVersion(response.clusterState))
            && Objects.equals(getClusterManagerNodeId(clusterState), getClusterManagerNodeId(response.clusterState));
    }

    @Override
    public int hashCode() {
        // Best effort. Only use cluster state version and cluster-manager node id,
        // because cluster state doesn't implement hashcode()
        return Objects.hash(clusterName, getVersion(clusterState), getClusterManagerNodeId(clusterState), waitForTimedOut);
    }

    private static String getClusterManagerNodeId(ClusterState clusterState) {
        if (clusterState == null) {
            return null;
        }
        DiscoveryNodes nodes = clusterState.getNodes();
        if (nodes != null) {
            return nodes.getClusterManagerNodeId();
        } else {
            return null;
        }
    }

    private static Long getVersion(ClusterState clusterState) {
        if (clusterState != null) {
            return clusterState.getVersion();
        } else {
            return null;
        }
    }

}

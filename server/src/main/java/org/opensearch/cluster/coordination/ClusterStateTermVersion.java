/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Identifies a specific version of ClusterState at a node.
 */
public class ClusterStateTermVersion implements Writeable {

    private final ClusterName clusterName;
    private final String clusterUUID;
    private final long term;
    private final long version;

    public ClusterStateTermVersion(ClusterName clusterName, String clusterUUID, long term, long version) {
        this.clusterName = clusterName;
        this.clusterUUID = clusterUUID;
        this.term = term;
        this.version = version;
    }

    public ClusterStateTermVersion(StreamInput in) throws IOException {
        this.clusterName = new ClusterName(in);
        this.clusterUUID = in.readString();
        this.term = in.readLong();
        this.version = in.readLong();
    }

    public ClusterStateTermVersion(ClusterState state) {
        this.clusterName = state.getClusterName();
        this.clusterUUID = state.metadata().clusterUUID();
        this.term = state.term();
        this.version = state.version();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        out.writeString(clusterUUID);
        out.writeLong(term);
        out.writeLong(version);
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public String getClusterUUID() {
        return clusterUUID;
    }

    public long getTerm() {
        return term;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "ClusterStateTermVersion{"
            + "clusterName="
            + clusterName
            + ", clusterUUID='"
            + clusterUUID
            + '\''
            + ", term="
            + term
            + ", version="
            + version
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterStateTermVersion that = (ClusterStateTermVersion) o;

        if (term != that.term) return false;
        if (version != that.version) return false;
        if (!clusterName.equals(that.clusterName)) return false;
        return clusterUUID.equals(that.clusterUUID);
    }

    @Override
    public int hashCode() {
        int result = clusterName.hashCode();
        result = 31 * result + clusterUUID.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }
}

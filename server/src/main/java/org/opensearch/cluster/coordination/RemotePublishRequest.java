/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import java.io.IOException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class RemotePublishRequest extends TermVersionRequest {

    // todo Do we need cluster name and UUID ?
    private final String clusterName;
    private final String clusterUUID;

    public RemotePublishRequest(DiscoveryNode sourceNode, long term, long version, String clusterName, String clusterUUID) {
        super(sourceNode, term, version);
        this.clusterName = clusterName;
        this.clusterUUID = clusterUUID;
    }

    public RemotePublishRequest(StreamInput in) throws IOException {
        super(in);
        this.clusterName = in.readString();
        this.clusterUUID = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(clusterName);
        out.writeString(clusterUUID);
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getClusterUUID() {
        return clusterUUID;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Send the publish request with the remote cluster state details
 * @opensearch.internal
 */
public class RemotePublishRequest extends TermVersionRequest {

    private final String clusterName;
    private final String clusterUUID;
    private final String manifestFile;

    public RemotePublishRequest(
        DiscoveryNode sourceNode,
        long term,
        long version,
        String clusterName,
        String clusterUUID,
        String manifestFile
    ) {
        super(sourceNode, term, version);
        this.clusterName = clusterName;
        this.clusterUUID = clusterUUID;
        this.manifestFile = manifestFile;
    }

    public RemotePublishRequest(StreamInput in) throws IOException {
        super(in);
        this.clusterName = in.readString();
        this.clusterUUID = in.readString();
        this.manifestFile = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(clusterName);
        out.writeString(clusterUUID);
        out.writeString(manifestFile);
    }

    @Override
    public String toString() {
        return "RemotePublishRequest{"
            + "term="
            + term
            + ", version="
            + version
            + ", clusterName="
            + clusterName
            + ", clusterUUID="
            + clusterUUID
            + ", sourceNode="
            + sourceNode
            + ", manifestFile="
            + manifestFile
            + '}';
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getClusterUUID() {
        return clusterUUID;
    }

    public String getManifestFile() {
        return manifestFile;
    }
}

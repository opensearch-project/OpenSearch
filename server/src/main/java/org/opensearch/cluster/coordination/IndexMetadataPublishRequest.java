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
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Send the publish request with the remote cluster state details
 * @opensearch.internal
 */
public class IndexMetadataPublishRequest extends TransportRequest implements Writeable {
    protected final DiscoveryNode sourceNode;
    private final String manifestVersion;

    public IndexMetadataPublishRequest(
        DiscoveryNode sourceNode,
        String manifestVersion
    ) {
        this.sourceNode = sourceNode;
        this.manifestVersion = manifestVersion;
    }

    public IndexMetadataPublishRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceNode = new DiscoveryNode(in);
        this.manifestVersion = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        out.writeString(manifestVersion);
    }

    @Override
    public String toString() {
        return "IndexMetadataPublishRequest{"
            + ", sourceNode="
            + sourceNode
            + ", manifestVersion="
            + manifestVersion
            + '}';
    }

    public String getManifestVersion() {
        return manifestVersion;
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.extensions.DiscoveryExtensionNode;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * PluginRequest to intialize plugin
 *
 * @opensearch.internal
 */
public class PluginRequest extends TransportRequest {
    private final DiscoveryNode sourceNode;
    /*
     * TODO change DiscoveryNode to Extension information
     */
    private final List<DiscoveryExtensionNode> extensions;

    public PluginRequest(DiscoveryNode sourceNode, List<DiscoveryExtensionNode> extensions) {
        this.sourceNode = sourceNode;
        this.extensions = extensions;
    }

    public PluginRequest(StreamInput in) throws IOException {
        super(in);
        sourceNode = new DiscoveryNode(in);
        extensions = in.readList(DiscoveryExtensionNode::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        out.writeList(extensions);
    }

    public List<DiscoveryExtensionNode> getExtensions() {
        return extensions;
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    @Override
    public String toString() {
        return "PluginRequest{" + "sourceNode=" + sourceNode + ", extensions=" + extensions + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PluginRequest that = (PluginRequest) o;
        return Objects.equals(sourceNode, that.sourceNode) && Objects.equals(extensions, that.extensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNode, extensions);
    }
}

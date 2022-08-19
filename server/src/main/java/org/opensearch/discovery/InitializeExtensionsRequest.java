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
import org.opensearch.extensions.DiscoveryExtension;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * InitializeExtensionsRequest to initialize extension
 *
 * @opensearch.internal
 */
public class InitializeExtensionsRequest extends TransportRequest {
    private final DiscoveryNode sourceNode;
    /*
     * TODO change DiscoveryNode to Extension information
     */
    private final List<DiscoveryExtension> extensions;
    public static final int OS_DEFAULT_PORT = 9200;
    private int port;

    public InitializeExtensionsRequest(DiscoveryNode sourceNode, List<DiscoveryExtension> extensions) {
        this.sourceNode = sourceNode;
        this.extensions = extensions;
        this.port = OS_DEFAULT_PORT;
    }

    public InitializeExtensionsRequest(StreamInput in) throws IOException {
        super(in);
        sourceNode = new DiscoveryNode(in);
        extensions = in.readList(DiscoveryExtension::new);
        port = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        out.writeList(extensions);
        out.writeInt(port);
    }

    public List<DiscoveryExtension> getExtensions() {
        return extensions;
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "InitializeExtensionsRequest{" + "sourceNode=" + sourceNode + ", extensions=" + extensions + ", port=" + port + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InitializeExtensionsRequest that = (InitializeExtensionsRequest) o;
        return Objects.equals(sourceNode, that.sourceNode)
            && Objects.equals(extensions, that.extensions)
            && Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNode, extensions, port);
    }
}

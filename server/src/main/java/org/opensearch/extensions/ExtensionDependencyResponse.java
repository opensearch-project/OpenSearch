/*
* Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

public class ExtensionDependencyResponse extends TransportResponse {
    private List<DiscoveryExtensionNode> dependency;

    public ExtensionDependencyResponse(List<DiscoveryExtensionNode> dependency) {
        this.dependency = dependency;
    }

    public ExtensionDependencyResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        dependency = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            dependency.add(new DiscoveryExtensionNode(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(dependency.size());
        for (DiscoveryExtensionNode dependency : dependency) {
            dependency.writeTo(out);
        }
    }

    public List<DiscoveryExtensionNode> getExtensionDependency() {
        return dependency;
    }

    @Override
    public String toString() {
        return "ExtensionDependencyResponse{extensiondependency=" + dependency.toString() + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtensionDependencyResponse that = (ExtensionDependencyResponse) o;
        return Objects.equals(dependency, that.dependency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dependency);
    }
}

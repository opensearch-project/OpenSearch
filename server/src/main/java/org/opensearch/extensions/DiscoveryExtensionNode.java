/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Discover extensions running independently or in a separate process
 *
 * @opensearch.internal
 */
public class DiscoveryExtensionNode extends DiscoveryNode implements Writeable, ToXContentFragment {

    private Version minimumCompatibleVersion;
    private List<ExtensionDependency> dependencies = Collections.emptyList();
    private List<String> implementedInterfaces = Collections.emptyList();

    public DiscoveryExtensionNode(
        String name,
        String id,
        TransportAddress address,
        Map<String, String> attributes,
        Version version,
        Version minimumCompatibleVersion,
        List<ExtensionDependency> dependencies
    ) {
        super(name, id, address, attributes, DiscoveryNodeRole.BUILT_IN_ROLES, version);
        this.minimumCompatibleVersion = minimumCompatibleVersion;
        this.dependencies = dependencies;
        validate();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVersion(minimumCompatibleVersion);
        out.writeVInt(dependencies.size());
        for (ExtensionDependency dependency : dependencies) {
            dependency.writeTo(out);
        }
    }

    /**
     * Construct DiscoveryExtensionNode from a stream.
     *
     * @param in the stream
     * @throws IOException if an I/O exception occurred reading the plugin info from the stream
     */
    public DiscoveryExtensionNode(final StreamInput in) throws IOException {
        super(in);
        minimumCompatibleVersion = in.readVersion();
        int size = in.readVInt();
        dependencies = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            dependencies.add(new ExtensionDependency(in));
        }
    }

    public List<ExtensionDependency> getDependencies() {
        return dependencies;
    }

    public Version getMinimumCompatibleVersion() {
        return minimumCompatibleVersion;
    }

    public List<String> getImplementedInterfaces() {
        return implementedInterfaces;
    }

    public void setImplementedInterfaces(List<String> implementedInterfaces) {
        this.implementedInterfaces = implementedInterfaces;
    }

    public boolean dependenciesContain(ExtensionDependency dependency) {
        for (ExtensionDependency extensiondependency : this.dependencies) {
            if (dependency.getUniqueId().equals(extensiondependency.getUniqueId())
                && dependency.getVersion().equals(extensiondependency.getVersion())) {
                return true;
            }
        }
        return false;
    }

    private void validate() {
        if (!Version.CURRENT.onOrAfter(minimumCompatibleVersion)) {
            throw new OpenSearchException(
                "Extension minimumCompatibleVersion: "
                    + minimumCompatibleVersion
                    + " is greater than current OpenSearch version: "
                    + Version.CURRENT
            );
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}

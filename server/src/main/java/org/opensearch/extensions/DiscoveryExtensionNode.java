/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.plugins.PluginInfo;

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

    private final PluginInfo pluginInfo;
    private List<ExtensionDependency> dependencies = Collections.emptyList();

    public DiscoveryExtensionNode(
        String name,
        String id,
        String ephemeralId,
        String hostName,
        String hostAddress,
        TransportAddress address,
        Map<String, String> attributes,
        Version version,
        PluginInfo pluginInfo,
        List<ExtensionDependency> dependencies
    ) {
        super(name, id, ephemeralId, hostName, hostAddress, address, attributes, DiscoveryNodeRole.BUILT_IN_ROLES, version);
        this.pluginInfo = pluginInfo;
        this.dependencies = dependencies;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        pluginInfo.writeTo(out);
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
        this.pluginInfo = new PluginInfo(in);
        int size = in.readVInt();
        dependencies = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            dependencies.add(new ExtensionDependency(in));
        }
    }

    public List<ExtensionDependency> getDependencies() {
        return dependencies;
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}

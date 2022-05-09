/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

public class CreateComponentResponse extends TransportResponse {
    private final ClusterState clusterState;
    private final Settings pluginSettings;
    private final DiscoveryNode localNode;

    public CreateComponentResponse(ClusterService clusterService) {
        this.localNode = clusterService.localNode();
        this.clusterState = clusterService.state();
        this.pluginSettings = clusterService.getSettings();
    }

    public CreateComponentResponse(StreamInput in) throws IOException {
        super(in);
        this.localNode = new DiscoveryNode(in);
        this.clusterState = ClusterState.readFrom(in, localNode);
        this.pluginSettings = Settings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.localNode.writeTo(out);
        this.clusterState.writeTo(out);
        Settings.writeSettingsToStream(pluginSettings, out);
    }

    @Override
    public String toString() {
        return "CreateComponentRequest{" + ", pluginSettings=" + pluginSettings + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateComponentResponse that = (CreateComponentResponse) o;
        return Objects.equals(pluginSettings, that.pluginSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pluginSettings);
    }

}

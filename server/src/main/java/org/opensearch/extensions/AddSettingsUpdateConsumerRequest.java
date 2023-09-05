/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.WriteableSetting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Add Settings Update Consumer Request for Extensibility
 *
 * @opensearch.internal
 */
public class AddSettingsUpdateConsumerRequest extends TransportRequest {
    private final DiscoveryExtensionNode extensionNode;
    private final List<WriteableSetting> componentSettings;

    public AddSettingsUpdateConsumerRequest(DiscoveryExtensionNode extensionNode, List<Setting<?>> componentSettings) {
        this.extensionNode = extensionNode;
        this.componentSettings = new ArrayList<>(componentSettings.size());
        for (Setting<?> setting : componentSettings) {
            this.componentSettings.add(new WriteableSetting(setting));
        }
    }

    public AddSettingsUpdateConsumerRequest(StreamInput in) throws IOException {
        super(in);

        // Set extension node to send update settings request to
        this.extensionNode = new DiscoveryExtensionNode(in);

        // Read in component setting list
        int componentSettingsCount = in.readVInt();
        this.componentSettings = new ArrayList<>(componentSettingsCount);
        for (int i = 0; i < componentSettingsCount; i++) {
            this.componentSettings.add(new WriteableSetting(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        // Write extension node to stream output
        this.extensionNode.writeTo(out);

        // Write component setting list to stream output
        out.writeVInt(this.componentSettings.size());
        for (WriteableSetting componentSetting : this.componentSettings) {
            componentSetting.writeTo(out);
        }
    }

    public List<WriteableSetting> getComponentSettings() {
        return new ArrayList<>(this.componentSettings);
    }

    public DiscoveryExtensionNode getExtensionNode() {
        return this.extensionNode;
    }

    @Override
    public String toString() {
        return "AddSettingsUpdateConsumerRequest{extensionNode=" + this.extensionNode.toString() + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AddSettingsUpdateConsumerRequest that = (AddSettingsUpdateConsumerRequest) obj;
        return Objects.equals(extensionNode, that.extensionNode) && Objects.equals(componentSettings, that.componentSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extensionNode, componentSettings);
    }

}

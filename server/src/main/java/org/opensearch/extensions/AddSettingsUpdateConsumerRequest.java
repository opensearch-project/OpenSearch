/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;
import org.opensearch.common.settings.Setting;

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
    private static final Logger logger = LogManager.getLogger(AddSettingsUpdateConsumerRequest.class);
    private final DiscoveryExtension extensionNode;
    private final List<Setting> componentSettings;

    public AddSettingsUpdateConsumerRequest(DiscoveryExtension extensionNode, List<Setting> componentSettings) {
        this.extensionNode = extensionNode;
        this.componentSettings = componentSettings;
    }

    public AddSettingsUpdateConsumerRequest(StreamInput in) throws IOException {
        super(in);

        // Set extension node to send update settings request to
        this.extensionNode = new DiscoveryExtension(in);

        // Read in component setting list
        List<Setting> componentSettings = new ArrayList<>();
        int componentSettingsCount = in.readVInt();
        for (int i = 0; i < componentSettingsCount; i++) {
            // TODO : determine how to read Setting<T> object from stream input and add Setting<> object to List
        }
        this.componentSettings = componentSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        // Write extension node to stream output
        this.extensionNode.writeTo(out);

        // Write component setting list to stream output
        out.writeVInt(this.componentSettings.size());
        for (Setting componentSetting : this.componentSettings) {
            // TODO : determine how to write Setting<T> object to stream output
        }
    }

    public List<Setting> getComponentSettings() {
        return this.componentSettings;
    }

    public DiscoveryExtension getExtensionNode() {
        return this.extensionNode;
    }

    @Override
    public String toString() {
        return "AddSettingsUpdateConsumerRequest{extensionNode="
            + this.extensionNode.toString()
            + ", componentSettings="
            + this.componentSettings.toString()
            + "}";
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

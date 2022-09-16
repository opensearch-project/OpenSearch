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
import org.opensearch.common.settings.WriteableSetting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Environment Settings Request for Extensibility
 *
 * @opensearch.internal
 */
public class EnvironmentSettingsRequest extends TransportRequest {
    private static final Logger logger = LogManager.getLogger(EnvironmentSettingsRequest.class);
    private List<Setting<?>> componentSettings;

    public EnvironmentSettingsRequest(List<Setting<?>> componentSettings) {
        this.componentSettings = new ArrayList<>(componentSettings);
    }

    public EnvironmentSettingsRequest(StreamInput in) throws IOException {
        super(in);
        int componentSettingsCount = in.readVInt();
        List<Setting<?>> componentSettings = new ArrayList<>(componentSettingsCount);
        for (int i = 0; i < componentSettingsCount; i++) {
            WriteableSetting writeableSetting = new WriteableSetting(in);
            componentSettings.add(writeableSetting.getSetting());
        }
        this.componentSettings = componentSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(this.componentSettings.size());
        for (Setting<?> componentSetting : componentSettings) {
            new WriteableSetting(componentSetting).writeTo(out);
        }
    }

    public List<Setting<?>> getComponentSettings() {
        return new ArrayList<>(componentSettings);
    }

    @Override
    public String toString() {
        return "EnvironmentSettingsRequest{componentSettings=" + componentSettings.toString() + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EnvironmentSettingsRequest that = (EnvironmentSettingsRequest) obj;
        return Objects.equals(componentSettings, that.componentSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentSettings);
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.env;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Environment Settings Response for Extensibility
 *
 * @opensearch.internal
 */
public class EnvironmentSettingsResponse extends TransportResponse {
    private Map<String, String> componentSettingValues;

    public EnvironmentSettingsResponse(Settings environmentSettings, List<String> componentSettingKeys) {
        Map<String, String> componentSettingValues = new HashMap<>();
        for (String componentSettingKey : componentSettingKeys) {
            String componentSettingValue = environmentSettings.get(componentSettingKey);
            if (componentSettingValue != null) {
                componentSettingValues.put(componentSettingKey, componentSettingValue);
            }
        }
        this.componentSettingValues = componentSettingValues;
    }

    public EnvironmentSettingsResponse(StreamInput in) throws IOException {
        super(in);
        Map<String, String> componentSettingValues = new HashMap<>();
        int componentSettingValuesCount = in.readVInt();
        for (int i = 0; i < componentSettingValuesCount; i++) {
            String componentSettingKey = in.readString();
            String componentSettingValue = in.readString();
            componentSettingValues.put(componentSettingKey, componentSettingValue);
        }
        this.componentSettingValues = componentSettingValues;
    }

    public Map<String, String> getComponentSettingValues() {
        return Collections.unmodifiableMap(this.componentSettingValues);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(componentSettingValues.size());
        for (Map.Entry<String, String> componentSettings : componentSettingValues.entrySet()) {
            out.writeString(componentSettings.getKey());
            out.writeString(componentSettings.getValue());
        }
    }

    @Override
    public String toString() {
        return "EnvironmentSettingsResponse{" + "componentSettingValues=" + componentSettingValues.toString() + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnvironmentSettingsResponse that = (EnvironmentSettingsResponse) o;
        return Objects.equals(componentSettingValues, that.componentSettingValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentSettingValues);
    }
}

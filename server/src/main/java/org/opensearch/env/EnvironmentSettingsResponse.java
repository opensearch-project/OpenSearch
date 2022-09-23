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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.transport.TransportResponse;
import org.opensearch.common.settings.WriteableSetting;

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
    private Map<Setting<?>, Object> componentSettingValues;

    public EnvironmentSettingsResponse(Settings environmentSettings, List<Setting<?>> componentSettings) {
        Map<Setting<?>, Object> componentSettingValues = new HashMap<>();
        for (Setting<?> componentSetting : componentSettings) {

            // Retrieve component setting value from enviornment settings, or default value if it does not exist
            Object componentSettingValue = componentSetting.get(environmentSettings);
            componentSettingValues.put(componentSetting, componentSettingValue);
        }
        this.componentSettingValues = componentSettingValues;
    }

    public EnvironmentSettingsResponse(StreamInput in) throws IOException {
        super(in);
        Map<Setting<?>, Object> componentSettingValues = new HashMap<>();
        int componentSettingValuesCount = in.readVInt();
        for (int i = 0; i < componentSettingValuesCount; i++) {
            Setting<?> componentSetting = new WriteableSetting(in).getSetting();
            Object componentSettingValue = in.readGenericValue();
            componentSettingValues.put(componentSetting, componentSettingValue);
        }
        this.componentSettingValues = componentSettingValues;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(componentSettingValues.size());
        for (Map.Entry<Setting<?>, Object> entry : componentSettingValues.entrySet()) {
            new WriteableSetting(entry.getKey()).writeTo(out);
            out.writeGenericValue(entry.getValue());
        }
    }

    public Map<Setting<?>, Object> getComponentSettingValues() {
        return Collections.unmodifiableMap(this.componentSettingValues);
    }

    @Override
    public String toString() {
        return "EnvironmentSettingsResponse{componentSettingValues=" + componentSettingValues.toString() + '}';
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

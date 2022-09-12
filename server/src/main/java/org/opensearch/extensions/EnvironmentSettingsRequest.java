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
    private List<String> componentSettingKeys;

    public EnvironmentSettingsRequest(List<String> componentSettingKeys) {
        this.componentSettingKeys = componentSettingKeys;
    }

    public EnvironmentSettingsRequest(StreamInput in) throws IOException {
        super(in);
        List<String> componentSettingKeys = new ArrayList<String>();
        int componentSettingKeysCount = in.readVInt();
        for (int i = 0; i < componentSettingKeysCount; i++) {
            componentSettingKeys.add(in.readString());
        }
        this.componentSettingKeys = componentSettingKeys;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(this.componentSettingKeys.size());
        for (String key : componentSettingKeys) {
            out.writeString(key);
        }
    }

    public List<String> getComponentSettingKeys() {
        return this.componentSettingKeys;
    }

    @Override
    public String toString() {
        return "EnvironmentSettingsRequest{componentSettingKeys=" + componentSettingKeys.toString() + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EnvironmentSettingsRequest that = (EnvironmentSettingsRequest) obj;
        return Objects.equals(componentSettingKeys, that.componentSettingKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentSettingKeys);
    }

}

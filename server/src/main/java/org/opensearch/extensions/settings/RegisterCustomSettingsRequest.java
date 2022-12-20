/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.settings;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.WriteableSetting;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Request to register a list of custom extension settings
 *
 * @opensearch.internal
 */
public class RegisterCustomSettingsRequest extends TransportRequest {
    private String uniqueId;
    private List<Setting<?>> settings;

    public RegisterCustomSettingsRequest(String uniqueId, List<Setting<?>> settings) {
        this.uniqueId = uniqueId;
        this.settings = new ArrayList<>(settings);
    }

    public RegisterCustomSettingsRequest(StreamInput in) throws IOException {
        super(in);
        this.uniqueId = in.readString();
        int size = in.readVInt();
        this.settings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            WriteableSetting ws = new WriteableSetting(in);
            this.settings.add(ws.getSetting());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(uniqueId);
        out.writeVInt(settings.size());
        for (Setting<?> setting : settings) {
            new WriteableSetting(setting).writeTo(out);
        }
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public List<Setting<?>> getSettings() {
        return new ArrayList<>(settings);
    }

    @Override
    public String toString() {
        return "RegisterSettingsRequest{uniqueId=" + uniqueId + ", settings=" + settings + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RegisterCustomSettingsRequest that = (RegisterCustomSettingsRequest) obj;
        return Objects.equals(uniqueId, that.uniqueId) && Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueId, settings);
    }
}

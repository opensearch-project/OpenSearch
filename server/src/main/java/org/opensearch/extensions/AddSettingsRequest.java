/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

import java.util.Objects;

/**
 * AddSetting Request for AD creating a detector
 *
 * @opensearch.internal
 */
public class AddSettingsRequest extends TransportRequest {
    String setting;

    public AddSettingsRequest(String setting) {
        this.setting = setting;
    }

    public AddSettingsRequest(StreamInput in) throws IOException {
        super(in);
        this.setting = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(setting);
    }

    public String getSettings() {
        return this.setting;
    }

    @Override
    public String toString() {
        return "AddSettingsRequest{" + "settings=" + setting + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AddSettingsRequest that = (AddSettingsRequest) o;
        return Objects.equals(setting, that.setting);
    }

    @Override
    public int hashCode() {
        return Objects.hash(setting);
    }
}


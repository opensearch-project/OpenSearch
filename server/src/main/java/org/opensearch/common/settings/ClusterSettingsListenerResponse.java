/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * ClusterSettings Response for Extensibility
 *
 * @opensearch.internal
 */
public class ClusterSettingsListenerResponse extends TransportResponse {
    private boolean addSettingsUpdateConsumer;

    public ClusterSettingsListenerResponse(boolean addSettingsUpdateConsumer) {
        this.addSettingsUpdateConsumer = addSettingsUpdateConsumer;
    }

    public ClusterSettingsListenerResponse(StreamInput in) throws IOException {
        this.addSettingsUpdateConsumer = in.readBoolean();
    }

    public boolean getAddSettingsUpdateConsumer() {
        return this.addSettingsUpdateConsumer;
    }

    @Override
    public String toString() {
        return "ClusterSettingsResponse{"
            + "addSettingsUpdateConsumer"
            + addSettingsUpdateConsumer
            + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterSettingsListenerResponse that = (ClusterSettingsListenerResponse) o;
        return Objects.equals(addSettingsUpdateConsumer, that.addSettingsUpdateConsumer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(addSettingsUpdateConsumer);
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(addSettingsUpdateConsumer);
    }
}

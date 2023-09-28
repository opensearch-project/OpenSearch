/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * PluginResponse to intialize plugin
 *
 * @opensearch.internal
 */
public class InitializeExtensionSecurityResponse extends TransportResponse {
    private String name;

    public InitializeExtensionSecurityResponse(String name) {
        this.name = name;
    }

    public InitializeExtensionSecurityResponse(StreamInput in) throws IOException {
        name = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
    }

    /**
     * @return the node that is currently leading, according to the responding node.
     */

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return "InitializeExtensionResponse{" + "name = " + name + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InitializeExtensionSecurityResponse that = (InitializeExtensionSecurityResponse) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}

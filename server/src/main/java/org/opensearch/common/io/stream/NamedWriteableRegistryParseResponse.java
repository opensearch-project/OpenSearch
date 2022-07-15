/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io.stream;

import org.opensearch.transport.TransportResponse;
import java.io.IOException;
import java.util.Objects;

/**
 * Extensibility support for Named Writeable Registry: parse response from extensions
 *
 * @opensearch.internal
 */
public class NamedWriteableRegistryParseResponse extends TransportResponse {

    private final boolean status;

    public NamedWriteableRegistryParseResponse(boolean status) {
        this.status = status;
    }

    public NamedWriteableRegistryParseResponse(StreamInput in) throws IOException {
        this.status = in.readBoolean();
    }

    public boolean getStatus() {
        return this.status;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(status);
    }

    @Override
    public String toString() {
        return "NamedWriteableRegistryParseResponse{" + "status=" + this.status + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedWriteableRegistryParseResponse that = (NamedWriteableRegistryParseResponse) o;
        return Objects.equals(this.status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }

}

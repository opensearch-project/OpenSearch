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

    private final String data;

    public NamedWriteableRegistryParseResponse(String data) {
        this.data = data;
    }

    public NamedWriteableRegistryParseResponse(StreamInput in) throws IOException {
        this.data = in.readString();
    }

    public String getData() {
        return this.data;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(data);
    }

    @Override
    public String toString() {
        return "NamedWriteableRegistryParseResponse{" + "data=" + this.data + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedWriteableRegistryParseResponse that = (NamedWriteableRegistryParseResponse) o;
        return Objects.equals(this.data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

}

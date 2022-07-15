/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io.stream;

import org.opensearch.transport.TransportRequest;
import java.io.IOException;
import java.util.Objects;

/**
 * Extensibility support for Named Writeable Registry: Request to SDK to send registry entries
 *
 * @opensearch.internal
 */
public class NamedWriteableRegistryRequest extends TransportRequest {

    private boolean flag;

    public NamedWriteableRegistryRequest(boolean flag) {
        this.flag = flag;
    }

    public NamedWriteableRegistryRequest(StreamInput in) throws IOException {
        super(in);
        this.flag = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(flag);
    }

    @Override
    public String toString() {
        return "NamedWriteableRegistryRequest{" + "flag=" + flag + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedWriteableRegistryRequest that = (NamedWriteableRegistryRequest) o;
        return Objects.equals(flag, that.flag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flag);
    }
}

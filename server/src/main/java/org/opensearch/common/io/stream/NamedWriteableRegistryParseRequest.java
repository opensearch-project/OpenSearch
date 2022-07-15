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
 * Extensibility support for Named Writeable Registry: request to extensions to parse context
 *
 * @opensearch.internal
 */
public class NamedWriteableRegistryParseRequest extends TransportRequest {

    private String categoryClassName;
    private byte[] context;

    public NamedWriteableRegistryParseRequest(String categoryClassName, byte[] context) {
        this.categoryClassName = categoryClassName;
        this.context = context;
    }

    public NamedWriteableRegistryParseRequest(StreamInput in) throws IOException {
        super(in);
        this.categoryClassName = in.readString();
        this.context = in.readByteArray();
    }

    public Class<?> getCategoryClass() throws ClassNotFoundException {
        Class<?> categoryClass = Class.forName(this.categoryClassName);
        return categoryClass;
    }

    public byte[] getContext() {
        return this.context;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(categoryClassName);
        out.writeByteArray(context);
    }

    @Override
    public String toString() {
        return "NamedWriteableRegistryParseRequest{"
            + "categoryClassName="
            + categoryClassName
            + ", context length="
            + context.length
            + " }";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedWriteableRegistryParseRequest that = (NamedWriteableRegistryParseRequest) o;
        return Objects.equals(categoryClassName, that.categoryClassName) && Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(categoryClassName, context);
    }
}

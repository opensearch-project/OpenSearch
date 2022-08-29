/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;
import java.io.IOException;
import java.util.Objects;

/**
 * Extensibility support for Named XContent Registry: request to extensions to parse context
 *
 * @opensearch.internal
 */
public class NamedXContentRegistryParseRequest extends TransportRequest {

    private final Class categoryClass;
    private final String context;

    /**
     * @param categoryClass Class category for this parse request
     * @param context String to transport to the extension
     */
    public NamedXContentRegistryParseRequest(Class categoryClass, String context) {
        this.categoryClass = categoryClass;
        this.context = context;
    }

    /**
     * @param in StreamInput from which class fields are read from
     * @throws IllegalArgumentException if the fully qualified class name is invalid and the class object cannot be generated at runtime
     */
    public NamedXContentRegistryParseRequest(StreamInput in) throws IOException {
        super(in);
        try {
            this.categoryClass = Class.forName(in.readString());
            this.context = in.readOptionalString();
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Category class definition not found", e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(categoryClass.getName());
        out.writeOptionalString(context);
    }

    @Override
    public String toString() {
        return "NamedXContentRegistryParseRequest{" + "categoryClass=" + categoryClass.getName() + ", context=" + context + " }";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedXContentRegistryParseRequest that = (NamedXContentRegistryParseRequest) o;
        return Objects.equals(categoryClass, that.categoryClass) && Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(categoryClass, context);
    }

    /**
     * Returns the class instance of the category class sent over by the SDK
     */
    public Class getCategoryClass() {
        return this.categoryClass;
    }

    /**
     * Returns the string context of this request
     */
    public String getContext() {
        return this.context;
    }
}

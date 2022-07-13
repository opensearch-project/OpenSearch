/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class DecommissionedAttributeMetadata implements Writeable {

    private final String name;
    private final List<String> values;

    public DecommissionedAttributeMetadata(DecommissionedAttributeMetadata metadata, List<String> values) {
        this(metadata.name, values);
    }

    /**
     * Constructs new decommissioned attributes metadata
     *
     * @param name   attribute name
     * @param values attribute values
     */
    public DecommissionedAttributeMetadata(String name, List<String> values) {
        this.name = name;
        this.values = values;
    }

    /**
     * Returns attribute name
     *
     * @return attribute name
     */
    public String name() {
        return this.name;
    }

    /**
     * Returns attribute value
     *
     * @return attribute value
     */
    public List<String> values() {
        return this.values;
    }

    public DecommissionedAttributeMetadata(StreamInput in) throws IOException {
        name = in.readString();
        values = in.readStringList();
    }

    /**
     * Writes decommissioned attribute metadata to stream output
     *
     * @param out stream output
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringCollection(values);
    }

    /**
     * Checks if this instance is equal to the other instance in name other than {@link #values}.
     *
     * @param other other decommissioned attribute metadata
     * @return {@code true} if both instances equal in all fields but the values fields
     */
    public boolean equalsIgnoreValues(DecommissionedAttributeMetadata other) {
        return name.equals(other.name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DecommissionedAttributeMetadata that = (DecommissionedAttributeMetadata) o;

        if (!name.equals(that.name)) return false;
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, values);
    }

    @Override
    public String toString() {
        return "DecommissionedAttributeMetadata{" + name + "}{" + values().toString() + "}";
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public final class DecommissionedAttribute implements Writeable {
    private final String key;
    private final List<String> values;

    public DecommissionedAttribute(DecommissionedAttribute decommissionedAttribute, List<String> values) {
        this(decommissionedAttribute.key, values);
    }

    /**
     * Constructs new decommission attribute key value pair
     *
     * @param key    attribute name
     * @param values attribute values
     */
    public DecommissionedAttribute(String key, List<String> values) {
        this.key = key;
        this.values = values;
    }

    /**
     * Returns attribute key
     *
     * @return attribute key
     */
    public String key() {
        return this.key;
    }

    /**
     * Returns attribute value
     *
     * @return attribute value
     */
    public List<String> values() {
        return this.values;
    }

    public DecommissionedAttribute(StreamInput in) throws IOException {
        key = in.readString();
        values = in.readStringList();
    }

    /**
     * Writes decommission attribute key values to stream output
     *
     * @param out stream output
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key);
        out.writeStringCollection(values);
    }

    /**
     * Checks if this instance is equal to the other instance in key other than {@link #values}.
     *
     * @param other other decommission attribute key values
     * @return {@code true} if both instances equal in key fields but the values fields
     */
    public boolean equalsIgnoreValues(DecommissionedAttribute other) {
        return key.equals(other.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DecommissionedAttribute that = (DecommissionedAttribute) o;

        if (!key.equals(that.key)) return false;
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, values);
    }

    @Override
    public String toString() {
        return "DecommissionedAttribute{" + key + "}{" + values().toString() + "}";
    }
}

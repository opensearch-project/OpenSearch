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

public final class DecommissionAttribute implements Writeable {
    private final String attributeName;
    private final List<String> attributeValues;

    /**
     * Update the attribute values for a given attribute name to decommission
     *
     * @param decommissionAttribute current decommissioned attribute object
     * @param attributeValues       values to be updated with
     */
    public DecommissionAttribute(DecommissionAttribute decommissionAttribute, List<String> attributeValues) {
        this(decommissionAttribute.attributeName, attributeValues);
    }

    /**
     * Constructs new decommission attribute name values pair
     *
     * @param attributeName   attribute name
     * @param attributeValues attribute values
     */
    public DecommissionAttribute(String attributeName, List<String> attributeValues) {
        this.attributeName = attributeName;
        this.attributeValues = attributeValues;
    }

    /**
     * Returns attribute name
     *
     * @return attributeName
     */
    public String attributeName() {
        return this.attributeName;
    }

    /**
     * Returns attribute values
     *
     * @return attributeValues
     */
    public List<String> attributeValues() {
        return this.attributeValues;
    }

    public DecommissionAttribute(StreamInput in) throws IOException {
        attributeName = in.readString();
        attributeValues = in.readStringList();
    }

    /**
     * Writes decommission attribute name values to stream output
     *
     * @param out stream output
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(attributeName);
        out.writeStringCollection(attributeValues);
    }

    /**
     * Checks if this instance is equal to the other instance in attributeName other than {@link #attributeValues}.
     *
     * @param other other decommission attribute name values
     * @return {@code true} if both instances equal in attributeName fields but the attributeValues fields
     */
    public boolean equalsIgnoreValues(DecommissionAttribute other) {
        return attributeName.equals(other.attributeName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DecommissionAttribute that = (DecommissionAttribute) o;

        if (!attributeName.equals(that.attributeName)) return false;
        return attributeValues.equals(that.attributeValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeName, attributeValues);
    }

    @Override
    public String toString() {
        return "DecommissionAttribute{" + attributeName + "}{" + attributeValues().toString() + "}";
    }
}

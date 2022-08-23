/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link DecommissionAttribute} encapsulates information about decommissioned node attribute like attribute name, attribute value.
 *
 * @opensearch.internal
 */
public final class DecommissionAttribute implements Writeable {
    private final String attributeName;
    private final String attributeValue;

    /**
     * Construct new decommission attribute with updated value from a given decommission attribute
     *
     * @param decommissionAttribute current decommissioned attribute object
     * @param attributeValue       attribute value to be updated with
     */
    public DecommissionAttribute(DecommissionAttribute decommissionAttribute, String attributeValue) {
        this(decommissionAttribute.attributeName, attributeValue);
    }

    /**
     * Constructs new decommission attribute name value pair
     *
     * @param attributeName   attribute name
     * @param attributeValue attribute value
     */
    public DecommissionAttribute(String attributeName, String attributeValue) {
        this.attributeName = attributeName;
        this.attributeValue = attributeValue;
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
     * Returns attribute value
     *
     * @return attributeValue
     */
    public String attributeValue() {
        return this.attributeValue;
    }

    public DecommissionAttribute(StreamInput in) throws IOException {
        attributeName = in.readString();
        attributeValue = in.readString();
    }

    /**
     * Writes decommission attribute name value to stream output
     *
     * @param out stream output
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(attributeName);
        out.writeString(attributeValue);
    }

    /**
     * Checks if this instance is equal to the other instance in attributeName but differ in attribute value {@link #attributeValue}.
     *
     * @param other other decommission attribute name value
     * @return {@code true} if both instances equal in attributeName fields but the attributeValue field
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
        return attributeValue.equals(that.attributeValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeName, attributeValue);
    }

    @Override
    public String toString() {
        return "DecommissionAttribute{" + "attributeName='" + attributeName + '\'' + ", attributeValue='" + attributeValue + '\'' + '}';
    }
}

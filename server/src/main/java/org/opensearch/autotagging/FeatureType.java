/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.common.ValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Represents a feature type in the auto-tagging feature. Implementations of this interface are responsible
 * for registering feature types in {@link AutoTaggingRegistry}.
 * @opensearch.experimental
 */
public interface FeatureType extends Writeable {
    String getName();

    int getMaxNumberOfValuesPerAttribute();

    int getMaxCharLengthPerAttributeValue();

    Set<Attribute> getAllowedAttributes();

    void registerFeatureType();

    default boolean isValidAttribute(Attribute attribute) {
        return getAllowedAttributes().contains(attribute);
    }

    default void validateAttributeMap(Map<Attribute, Set<String>> attributeMap, ValidationException validationException) {
        for (Map.Entry<Attribute, Set<String>> entry : attributeMap.entrySet()) {
            Attribute attribute = entry.getKey();
            Set<String> attributeValues = entry.getValue();
            if (getAttributeFromName(attribute.getName()) == null) {
                validationException.addValidationError(
                    attribute.getName() + " is not a valid attribute within the " + getName() + " feature."
                );
            }
            int maxValues = getMaxNumberOfValuesPerAttribute();
            if (attributeValues.size() > maxValues) {
                validationException.addValidationError(
                    "Each attribute can only have a maximum of "
                        + maxValues
                        + " values. The input attribute "
                        + attribute
                        + " exceeds this limit."
                );
            }
            int maxValueLength = getMaxCharLengthPerAttributeValue();
            for (String attributeValue : attributeValues) {
                if (attributeValue.isEmpty() || attributeValue.length() > maxValueLength) {
                    validationException.addValidationError(
                        "Attribute value [" + attributeValue + "] is invalid (empty or exceeds " + maxValueLength + " characters)"
                    );
                }
            }
        }
    }

    default Attribute getAttributeFromName(String fieldName) {
        return getAllowedAttributes().stream().filter(attr -> attr.getName().equals(fieldName)).findFirst().orElse(null);
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        out.writeString(getClass().getName());
        out.writeString(getName());
    }

    static FeatureType from(StreamInput in) throws IOException {
        return AutoTaggingRegistry.getFeatureType(in.readString(), in.readString());
    }
}

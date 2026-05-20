/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.autotagging;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a feature type within the auto-tagging feature. Feature types define different categories of
 * characteristics that can be used for tagging and classification. Implementations must ensure that
 * feature types are uniquely identifiable by their class and name.
 *
 * Implementers should follow these guidelines:
 * Feature types should be singletons and managed centrally to avoid duplicates.
 *
 * @opensearch.experimental
 */
public interface FeatureType extends Writeable {
    /**
     * Default value for max attribute values
     */
    int DEFAULT_MAX_ATTRIBUTE_VALUES = 10;
    /**
     *  Default value for max number of chars in a single value
     */
    int DEFAULT_MAX_ATTRIBUTE_VALUE_LENGTH = 100;

    /**
     * Returns name
     * @return
     */
    String getName();

    /**
     * Returns a map of top-level attributes sorted by priority, with 1 representing the highest priority.
     * Subfields within each attribute are managed separately here {@link Attribute#getWeightedSubfields()}.
     */
    Map<Attribute, Integer> getOrderedAttributes();

    /**
     * Returns the registry of allowed attributes for this feature type.
     * Implementations must ensure that access to this registry is thread-safe.
     */
    default Map<String, Attribute> getAllowedAttributesRegistry() {
        return getOrderedAttributes().keySet().stream().collect(Collectors.toUnmodifiableMap(Attribute::getName, attribute -> attribute));
    }

    /**
     * returns the validator for feature value
     */
    default FeatureValueValidator getFeatureValueValidator() {
        return new FeatureValueValidator() {
            @Override
            public void validate(String featureValue) {}
        };
    }

    /**
     * returns max attribute values
     * @return
     */
    default int getMaxNumberOfValuesPerAttribute() {
        return DEFAULT_MAX_ATTRIBUTE_VALUES;
    }

    /**
     * returns value for max number of chars in a single value
     * @return
     */
    default int getMaxCharLengthPerAttributeValue() {
        return DEFAULT_MAX_ATTRIBUTE_VALUE_LENGTH;
    }

    /**
     * checks the validity of the input attribute
     * @param attribute
     * @return
     */
    default boolean isValidAttribute(Attribute attribute) {
        return getAllowedAttributesRegistry().containsValue(attribute);
    }

    /**
     * Retrieves an attribute by its name from the allowed attributes' registry.
     * Implementations must ensure that this method is thread-safe.
     * @param name The name of the attribute.
     */
    default Attribute getAttributeFromName(String name) {
        return getAllowedAttributesRegistry().get(name);
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        out.writeString(getName());
    }

    /**
     * parses the FeatureType using StreamInput
     * @param in
     * @return
     * @throws IOException
     */
    static FeatureType from(StreamInput in) throws IOException {
        return AutoTaggingRegistry.getFeatureType(in.readString());
    }

    /**
     * Returns the instance for the passed param
     * @param name
     * @return
     */
    static FeatureType from(String name) {
        return AutoTaggingRegistry.getFeatureType(name);
    }
}

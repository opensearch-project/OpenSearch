/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;

/**
 * Represents a feature type within the auto-tagging feature. Feature types define different categories of
 * characteristics that can be used for tagging and classification. Implementations of this interface are
 * responsible for registering feature types in {@link AutoTaggingRegistry}. Implementations must ensure that
 * feature types are uniquely identifiable by their class and name.
 *
 * Implementers should follow these guidelines:
 * Feature types should be singletons and managed centrally to avoid duplicates.
 * {@link #registerFeatureType()} must be called during initialization to ensure the feature type is available.
 *
 * @opensearch.experimental
 */
public interface FeatureType extends Writeable {
    int DEFAULT_MAX_ATTRIBUTE_VALUES = 10;
    int DEFAULT_MAX_ATTRIBUTE_VALUE_LENGTH = 100;

    String getName();

    /**
     * Returns the registry of allowed attributes for this feature type.
     * Implementations must ensure that access to this registry is thread-safe.
     */
    Map<String, Attribute> getAllowedAttributesRegistry();

    default int getMaxNumberOfValuesPerAttribute() {
        return DEFAULT_MAX_ATTRIBUTE_VALUES;
    }

    default int getMaxCharLengthPerAttributeValue() {
        return DEFAULT_MAX_ATTRIBUTE_VALUE_LENGTH;
    }

    void registerFeatureType();

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

    static FeatureType from(StreamInput in) throws IOException {
        return AutoTaggingRegistry.getFeatureType(in.readString());
    }
}

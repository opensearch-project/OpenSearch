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
import java.util.Set;

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
    String getName();

    int getMaxNumberOfValuesPerAttribute();

    int getMaxCharLengthPerAttributeValue();

    Set<Attribute> getAllowedAttributes();

    void registerFeatureType();

    default boolean isValidAttribute(Attribute attribute) {
        return getAllowedAttributes().contains(attribute);
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

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

/**
 * Represents an attribute within the auto-tagging feature. Attributes define characteristics that can
 * be used for tagging and classification. Implementations must ensure that attributes
 * are uniquely identifiable by their name. Attributes should be singletons and managed centrally to
 * avoid duplicates.
 *
 * @opensearch.experimental
 */
public interface Attribute extends Writeable {
    String getName();

    /**
     * Ensure that `validateAttribute` is called in the constructor of attribute implementations
     * to prevent potential serialization issues.
     */
    default void validateAttribute() {
        String name = getName();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Attribute name cannot be null or empty");
        }
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        out.writeString(getName());
    }

    /**
     * Retrieves an attribute from the given feature type based on its name.
     * Implementations of `FeatureType.getAttributeFromName` must be thread-safe as this method
     * may be called concurrently.
     * @param in - the {@link StreamInput} from which the attribute name is read
     * @param featureType - the FeatureType used to look up the attribute
     */
    static Attribute from(StreamInput in, FeatureType featureType) throws IOException {
        String attributeName = in.readString();
        Attribute attribute = featureType.getAttributeFromName(attributeName);
        if (attribute == null) {
            throw new IllegalStateException(attributeName + " is not a valid attribute under feature type " + featureType.getName());
        }
        return attribute;
    }
}

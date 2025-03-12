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
 * be used for tagging and classification. Implementations Implementations must ensure that attributes
 * are uniquely identifiable by their name. Attributes should be singletons and managed centrally to
 * avoid duplicates.
 *
 * @opensearch.experimental
 */
public interface Attribute extends Writeable {
    String getName();

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        out.writeString(getName());
    }

    static Attribute from(StreamInput in, FeatureType featureType) throws IOException {
        String attributeName = in.readString();
        Attribute attribute = featureType.getAttributeFromName(attributeName);
        if (attribute == null) {
            throw new IllegalStateException(attributeName + " is not a valid attribute under feature type " + featureType.getName());
        }
        return attribute;
    }
}

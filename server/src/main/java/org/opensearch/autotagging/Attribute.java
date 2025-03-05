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
 * be used for tagging and classification. Implementations of this interface are responsible for
 * registering attributes in {@link AutoTaggingRegistry}. Implementations must ensure that attributes
 * are uniquely identifiable by their class and name.
 *
 * Implementers should follow these guidelines:
 * Attributes should be singletons and managed centrally to avoid duplicates.
 * {@link #registerAttribute()} must be called during initialization to ensure the attribute is available.
 *
 * @opensearch.experimental
 */
public interface Attribute extends Writeable {
    String getName();

    void registerAttribute();

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        out.writeString(getClass().getName());
        out.writeString(getName());
    }

    static Attribute from(StreamInput in) throws IOException {
        return AutoTaggingRegistry.getAttribute(in.readString(), in.readString());
    }
}

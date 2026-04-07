/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Set;

/**
 * Declares that a backend can evaluate a specific {@link WindowFunction}
 * on a specific {@link FieldType} in the given data formats.
 *
 * <p>Flat record because all subcategories share the same shape. The category
 * lives on {@link WindowFunction#getType()}. Per-type factory methods
 * validate the function type at construction.
 *
 * @opensearch.internal
 */
public record WindowCapability(WindowFunction function, FieldType fieldType,
                                Set<String> formats) {

    public WindowCapability {
        formats = Set.copyOf(formats);
    }

    public static WindowCapability ranking(WindowFunction function, FieldType fieldType,
                                            Set<String> formats) {
        assert function.getType() == WindowFunction.Type.RANKING;
        return new WindowCapability(function, fieldType, formats);
    }

    public static WindowCapability value(WindowFunction function, FieldType fieldType,
                                          Set<String> formats) {
        assert function.getType() == WindowFunction.Type.VALUE;
        return new WindowCapability(function, fieldType, formats);
    }
}

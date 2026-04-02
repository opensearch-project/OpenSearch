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
 * Declares that a backend can evaluate a specific {@link AggregateFunction}
 * on a specific {@link FieldType} in the given data formats.
 *
 * <p>Flat record because all subcategories share the same shape. The category
 * lives on {@link AggregateFunction#getType()}. Per-type factory methods
 * validate the function type at construction and make backend declarations
 * self-documenting.
 *
 * @opensearch.internal
 */
public record AggregateCapability(AggregateFunction function, FieldType fieldType,
                                   Set<String> formats) {

    public AggregateCapability {
        formats = Set.copyOf(formats);
    }

    public static AggregateCapability simple(AggregateFunction function, FieldType fieldType,
                                              Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.SIMPLE;
        return new AggregateCapability(function, fieldType, formats);
    }

    public static AggregateCapability statistical(AggregateFunction function, FieldType fieldType,
                                                   Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.STATISTICAL;
        return new AggregateCapability(function, fieldType, formats);
    }

    public static AggregateCapability stateExpanding(AggregateFunction function, FieldType fieldType,
                                                      Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.STATE_EXPANDING;
        return new AggregateCapability(function, fieldType, formats);
    }

    public static AggregateCapability approximate(AggregateFunction function, FieldType fieldType,
                                                    Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.APPROXIMATE;
        return new AggregateCapability(function, fieldType, formats);
    }
}

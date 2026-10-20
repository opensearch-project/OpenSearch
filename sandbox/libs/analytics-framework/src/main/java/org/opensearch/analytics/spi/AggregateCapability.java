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
 * <p>Decomposition of partial/final aggregate pairs is handled uniformly, outside
 * this record:
 * <ul>
 *   <li>Multi-field primitive decomposition (AVG / STDDEV / VAR) runs in HEP via
 *       {@code OpenSearchAggregateReduceRule}.</li>
 *   <li>Single-field pass-through / function-swap / engine-native reductions run in
 *       {@code AggregateDecompositionResolver} using
 *       {@link AggregateFunction#intermediateFields()} as the sole source of truth.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public record AggregateCapability(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {

    public static AggregateCapability simple(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.SIMPLE;
        return new AggregateCapability(function, fieldTypes, formats);
    }

    public static AggregateCapability statistical(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.STATISTICAL;
        return new AggregateCapability(function, fieldTypes, formats);
    }

    public static AggregateCapability stateExpanding(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.STATE_EXPANDING;
        return new AggregateCapability(function, fieldTypes, formats);
    }

    public static AggregateCapability approximate(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.APPROXIMATE;
        return new AggregateCapability(function, fieldTypes, formats);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.Nullable;

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
 * <p>{@link #decomposition()} is null for most functions — the planner applies
 * Calcite's standard decomposition (AVG → SUM/COUNT, STDDEV → SUM(x²)+SUM(x)+COUNT).
 * Backends with non-standard partial state (e.g. HLL sketches, Welford STDDEV)
 * provide a custom {@link AggregateDecomposition}.
 *
 * <p>TODO (plan forking): during resolution of a plan alternative, after a single
 * backend is chosen for an aggregate operator, apply decomposition as a paired
 * rewrite of PARTIAL output schema + FINAL input schema:
 * <ol>
 *   <li>If decomposition == null: apply Calcite's AggregateReduceFunctionsRule
 *       to the PARTIAL+FINAL pair.</li>
 *   <li>If decomposition != null: use decomposition.partialCalls() to rewrite
 *       PARTIAL's aggCalls and output row type, then use decomposition.finalExpression()
 *       to rewrite FINAL's aggCalls. Both must be updated together — the exchange
 *       row type between them must be consistent.</li>
 * </ol>
 *
 * @opensearch.internal
 */
public record AggregateCapability(AggregateFunction function, FieldType fieldType,
                                   Set<String> formats, @Nullable AggregateDecomposition decomposition) {

    public AggregateCapability {
        formats = Set.copyOf(formats);
    }

    /** Convenience constructor with no custom decomposition (uses Calcite's standard). */
    public AggregateCapability(AggregateFunction function, FieldType fieldType, Set<String> formats) {
        this(function, fieldType, formats, null);
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

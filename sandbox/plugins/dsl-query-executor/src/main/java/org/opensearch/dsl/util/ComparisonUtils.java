/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.util;

import java.util.Objects;

/**
 * Utility methods for comparing values.
 */
public final class ComparisonUtils {

    private ComparisonUtils() {}

    /**
     * Compares two values for equality, coercing numeric types (the engine may box the same
     * column differently across plans). Integral pairs compare as long to avoid precision
     * loss; floating-point pairs via {@link Double#compare}. No cross-type string coercion —
     * bucket keys are typed by the schema.
     */
    public static boolean valuesEqual(Object a, Object b) {
        if (Objects.equals(a, b)) return true;

        if (a instanceof Number na && b instanceof Number nb) {
            if (isIntegral(na) && isIntegral(nb)) {
                return na.longValue() == nb.longValue();
            }
            return Double.compare(na.doubleValue(), nb.doubleValue()) == 0;
        }

        return false;
    }

    private static boolean isIntegral(Number n) {
        return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
    }
}

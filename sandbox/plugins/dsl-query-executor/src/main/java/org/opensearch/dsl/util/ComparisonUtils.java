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
     * Compares two values for equality, handling type coercion for numbers and strings.
     */
    public static boolean valuesEqual(Object a, Object b) {
        if (Objects.equals(a, b)) return true;

        if (a == null || b == null) return false;

        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }

        return a.toString().equals(b.toString());
    }
}

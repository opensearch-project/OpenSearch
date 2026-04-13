/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.util;

/**
 * Utility methods for comparing values.
 */
public final class ComparisonUtils {
    
    private ComparisonUtils() {}
    
    /**
     * Compares two values for equality, handling type differences by falling back to string comparison.
     */
    public static boolean valuesEqual(Object a, Object b) {
        if (a == null) return b == null;
        if (b == null) return false;
        return a.equals(b) || a.toString().equals(b.toString());
    }
}

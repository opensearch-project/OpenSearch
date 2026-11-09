/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.util;

import org.opensearch.test.OpenSearchTestCase;

public class ComparisonUtilsTests extends OpenSearchTestCase {

    public void testBothNull() {
        assertTrue(ComparisonUtils.valuesEqual(null, null));
    }

    public void testFirstNull() {
        assertFalse(ComparisonUtils.valuesEqual(null, "value"));
    }

    public void testSecondNull() {
        assertFalse(ComparisonUtils.valuesEqual("value", null));
    }

    public void testEqualStrings() {
        assertTrue(ComparisonUtils.valuesEqual("test", "test"));
    }

    public void testDifferentValues() {
        assertFalse(ComparisonUtils.valuesEqual("foo", "bar"));
    }

    public void testIntegerAndLong() {
        assertTrue(ComparisonUtils.valuesEqual(42, 42L));
    }

    public void testIntegerAndDouble() {
        assertTrue(ComparisonUtils.valuesEqual(42, 42.0));
    }

    public void testDoubleAndFloat() {
        assertTrue(ComparisonUtils.valuesEqual(42.0, 42.0f));
    }

    public void testDifferentNumbers() {
        assertFalse(ComparisonUtils.valuesEqual(42, 43));
    }

    public void testNumberAndStringAreNotEqual() {
        // No cross-type stringification: bucket keys are schema-typed, "42" != 42.
        assertFalse(ComparisonUtils.valuesEqual(42, "42"));
    }

    public void testLargeLongsBeyondDoublePrecision() {
        // 2^53 + 1 vs 2^53: indistinguishable as doubles, distinct as longs.
        assertFalse(ComparisonUtils.valuesEqual(9007199254740993L, 9007199254740992L));
        assertTrue(ComparisonUtils.valuesEqual(9007199254740993L, 9007199254740993L));
    }

    public void testBooleanComparison() {
        assertTrue(ComparisonUtils.valuesEqual(true, true));
        assertFalse(ComparisonUtils.valuesEqual(true, false));
    }
}

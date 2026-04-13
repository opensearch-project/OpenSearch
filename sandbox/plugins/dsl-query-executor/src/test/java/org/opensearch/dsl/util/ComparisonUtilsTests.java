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

    public void testSameObject() {
        String value = "test";
        assertTrue(ComparisonUtils.valuesEqual(value, value));
    }

    public void testEqualStrings() {
        assertTrue(ComparisonUtils.valuesEqual("test", "test"));
    }

    public void testEqualIntegers() {
        assertTrue(ComparisonUtils.valuesEqual(42, 42));
    }

    public void testDifferentTypes() {
        assertTrue(ComparisonUtils.valuesEqual(42, "42"));
    }

    public void testDifferentValues() {
        assertFalse(ComparisonUtils.valuesEqual("foo", "bar"));
    }

    public void testIntegerAndLong() {
        assertTrue(ComparisonUtils.valuesEqual(42, 42L));
    }

    public void testDoubleAndString() {
        assertTrue(ComparisonUtils.valuesEqual(3.14, "3.14"));
    }
}

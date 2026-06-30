/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.util;

import org.opensearch.test.OpenSearchTestCase;

public class ProtobufEnumUtilsTests extends OpenSearchTestCase {

    // Test enum to simulate protobuf enum behavior
    private enum TestEnum {
        TEST_ENUM_VALUE_ONE,
        TEST_ENUM_VALUE_TWO,
        SIMPLE_VALUE
    }

    // Another test enum with different naming pattern
    private enum SortOrder {
        SORT_ORDER_ASC,
        SORT_ORDER_DESC
    }

    public void testConvertToStringWithNull() {
        // Test null input
        String result = ProtobufEnumUtils.convertToString(null);
        assertNull("Should return null for null input", result);
    }

    public void testConvertToStringWithPrefix() {
        // Test enum with prefix that matches class name pattern
        String result = ProtobufEnumUtils.convertToString(SortOrder.SORT_ORDER_ASC);
        assertEquals("Should remove prefix and convert to lowercase", "asc", result);

        result = ProtobufEnumUtils.convertToString(SortOrder.SORT_ORDER_DESC);
        assertEquals("Should remove prefix and convert to lowercase", "desc", result);
    }

    public void testConvertToStringWithComplexPrefix() {
        // Test enum with more complex prefix
        String result = ProtobufEnumUtils.convertToString(TestEnum.TEST_ENUM_VALUE_ONE);
        assertEquals("Should remove prefix and convert to lowercase", "value_one", result);

        result = ProtobufEnumUtils.convertToString(TestEnum.TEST_ENUM_VALUE_TWO);
        assertEquals("Should remove prefix and convert to lowercase", "value_two", result);
    }

    public void testConvertToStringWithoutPrefix() {
        // Test enum without matching prefix - should just convert to lowercase
        String result = ProtobufEnumUtils.convertToString(TestEnum.SIMPLE_VALUE);
        assertEquals("Should convert to lowercase when no prefix matches", "simple_value", result);
    }

    public void testCamelCaseToSnakeCase() {
        // This tests the private method indirectly through the public method
        // Test with enum that has CamelCase class name
        enum MultiWordEnum {
            MULTI_WORD_ENUM_TEST_VALUE
        }

        String result = ProtobufEnumUtils.convertToString(MultiWordEnum.MULTI_WORD_ENUM_TEST_VALUE);
        assertEquals("Should handle CamelCase to snake_case conversion", "test_value", result);
    }
}

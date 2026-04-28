/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.common;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class FieldValueProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithInteger() {
        Integer intValue = 42;
        FieldValue fieldValue = FieldValueProtoUtils.toProto(intValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have general number", fieldValue.hasGeneralNumber());
        assertTrue(fieldValue.getGeneralNumber().hasInt32Value());
        assertEquals("Integer value should match", 42, fieldValue.getGeneralNumber().getInt32Value());
    }

    public void testToProtoWithLong() {
        Long longValue = 9223372036854775807L; // Max long value
        FieldValue fieldValue = FieldValueProtoUtils.toProto(longValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have general number", fieldValue.hasGeneralNumber());
        assertTrue(fieldValue.getGeneralNumber().hasInt64Value());
        assertEquals("Long value should match", 9223372036854775807L, fieldValue.getGeneralNumber().getInt64Value());
    }

    public void testToProtoWithDouble() {
        Double doubleValue = 3.14159;
        FieldValue fieldValue = FieldValueProtoUtils.toProto(doubleValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have general number", fieldValue.hasGeneralNumber());
        assertTrue(fieldValue.getGeneralNumber().hasDoubleValue());
        assertEquals("Double value should match", 3.14159, fieldValue.getGeneralNumber().getDoubleValue(), 0.001);
    }

    public void testToProtoWithFloat() {
        Float floatValue = 2.71828f;
        FieldValue fieldValue = FieldValueProtoUtils.toProto(floatValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have general number", fieldValue.hasGeneralNumber());
        assertTrue(fieldValue.getGeneralNumber().hasFloatValue());
        assertEquals("Float value should match", 2.71828f, fieldValue.getGeneralNumber().getFloatValue(), 0.0f);
    }

    public void testToProtoWithString() {
        String stringValue = "test string";
        FieldValue fieldValue = FieldValueProtoUtils.toProto(stringValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have string value", fieldValue.hasString());
        assertEquals("String value should match", "test string", fieldValue.getString());
    }

    public void testToProtoWithBoolean() {
        // Test with true
        Boolean trueValue = true;
        FieldValue trueFieldValue = FieldValueProtoUtils.toProto(trueValue);

        assertNotNull("FieldValue should not be null", trueFieldValue);
        assertTrue("FieldValue should have bool value", trueFieldValue.hasBool());
        assertTrue("Bool value should be true", trueFieldValue.getBool());

        // Test with false
        Boolean falseValue = false;
        FieldValue falseFieldValue = FieldValueProtoUtils.toProto(falseValue);

        assertNotNull("FieldValue should not be null", falseFieldValue);
        assertTrue("FieldValue should have bool value", falseFieldValue.hasBool());
        assertFalse("Bool value should be false", falseFieldValue.getBool());
    }

    public void testToProtoWithEnum() {
        // Use a test enum
        TestEnum enumValue = TestEnum.TEST_VALUE;
        FieldValue fieldValue = FieldValueProtoUtils.toProto(enumValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have string value", fieldValue.hasString());
        assertEquals("String value should match enum toString", "TEST_VALUE", fieldValue.getString());
    }

    public void testToProtoWithMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("string", "value");
        map.put("integer", 42);
        map.put("boolean", true);

        // Maps are not supported in FieldValue protobuf, should throw exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> FieldValueProtoUtils.toProto(map));

        assertTrue(
            "Exception message should mention cannot convert map",
            exception.getMessage().contains("Cannot convert") && exception.getMessage().contains("to FieldValue")
        );
    }

    // TODO: ObjectMap functionality removed in protobufs 0.8.0 - FieldValue now only supports basic types
    // This test needs to be rewritten for the simplified FieldValue structure
    /*
    public void testToProtoWithNestedMap() {
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("nested_string", "nested value");
        nestedMap.put("nested_integer", 99);

        Map<String, Object> outerMap = new HashMap<>();
        outerMap.put("outer_string", "outer value");
        outerMap.put("nested_map", nestedMap);

        FieldValue fieldValue = FieldValueProtoUtils.toProto(outerMap);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have object map", fieldValue.hasObjectMap());

        org.opensearch.protobufs.ObjectMap outerObjectMap = fieldValue.getObjectMap();
        assertEquals("Outer object map should have 2 fields", 2, outerObjectMap.getFieldsCount());

        // Check outer string field
        assertTrue("Outer string field should exist", outerObjectMap.containsFields("outer_string"));
        assertTrue("Outer string field should have string value", outerObjectMap.getFieldsOrThrow("outer_string").hasStringValue());
        assertEquals("Outer string field should match", "outer value", outerObjectMap.getFieldsOrThrow("outer_string").getStringValue());

        // Check nested map field
        assertTrue("Nested map field should exist", outerObjectMap.containsFields("nested_map"));
        assertTrue("Nested map field should have object map", outerObjectMap.getFieldsOrThrow("nested_map").hasObjectMap());

        org.opensearch.protobufs.ObjectMap nestedObjectMap = outerObjectMap.getFieldsOrThrow("nested_map").getObjectMap();
        assertEquals("Nested object map should have 2 fields", 2, nestedObjectMap.getFieldsCount());

        // Check nested string field
        assertTrue("Nested string field should exist", nestedObjectMap.containsFields("nested_string"));
        assertTrue("Nested string field should have string value", nestedObjectMap.getFieldsOrThrow("nested_string").hasStringValue());
        assertEquals("Nested string field should match", "nested value", nestedObjectMap.getFieldsOrThrow("nested_string").getStringValue());

        // Check nested integer field
        assertTrue("Nested integer field should exist", nestedObjectMap.containsFields("nested_integer"));
        assertTrue("Nested integer field should have int32 value", nestedObjectMap.getFieldsOrThrow("nested_integer").hasInt32());
        assertEquals("Nested integer field should match", 99, nestedObjectMap.getFieldsOrThrow("nested_integer").getInt32());
    }
    */

    public void testToProtoWithUnsupportedType() {
        // Create an object of an unsupported type
        Object unsupportedObject = new StringBuilder("unsupported");

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> FieldValueProtoUtils.toProto(unsupportedObject)
        );

        assertTrue("Exception message should mention cannot convert", exception.getMessage().contains("Cannot convert"));
    }

    public void testFromProtoWithUint64Value() {
        // Create a FieldValue with UINT64_VALUE
        // Using a value that fits in Java long (max long is 9223372036854775807L)
        long uint64Value = 9223372036854775807L;
        org.opensearch.protobufs.GeneralNumber generalNumber = org.opensearch.protobufs.GeneralNumber.newBuilder()
            .setUint64Value(uint64Value)
            .build();
        FieldValue fieldValue = FieldValue.newBuilder().setGeneralNumber(generalNumber).build();

        // Convert from Protocol Buffer
        Object result = FieldValueProtoUtils.fromProto(fieldValue);

        // Verify the conversion
        assertNotNull("Result should not be null", result);
        assertTrue("Result should be Long", result instanceof Long);
        assertEquals("Uint64 value should match", uint64Value, result);
    }

    public void testFromProtoWithUint64ValueExceedingLongMax() {
        // Create a FieldValue with UINT64_VALUE that exceeds Long.MAX_VALUE
        // When a uint64 value > Long.MAX_VALUE is stored, it appears as a negative long
        // (e.g., max uint64 = 2^64-1 appears as -1L in two's complement)
        // The expected behavior is to return BigInteger for values that don't fit in signed long
        long uint64ValueExceedingLongMax = -1L; // This represents 2^64-1 (max unsigned long)
        org.opensearch.protobufs.GeneralNumber generalNumber = org.opensearch.protobufs.GeneralNumber.newBuilder()
            .setUint64Value(uint64ValueExceedingLongMax)
            .build();
        FieldValue fieldValue = FieldValue.newBuilder().setGeneralNumber(generalNumber).build();

        // Convert from Protocol Buffer
        Object result = FieldValueProtoUtils.fromProto(fieldValue);

        // Verify the conversion - should return BigInteger for values exceeding Long.MAX_VALUE
        assertNotNull("Result should not be null", result);
        assertTrue("Result should be BigInteger when value exceeds Long.MAX_VALUE", result instanceof BigInteger);
        BigInteger expectedValue = new BigInteger("18446744073709551615"); // 2^64 - 1
        assertEquals("Uint64 value should match max unsigned long", expectedValue, result);
    }

    public void testToProtoWithBigInteger() {
        // Test with a BigInteger that represents an unsigned_long value
        BigInteger bigIntValue = new BigInteger("18446744073709551615"); // Max unsigned long
        FieldValue fieldValue = FieldValueProtoUtils.toProto(bigIntValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have general number", fieldValue.hasGeneralNumber());
        assertTrue("GeneralNumber should have uint64 value", fieldValue.getGeneralNumber().hasUint64Value());
        assertEquals("Uint64 value should match", -1L, fieldValue.getGeneralNumber().getUint64Value()); // -1L is max unsigned
    }

    public void testToProtoWithBigIntegerSmallValue() {
        // Test with a BigInteger that has a value within signed long range
        BigInteger bigIntValue = new BigInteger("2147395412");
        FieldValue fieldValue = FieldValueProtoUtils.toProto(bigIntValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have general number", fieldValue.hasGeneralNumber());
        assertTrue("GeneralNumber should have uint64 value", fieldValue.getGeneralNumber().hasUint64Value());
        assertEquals("Uint64 value should match", 2147395412L, fieldValue.getGeneralNumber().getUint64Value());
    }

    public void testRoundTripBigInteger() {
        // Test round-trip conversion for BigInteger values
        BigInteger[] testValues = {
            new BigInteger("0"),
            new BigInteger("2147395412"),
            new BigInteger("9223372036854775807"), // Max signed long
            BigInteger.valueOf(Long.MAX_VALUE) };

        for (BigInteger original : testValues) {
            FieldValue fieldValue = FieldValueProtoUtils.toProto(original);
            Object result = FieldValueProtoUtils.fromProto(fieldValue);

            assertNotNull("Result should not be null for " + original, result);
            assertTrue("Result should be Long for " + original, result instanceof Long);
            assertEquals("Value should match for " + original, original.longValue(), ((Long) result).longValue());
        }
    }

    public void testToProtoWithBigIntegerOutOfRange() {
        // Test that BigInteger values outside unsigned long range throw exception
        BigInteger tooLarge = new BigInteger("18446744073709551616"); // 2^64 (exceeds max unsigned long)
        BigInteger negative = new BigInteger("-1");

        // Should throw IllegalArgumentException for values out of range
        IllegalArgumentException exception1 = expectThrows(IllegalArgumentException.class, () -> FieldValueProtoUtils.toProto(tooLarge));
        assertTrue(
            "Exception should mention out of range",
            exception1.getMessage().contains("out of range") || exception1.getMessage().contains("unsigned long")
        );

        IllegalArgumentException exception2 = expectThrows(IllegalArgumentException.class, () -> FieldValueProtoUtils.toProto(negative));
        assertTrue(
            "Exception should mention out of range",
            exception2.getMessage().contains("out of range") || exception2.getMessage().contains("unsigned long")
        );
    }

    // Test enum for testing enum conversion
    private enum TestEnum {
        TEST_VALUE
    }
}

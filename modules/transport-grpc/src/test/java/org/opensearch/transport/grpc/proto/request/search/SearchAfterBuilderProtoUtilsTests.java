/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.GeneralNumber;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SearchAfterBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithEmptyList() throws IOException {
        // Call the method under test with an empty list
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(Collections.emptyList());

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should be empty", 0, values.length);
    }

    public void testFromProtoWithStringValue() throws IOException {
        // Create a list with a string value
        List<FieldValue> fieldValues = Collections.singletonList(FieldValue.newBuilder().setStringValue("test_string").build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be a string", "test_string", values[0]);
    }

    public void testFromProtoWithBooleanValue() throws IOException {
        // Create a list with a boolean value
        List<FieldValue> fieldValues = Collections.singletonList(FieldValue.newBuilder().setBoolValue(true).build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be a boolean", true, values[0]);
    }

    public void testFromProtoWithInt32Value() throws IOException {
        // Create a list with an int32 value
        List<FieldValue> fieldValues = Collections.singletonList(
            FieldValue.newBuilder().setGeneralNumber(GeneralNumber.newBuilder().setInt32Value(42).build()).build()
        );

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be an integer", 42, values[0]);
    }

    public void testFromProtoWithInt64Value() throws IOException {
        // Create a list with an int64 value
        List<FieldValue> fieldValues = Collections.singletonList(
            FieldValue.newBuilder().setGeneralNumber(GeneralNumber.newBuilder().setInt64Value(9223372036854775807L).build()).build()
        );

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be a long", 9223372036854775807L, values[0]);
    }

    public void testFromProtoWithDoubleValue() throws IOException {
        // Create a list with a double value
        List<FieldValue> fieldValues = Collections.singletonList(
            FieldValue.newBuilder().setGeneralNumber(GeneralNumber.newBuilder().setDoubleValue(3.14159).build()).build()
        );

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be a double", 3.14159, values[0]);
    }

    public void testFromProtoWithFloatValue() throws IOException {
        // Create a list with a float value
        List<FieldValue> fieldValues = Collections.singletonList(
            FieldValue.newBuilder().setGeneralNumber(GeneralNumber.newBuilder().setFloatValue(2.71828f).build()).build()
        );

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be a float", 2.71828f, values[0]);
    }

    public void testFromProtoWithMultipleValues() throws IOException {
        // Create a list with multiple values of different types
        List<FieldValue> fieldValues = new ArrayList<>();
        fieldValues.add(FieldValue.newBuilder().setStringValue("test_string").build());
        fieldValues.add(FieldValue.newBuilder().setBoolValue(true).build());
        fieldValues.add(FieldValue.newBuilder().setGeneralNumber(GeneralNumber.newBuilder().setInt32Value(42).build()).build());
        fieldValues.add(FieldValue.newBuilder().setGeneralNumber(GeneralNumber.newBuilder().setDoubleValue(3.14159).build()).build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 4 elements", 4, values.length);
        assertEquals("First value should be a string", "test_string", values[0]);
        assertEquals("Second value should be a boolean", true, values[1]);
        assertEquals("Third value should be an integer", 42, values[2]);
        assertEquals("Fourth value should be a double", 3.14159, values[3]);
    }

    public void testFromProtoWithEmptyFieldValue() throws IOException {
        // Create a list with an empty field value (no value set)
        List<FieldValue> fieldValues = Collections.singletonList(FieldValue.newBuilder().build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should be empty", 0, values.length);
    }

    public void testFromProtoWithEmptyGeneralNumber() throws IOException {
        // Create a list with a field value containing an empty general number (no value set)
        List<FieldValue> fieldValues = Collections.singletonList(
            FieldValue.newBuilder().setGeneralNumber(GeneralNumber.newBuilder().build()).build()
        );

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should be empty", 0, values.length);
    }
}

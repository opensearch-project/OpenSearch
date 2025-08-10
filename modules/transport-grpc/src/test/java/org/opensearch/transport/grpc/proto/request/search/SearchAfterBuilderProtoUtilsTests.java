/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.protobufs.FieldValue;
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
        List<FieldValue> fieldValues = Collections.singletonList(FieldValue.newBuilder().setString("test_string").build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be a string", "test_string", values[0]);
    }

    public void testFromProtoWithBooleanValue() throws IOException {
        // Create a list with a boolean value
        List<FieldValue> fieldValues = Collections.singletonList(FieldValue.newBuilder().setBool(true).build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be a boolean", true, values[0]);
    }

    public void testFromProtoWithInt32Value() throws IOException {
        // Create a list with an int32 value
        List<FieldValue> fieldValues = Collections.singletonList(FieldValue.newBuilder().setFloat(42.0f).build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be a float", 42.0f, values[0]);
    }

    public void testFromProtoWithInt64Value() throws IOException {
        // Create a list with an int64 value
        List<FieldValue> fieldValues = Collections.singletonList(FieldValue.newBuilder().setFloat(9223372036854775807.0f).build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be a float", 9223372036854775807.0f, values[0]);
    }

    public void testFromProtoWithDoubleValue() throws IOException {
        // Create a list with a double value
        List<FieldValue> fieldValues = Collections.singletonList(FieldValue.newBuilder().setFloat((float) 3.14159).build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be a float", (float) 3.14159, values[0]);
    }

    public void testFromProtoWithFloatValue() throws IOException {
        // Create a list with a float value
        List<FieldValue> fieldValues = Collections.singletonList(FieldValue.newBuilder().setFloat(2.71828f).build());

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
        fieldValues.add(FieldValue.newBuilder().setString("test_string").build());
        fieldValues.add(FieldValue.newBuilder().setBool(true).build());
        fieldValues.add(FieldValue.newBuilder().setFloat(42.0f).build());
        fieldValues.add(FieldValue.newBuilder().setFloat((float) 3.14159).build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 4 elements", 4, values.length);
        assertEquals("First value should be a string", "test_string", values[0]);
        assertEquals("Second value should be a boolean", true, values[1]);
        assertEquals("Third value should be a float", 42.0f, values[2]);
        assertEquals("Fourth value should be a float", (float) 3.14159, values[3]);
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

    public void testFromProtoWithZeroFloatValue() throws IOException {
        // Create a list with a field value containing zero float
        List<FieldValue> fieldValues = Collections.singletonList(FieldValue.newBuilder().setFloat(0.0f).build());

        // Call the method under test
        Object[] values = SearchAfterBuilderProtoUtils.fromProto(fieldValues);

        // Verify the result
        assertNotNull("Values array should not be null", values);
        assertEquals("Values array should have 1 element", 1, values.length);
        assertEquals("Value should be 0.0f", 0.0f, values[0]);
    }
}

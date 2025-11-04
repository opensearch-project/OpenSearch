/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class StoredFieldsContextProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithValidFieldList() throws IOException {
        // Create a list of field names
        List<String> fieldNames = Arrays.asList("field1", "field2", "field3");

        // Call the method under test
        StoredFieldsContext storedFieldsContext = StoredFieldsContextProtoUtils.fromProto(fieldNames);

        // Verify the result
        assertNotNull("StoredFieldsContext should not be null", storedFieldsContext);
        assertEquals("Field names should match", fieldNames, storedFieldsContext.fieldNames());
        assertTrue("FetchFields should be true", storedFieldsContext.fetchFields());
    }

    public void testFromProtoWithEmptyFieldList() throws IOException {
        // Call the method under test with an empty list
        StoredFieldsContext storedFieldsContext = StoredFieldsContextProtoUtils.fromProto(Collections.emptyList());

        // Verify the result
        assertNull("StoredFieldsContext should be null for empty list", storedFieldsContext);
    }

    public void testFromProtoWithNullFieldList() throws IOException {
        // Call the method under test with null
        StoredFieldsContext storedFieldsContext = StoredFieldsContextProtoUtils.fromProto(null);

        // Verify the result
        assertNull("StoredFieldsContext should be null for null list", storedFieldsContext);
    }

    public void testFromProtoWithSingleField() throws IOException {
        // Create a list with a single field name
        List<String> fieldNames = Collections.singletonList("single_field");

        // Call the method under test
        StoredFieldsContext storedFieldsContext = StoredFieldsContextProtoUtils.fromProto(fieldNames);

        // Verify the result
        assertNotNull("StoredFieldsContext should not be null", storedFieldsContext);
        assertEquals("Field names should match", fieldNames, storedFieldsContext.fieldNames());
        assertEquals("Should have 1 field", 1, storedFieldsContext.fieldNames().size());
    }

    public void testFromProtoRequestWithStoredFields() {
        // Create a SearchRequestBody with stored fields
        List<String> fieldNames = Arrays.asList("field1", "field2", "field3");
        org.opensearch.protobufs.SearchRequestBody searchRequestBody = org.opensearch.protobufs.SearchRequestBody.newBuilder()
            .addAllStoredFields(fieldNames)
            .build();

        // Call the method under test
        StoredFieldsContext storedFieldsContext = StoredFieldsContextProtoUtils.fromProtoRequest(searchRequestBody);

        // Verify the result
        assertNotNull("StoredFieldsContext should not be null", storedFieldsContext);
        assertEquals("Field names should match", fieldNames, storedFieldsContext.fieldNames());
        assertTrue("FetchFields should be true", storedFieldsContext.fetchFields());
    }

    public void testFromProtoRequestWithNoStoredFields() {
        // Create a SearchRequestBody with no stored fields
        org.opensearch.protobufs.SearchRequestBody searchRequestBody = org.opensearch.protobufs.SearchRequestBody.newBuilder().build();

        // Call the method under test
        StoredFieldsContext storedFieldsContext = StoredFieldsContextProtoUtils.fromProtoRequest(searchRequestBody);

        // Verify the result
        assertNull("StoredFieldsContext should be null for request with no stored fields", storedFieldsContext);
    }

    public void testFromProtoRequestWithEmptyStoredFields() {
        // Create a SearchRequestBody with empty stored fields list
        org.opensearch.protobufs.SearchRequestBody searchRequestBody = org.opensearch.protobufs.SearchRequestBody.newBuilder()
            .addAllStoredFields(Collections.emptyList())
            .build();

        // Call the method under test
        StoredFieldsContext storedFieldsContext = StoredFieldsContextProtoUtils.fromProtoRequest(searchRequestBody);

        // Verify the result
        assertNull("StoredFieldsContext should be null for request with empty stored fields", storedFieldsContext);
    }

    public void testFromProtoWithSpecialFields() throws IOException {
        // Create a list with special field names like _source, _id, etc.
        List<String> fieldNames = Arrays.asList("_id", "_source", "_routing", "_field_names");

        // Call the method under test
        StoredFieldsContext storedFieldsContext = StoredFieldsContextProtoUtils.fromProto(fieldNames);

        // Verify the result
        assertNotNull("StoredFieldsContext should not be null", storedFieldsContext);
        assertEquals("Field names should match", fieldNames, storedFieldsContext.fieldNames());
        assertEquals("Should have 4 fields", 4, storedFieldsContext.fieldNames().size());
    }

    public void testFromProtoWithLargeNumberOfFields() throws IOException {
        // Create a list with a large number of field names
        List<String> fieldNames = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            fieldNames.add("field" + i);
        }

        // Call the method under test
        StoredFieldsContext storedFieldsContext = StoredFieldsContextProtoUtils.fromProto(fieldNames);

        // Verify the result
        assertNotNull("StoredFieldsContext should not be null", storedFieldsContext);
        assertEquals("Field names should match", fieldNames, storedFieldsContext.fieldNames());
        assertEquals("Should have 100 fields", 100, storedFieldsContext.fieldNames().size());
    }
}

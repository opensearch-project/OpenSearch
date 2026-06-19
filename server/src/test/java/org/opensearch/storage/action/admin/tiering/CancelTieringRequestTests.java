/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.storage.action.tiering.CancelTieringRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for CancelTieringRequest.
 * Tests serialization, validation, equality, and other model behaviors.
 */
public class CancelTieringRequestTests extends OpenSearchTestCase {

    public void testConstructorWithIndexName() {
        String indexName = "test-index";
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex(indexName);

        assertEquals("Index name should be set correctly", indexName, request.getIndex());
    }

    public void testDefaultConstructor() {
        CancelTieringRequest request = new CancelTieringRequest();

        assertNull("Index should be null by default", request.getIndex());
    }

    public void testSetAndGetIndex() {
        CancelTieringRequest request = new CancelTieringRequest();
        String indexName = "my-test-index";

        request.setIndex(indexName);
        assertEquals("Index should be set correctly", indexName, request.getIndex());
    }

    public void testValidationWithValidIndex() {
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex("valid-index");

        ActionRequestValidationException validation = request.validate();
        assertNull("Valid index should pass validation", validation);
    }

    public void testValidationWithNullIndex() {
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex(null);

        ActionRequestValidationException validation = request.validate();
        assertNotNull("Null index should fail validation", validation);
        assertThat(
            "Validation message should be appropriate",
            validation.getMessage(),
            containsString("Index name cannot be null or empty")
        );
    }

    public void testValidationWithEmptyIndex() {
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex("");

        ActionRequestValidationException validation = request.validate();
        assertNotNull("Empty index should fail validation", validation);
        assertThat(
            "Validation message should be appropriate",
            validation.getMessage(),
            containsString("Index name cannot be null or empty")
        );
    }

    public void testValidationWithBlankIndex() {
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex("   ");

        ActionRequestValidationException validation = request.validate();
        assertNotNull("Blank index should fail validation", validation);
        assertThat(
            "Validation message should be appropriate",
            validation.getMessage(),
            containsString("Index name cannot be null or empty")
        );
    }

    public void testSerializationRoundTrip() throws IOException {
        // Create original request
        String originalIndex = "test-serialization-index";
        CancelTieringRequest original = new CancelTieringRequest();
        original.setIndex(originalIndex);
        original.timeout("30s");
        original.clusterManagerNodeTimeout("60s");

        // Serialize to bytes
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        // Deserialize from bytes
        StreamInput input = output.bytes().streamInput();
        CancelTieringRequest deserialized = new CancelTieringRequest(input);

        // Verify deserialized request matches original
        assertEquals("Index should be preserved after serialization", original.getIndex(), deserialized.getIndex());
        assertEquals("Timeout should be preserved after serialization", original.timeout(), deserialized.timeout());
        assertEquals(
            "Cluster manager timeout should be preserved after serialization",
            original.clusterManagerNodeTimeout(),
            deserialized.clusterManagerNodeTimeout()
        );
    }

    public void testEqualityWithSameIndex() {
        String indexName = "test-index";
        CancelTieringRequest request1 = new CancelTieringRequest();
        request1.setIndex(indexName);
        CancelTieringRequest request2 = new CancelTieringRequest();
        request2.setIndex(indexName);

        assertTrue("Requests with same index should be equal", request1.equals(request2));
        assertEquals("Hash codes should be equal for equal objects", request1.hashCode(), request2.hashCode());
    }

    public void testEqualityWithDifferentIndex() {
        CancelTieringRequest request1 = new CancelTieringRequest();
        request1.setIndex("index1");
        CancelTieringRequest request2 = new CancelTieringRequest();
        request2.setIndex("index2");

        assertFalse("Requests with different indices should not be equal", request1.equals(request2));
        assertNotEquals("Hash codes should be different for different objects", request1.hashCode(), request2.hashCode());
    }

    public void testEqualityWithNullIndex() {
        CancelTieringRequest request1 = new CancelTieringRequest();
        CancelTieringRequest request2 = new CancelTieringRequest();

        assertTrue("Requests with both null indices should be equal", request1.equals(request2));
        assertEquals("Hash codes should be equal for equal objects", request1.hashCode(), request2.hashCode());
    }

    public void testEqualityWithOneNullIndex() {
        CancelTieringRequest request1 = new CancelTieringRequest();
        request1.setIndex("test-index");
        CancelTieringRequest request2 = new CancelTieringRequest();

        assertFalse("Request with null index should not equal request with non-null index", request1.equals(request2));
        assertFalse("Request with non-null index should not equal request with null index", request2.equals(request1));
    }

    public void testEqualityWithSelf() {
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex("test-index");

        assertTrue("Request should be equal to itself", request.equals(request));
        assertEquals("Hash code should be consistent", request.hashCode(), request.hashCode());
    }

    public void testEqualityWithNull() {
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex("test-index");

        assertFalse("Request should not be equal to null", request.equals(null));
    }

    public void testEqualityWithDifferentClass() {
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex("test-index");
        String otherObject = "test-index";

        assertFalse("Request should not be equal to object of different class", request.equals(otherObject));
    }

    public void testToString() {
        String indexName = "test-index-for-string";
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex(indexName);

        String toString = request.toString();
        assertNotNull("toString should not be null", toString);
        assertTrue("toString should contain class name", toString.contains("CancelTieringRequest"));
        assertTrue("toString should contain index name", toString.contains(indexName));
    }

    public void testToStringWithNullIndex() {
        CancelTieringRequest request = new CancelTieringRequest();

        String toString = request.toString();
        assertNotNull("toString should not be null even with null index", toString);
        assertTrue("toString should contain class name", toString.contains("CancelTieringRequest"));
    }

    public void testTimeoutFunctionality() {
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex("test-index");

        // Test default timeout
        assertNotNull("Default timeout should not be null", request.timeout());

        // Test setting timeout
        request.timeout("45s");
        assertNotNull("Timeout should be set", request.timeout());
        assertEquals("Timeout value should match", "45s", request.timeout().toString());
    }

    public void testClusterManagerNodeTimeoutFunctionality() {
        CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex("test-index");

        // Test default cluster manager timeout
        assertNotNull("Default cluster manager timeout should not be null", request.clusterManagerNodeTimeout());

        // Test setting cluster manager timeout
        request.clusterManagerNodeTimeout("90s");
        assertNotNull("Cluster manager timeout should be set", request.clusterManagerNodeTimeout());
        assertEquals("Cluster manager timeout value should match", "1.5m", request.clusterManagerNodeTimeout().toString());
    }

    public void testMultipleValidIndexNames() {
        String[] validIndices = {
            "simple-index",
            "index_with_underscores",
            "index123",
            "a",  // single character
            ".ds-logs-2023.01.01-000001", // data stream backing index
            "very-long-index-name-with-many-parts-and-numbers-123456789" };

        for (String indexName : validIndices) {
            CancelTieringRequest request = new CancelTieringRequest();
            request.setIndex(indexName);
            assertNull("Index '" + indexName + "' should pass validation", request.validate());
        }
    }
}

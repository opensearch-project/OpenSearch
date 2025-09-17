/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.cache;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Tests for {@link PruneCacheRequest} and {@link PruneCacheResponse} serialization.
 */
public class PruneCacheRequestResponseTests extends OpenSearchTestCase {

    /**
     * Tests PruneCacheRequest serialization and deserialization.
     */
    public void testPruneCacheRequestSerialization() throws IOException {
        PruneCacheRequest originalRequest = new PruneCacheRequest();
        originalRequest.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(30));

        // Serialize the request
        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        // Deserialize the request
        StreamInput in = out.bytes().streamInput();
        PruneCacheRequest deserializedRequest = new PruneCacheRequest(in);

        // Assert that timeout is properly serialized/deserialized
        assertEquals(originalRequest.clusterManagerNodeTimeout(), deserializedRequest.clusterManagerNodeTimeout());
    }

    /**
     * Tests PruneCacheRequest with default timeout.
     */
    public void testPruneCacheRequestDefaultTimeout() throws IOException {
        PruneCacheRequest originalRequest = new PruneCacheRequest();
        // Don't set timeout explicitly, should use default

        // Serialize the request
        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        // Deserialize the request
        StreamInput in = out.bytes().streamInput();
        PruneCacheRequest deserializedRequest = new PruneCacheRequest(in);

        // Assert that default timeout is maintained
        assertEquals(originalRequest.clusterManagerNodeTimeout(), deserializedRequest.clusterManagerNodeTimeout());
    }

    /**
     * Tests PruneCacheResponse serialization and deserialization.
     */
    public void testPruneCacheResponseSerialization() throws IOException {
        boolean acknowledged = true;
        long prunedBytes = 1048576L;

        PruneCacheResponse originalResponse = new PruneCacheResponse(acknowledged, prunedBytes);

        // Serialize the response
        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);

        // Deserialize the response
        StreamInput in = out.bytes().streamInput();
        PruneCacheResponse deserializedResponse = new PruneCacheResponse(in);

        // Assert that all fields are properly serialized/deserialized
        assertEquals(originalResponse.isAcknowledged(), deserializedResponse.isAcknowledged());
        assertEquals(originalResponse.getPrunedBytes(), deserializedResponse.getPrunedBytes());
        assertEquals(originalResponse, deserializedResponse);
        assertEquals(originalResponse.hashCode(), deserializedResponse.hashCode());
        assertEquals(originalResponse.toString(), deserializedResponse.toString());
    }

    /**
     * Tests PruneCacheResponse with different values.
     */
    public void testPruneCacheResponseVariousValues() throws IOException {
        // Test with zero bytes pruned
        testResponseSerialization(true, 0L);
        testResponseSerialization(false, 0L);

        // Test with various byte amounts
        testResponseSerialization(true, 1L);
        testResponseSerialization(true, 1024L);
        testResponseSerialization(true, 1048576L);
        testResponseSerialization(true, Long.MAX_VALUE);
    }

    private void testResponseSerialization(boolean acknowledged, long prunedBytes) throws IOException {
        PruneCacheResponse originalResponse = new PruneCacheResponse(acknowledged, prunedBytes);

        // Serialize the response
        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);

        // Deserialize the response
        StreamInput in = out.bytes().streamInput();
        PruneCacheResponse deserializedResponse = new PruneCacheResponse(in);

        // Assert equality
        assertEquals(originalResponse.isAcknowledged(), deserializedResponse.isAcknowledged());
        assertEquals(originalResponse.getPrunedBytes(), deserializedResponse.getPrunedBytes());
        assertEquals(originalResponse, deserializedResponse);
    }

    /**
     * Tests PruneCacheResponse XContent (JSON) serialization.
     */
    public void testPruneCacheResponseXContentSerialization() throws IOException {
        PruneCacheResponse response = new PruneCacheResponse(true, 2048576L);

        // Generate XContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = builder.toString();

        // Verify JSON structure
        assertTrue("JSON should contain acknowledged field", jsonString.contains("\"acknowledged\":true"));
        assertTrue("JSON should contain pruned_bytes field", jsonString.contains("\"pruned_bytes\":2048576"));

        // Verify proper JSON format
        assertTrue("JSON should start with {", jsonString.startsWith("{"));
        assertTrue("JSON should end with }", jsonString.endsWith("}"));
    }

    /**
     * Tests PruneCacheResponse XContent with different values.
     */
    public void testPruneCacheResponseXContentVariousValues() throws IOException {
        // Test acknowledged=false, zero bytes
        testXContentGeneration(false, 0L, "\"acknowledged\":false", "\"pruned_bytes\":0");

        // Test acknowledged=true, large number
        testXContentGeneration(true, 999999999L, "\"acknowledged\":true", "\"pruned_bytes\":999999999");

        // Test with max long value
        testXContentGeneration(true, Long.MAX_VALUE, "\"acknowledged\":true", "\"pruned_bytes\":" + Long.MAX_VALUE);
    }

    private void testXContentGeneration(boolean acknowledged, long prunedBytes, String expectedAck, String expectedBytes)
        throws IOException {
        PruneCacheResponse response = new PruneCacheResponse(acknowledged, prunedBytes);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = builder.toString();

        assertTrue("JSON should contain expected acknowledged value", jsonString.contains(expectedAck));
        assertTrue("JSON should contain expected pruned_bytes value", jsonString.contains(expectedBytes));
    }

    /**
     * Tests equals and hashCode contract for PruneCacheResponse.
     */
    public void testPruneCacheResponseEqualsAndHashCode() {
        PruneCacheResponse response1 = new PruneCacheResponse(true, 1024L);
        PruneCacheResponse response2 = new PruneCacheResponse(true, 1024L);
        PruneCacheResponse response3 = new PruneCacheResponse(false, 1024L);
        PruneCacheResponse response4 = new PruneCacheResponse(true, 2048L);

        // Test reflexivity
        assertEquals(response1, response1);
        assertEquals(response1.hashCode(), response1.hashCode());

        // Test symmetry
        assertEquals(response1, response2);
        assertEquals(response2, response1);
        assertEquals(response1.hashCode(), response2.hashCode());

        // Test inequality
        assertNotEquals(response1, response3);
        assertNotEquals(response1, response4);
        assertNotEquals(response3, response4);

        // Test null handling
        assertNotEquals(response1, null);

        // Test different class
        assertNotEquals(response1, "not a response");
    }

    /**
     * Tests toString method for PruneCacheResponse.
     */
    public void testPruneCacheResponseToString() {
        PruneCacheResponse response = new PruneCacheResponse(true, 12345L);
        String toString = response.toString();

        assertTrue("toString should contain class name", toString.contains("PruneCacheResponse"));
        assertTrue("toString should contain acknowledged value", toString.contains("acknowledged=true"));
        assertTrue("toString should contain prunedBytes value", toString.contains("prunedBytes=12345"));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document;

import org.opensearch.action.get.GetResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.get.GetResult;
import org.opensearch.protobufs.GetDocumentResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Comprehensive unit tests for GetDocumentResponseProtoUtils.
 */
public class GetDocumentResponseProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoDocumentFound() {
        // Create a GetResult for a found document
        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("title", "Test Document");
        sourceMap.put("content", "This is test content");

        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            1L, // seqNo
            2L, // primaryTerm
            3L, // version
            true, // exists
            new BytesArray("{\"title\":\"Test Document\",\"content\":\"This is test content\"}"),
            Collections.emptyMap(), // fields
            Collections.emptyMap()  // metaFields
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(getResponse);

        assertEquals("test-index", protoResponse.getGetDocumentResponseBody().getXIndex());
        assertEquals("test-id", protoResponse.getGetDocumentResponseBody().getXId());
        assertEquals("1", protoResponse.getGetDocumentResponseBody().getXSeqNo());
        assertEquals("2", protoResponse.getGetDocumentResponseBody().getXPrimaryTerm());
        assertEquals("3", protoResponse.getGetDocumentResponseBody().getXVersion());
        assertTrue(protoResponse.getGetDocumentResponseBody().getFound());
        assertFalse(protoResponse.getGetDocumentResponseBody().getXSource().isEmpty());
    }

    public void testToProtoDocumentNotFound() {
        // Create a GetResult for a document that doesn't exist
        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            -2L, // UNASSIGNED_SEQ_NO
            0L,  // UNASSIGNED_PRIMARY_TERM
            -1L, // NOT_FOUND version
            false, // exists
            null, // source
            Collections.emptyMap(), // fields
            Collections.emptyMap()  // metaFields
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(getResponse);

        assertEquals("test-index", protoResponse.getGetDocumentResponseBody().getXIndex());
        assertEquals("test-id", protoResponse.getGetDocumentResponseBody().getXId());
        assertEquals("-2", protoResponse.getGetDocumentResponseBody().getXSeqNo());
        assertEquals("0", protoResponse.getGetDocumentResponseBody().getXPrimaryTerm());
        assertEquals("-1", protoResponse.getGetDocumentResponseBody().getXVersion());
        assertFalse(protoResponse.getGetDocumentResponseBody().getFound());
        assertTrue(protoResponse.getGetDocumentResponseBody().getXSource().isEmpty());
    }

    public void testToProtoWithComplexSource() {
        // Create a complex JSON document
        String complexJson =
            "{\"user\":{\"name\":\"John Doe\",\"age\":30,\"preferences\":{\"theme\":\"dark\",\"language\":\"en\"}},\"tags\":[\"important\",\"work\"],\"metadata\":{\"created\":\"2025-01-01\",\"modified\":\"2025-01-02\"}}";

        GetResult getResult = new GetResult(
            "complex-index",
            "complex-id",
            5L, // seqNo
            3L, // primaryTerm
            10L, // version
            true, // exists
            new BytesArray(complexJson),
            Collections.emptyMap(), // fields
            Collections.emptyMap()  // metaFields
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(getResponse);

        assertEquals("complex-index", protoResponse.getGetDocumentResponseBody().getXIndex());
        assertEquals("complex-id", protoResponse.getGetDocumentResponseBody().getXId());
        assertTrue(protoResponse.getGetDocumentResponseBody().getFound());
        assertEquals(complexJson, protoResponse.getGetDocumentResponseBody().getXSource().toStringUtf8());
    }

    public void testToProtoVersionBoundaries() {
        // Test minimum values
        GetResult getResult1 = new GetResult(
            "test-index",
            "test-id",
            0L, // minimum seqNo
            1L, // minimum primaryTerm
            1L, // minimum version
            true,
            new BytesArray("{\"field\":\"value\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse1 = new GetResponse(getResult1);
        GetDocumentResponse protoResponse1 = GetDocumentResponseProtoUtils.toProto(getResponse1);
        assertEquals("0", protoResponse1.getGetDocumentResponseBody().getXSeqNo());
        assertEquals("1", protoResponse1.getGetDocumentResponseBody().getXPrimaryTerm());
        assertEquals("1", protoResponse1.getGetDocumentResponseBody().getXVersion());

        // Test maximum values
        GetResult getResult2 = new GetResult(
            "test-index",
            "test-id",
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            true,
            new BytesArray("{\"field\":\"value\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse2 = new GetResponse(getResult2);
        GetDocumentResponse protoResponse2 = GetDocumentResponseProtoUtils.toProto(getResponse2);
        assertEquals(String.valueOf(Long.MAX_VALUE), protoResponse2.getGetDocumentResponseBody().getXSeqNo());
        assertEquals(String.valueOf(Long.MAX_VALUE), protoResponse2.getGetDocumentResponseBody().getXPrimaryTerm());
        assertEquals(String.valueOf(Long.MAX_VALUE), protoResponse2.getGetDocumentResponseBody().getXVersion());
    }

    public void testToProtoSpecialCharactersInFields() {
        GetResult getResult = new GetResult(
            "test-index-with-dashes_and_underscores.and.dots",
            "test:id/with\\special@characters#and$symbols%",
            1L,
            2L,
            3L,
            true,
            new BytesArray("{\"field\":\"value with special chars: @#$%^&*()\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(getResponse);

        assertEquals("test-index-with-dashes_and_underscores.and.dots", protoResponse.getGetDocumentResponseBody().getXIndex());
        assertEquals("test:id/with\\special@characters#and$symbols%", protoResponse.getGetDocumentResponseBody().getXId());
    }

    public void testToProtoUnicodeCharacters() {
        GetResult getResult = new GetResult(
            "测试索引",
            "测试文档ID",
            1L,
            2L,
            3L,
            true,
            new BytesArray("{\"标题\":\"测试文档\",\"内容\":\"这是测试内容\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(getResponse);

        assertEquals("测试索引", protoResponse.getGetDocumentResponseBody().getXIndex());
        assertEquals("测试文档ID", protoResponse.getGetDocumentResponseBody().getXId());
        assertEquals("{\"标题\":\"测试文档\",\"内容\":\"这是测试内容\"}", protoResponse.getGetDocumentResponseBody().getXSource().toStringUtf8());
    }

    public void testToProtoEmptySource() {
        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            1L,
            2L,
            3L,
            true,
            new BytesArray("{}"), // Empty JSON object
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(getResponse);

        assertTrue(protoResponse.getGetDocumentResponseBody().getFound());
        assertEquals("{}", protoResponse.getGetDocumentResponseBody().getXSource().toStringUtf8());
    }

    public void testToProtoNullSource() {
        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            1L,
            2L,
            3L,
            true,
            null, // null source
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(getResponse);

        assertTrue(protoResponse.getGetDocumentResponseBody().getFound());
        assertTrue(protoResponse.getGetDocumentResponseBody().getXSource().isEmpty());
    }

    public void testToProtoConsistency() {
        GetResult getResult = new GetResult(
            "consistency-test",
            "doc-456",
            5L,
            10L,
            15L,
            true,
            new BytesArray("{\"consistency\":\"test\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse proto1 = GetDocumentResponseProtoUtils.toProto(getResponse);
        GetDocumentResponse proto2 = GetDocumentResponseProtoUtils.toProto(getResponse);

        // Test that multiple conversions yield identical results
        assertEquals(proto1.getGetDocumentResponseBody().getXIndex(), proto2.getGetDocumentResponseBody().getXIndex());
        assertEquals(proto1.getGetDocumentResponseBody().getXId(), proto2.getGetDocumentResponseBody().getXId());
        assertEquals(proto1.getGetDocumentResponseBody().getXSeqNo(), proto2.getGetDocumentResponseBody().getXSeqNo());
        assertEquals(proto1.getGetDocumentResponseBody().getXPrimaryTerm(), proto2.getGetDocumentResponseBody().getXPrimaryTerm());
        assertEquals(proto1.getGetDocumentResponseBody().getXVersion(), proto2.getGetDocumentResponseBody().getXVersion());
        assertEquals(proto1.getGetDocumentResponseBody().getFound(), proto2.getGetDocumentResponseBody().getFound());
        assertEquals(proto1.getGetDocumentResponseBody().getXSource(), proto2.getGetDocumentResponseBody().getXSource());
    }

    public void testToProtoNullGetResponse() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GetDocumentResponseProtoUtils.toProto(null)
        );
        assertEquals("GetResponse cannot be null", exception.getMessage());
    }

    public void testToProtoResponseStructure() {
        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            1L,
            2L,
            3L,
            true,
            new BytesArray("{\"field\":\"value\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(getResponse);

        // Verify the response structure
        assertNotNull(protoResponse);
        assertNotNull(protoResponse.getGetDocumentResponseBody());

        // Verify all required fields are present
        assertFalse(protoResponse.getGetDocumentResponseBody().getXIndex().isEmpty());
        assertFalse(protoResponse.getGetDocumentResponseBody().getXId().isEmpty());
        // Note: These are primitive fields, not objects, so no isEmpty() method
    }

    public void testToProtoLargeDocument() {
        // Test with a large document (1MB)
        StringBuilder largeContent = new StringBuilder();
        largeContent.append("{\"data\":\"");
        for (int i = 0; i < 100000; i++) {
            largeContent.append("0123456789");
        }
        largeContent.append("\"}");

        GetResult getResult = new GetResult(
            "large-doc-index",
            "large-doc-id",
            1L,
            2L,
            3L,
            true,
            new BytesArray(largeContent.toString()),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(getResponse);

        assertTrue(protoResponse.getGetDocumentResponseBody().getFound());
        assertTrue(protoResponse.getGetDocumentResponseBody().getXSource().size() > 1000000);
        assertEquals(largeContent.toString(), protoResponse.getGetDocumentResponseBody().getXSource().toStringUtf8());
    }

    public void testToProtoEdgeCaseVersions() {
        // Test with special version values used by OpenSearch
        GetResult getResult1 = new GetResult(
            "test-index",
            "test-id",
            -2L, // UNASSIGNED_SEQ_NO
            0L,  // UNASSIGNED_PRIMARY_TERM
            -1L, // NOT_FOUND
            false,
            null,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse1 = new GetResponse(getResult1);
        GetDocumentResponse protoResponse1 = GetDocumentResponseProtoUtils.toProto(getResponse1);
        assertEquals("-2", protoResponse1.getGetDocumentResponseBody().getXSeqNo());
        assertEquals("0", protoResponse1.getGetDocumentResponseBody().getXPrimaryTerm());
        assertEquals("-1", protoResponse1.getGetDocumentResponseBody().getXVersion());
        assertFalse(protoResponse1.getGetDocumentResponseBody().getFound());

        // Test with NO_OPS_PERFORMED (-1)
        GetResult getResult2 = new GetResult(
            "test-index",
            "test-id",
            -1L, // NO_OPS_PERFORMED
            1L,
            1L,
            true,
            new BytesArray("{\"field\":\"value\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse2 = new GetResponse(getResult2);
        GetDocumentResponse protoResponse2 = GetDocumentResponseProtoUtils.toProto(getResponse2);
        assertEquals("-1", protoResponse2.getGetDocumentResponseBody().getXSeqNo());
        assertTrue(protoResponse2.getGetDocumentResponseBody().getFound());
    }

    public void testToProtoSourceEncodingEdgeCases() {
        // Test with binary data in source (should be handled as bytes)
        byte[] binaryData = new byte[] { 0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE };

        GetResult getResult = new GetResult(
            "binary-index",
            "binary-id",
            1L,
            2L,
            3L,
            true,
            new BytesArray(binaryData),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        GetResponse getResponse = new GetResponse(getResult);
        GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(getResponse);

        assertTrue(protoResponse.getGetDocumentResponseBody().getFound());
        assertEquals(binaryData.length, protoResponse.getGetDocumentResponseBody().getXSource().size());
        assertArrayEquals(binaryData, protoResponse.getGetDocumentResponseBody().getXSource().toByteArray());
    }
}

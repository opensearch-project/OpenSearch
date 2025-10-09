/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document;

import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.protobufs.DeleteDocumentResponse;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Comprehensive unit tests for DeleteDocumentResponseProtoUtils.
 */
public class DeleteDocumentResponseProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoBasicFields() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 3L, true);

        DeleteDocumentResponse protoResponse = DeleteDocumentResponseProtoUtils.toProto(deleteResponse);

        assertEquals("test-index", protoResponse.getDeleteDocumentResponseBody().getXIndex());
        assertEquals("test-id", protoResponse.getDeleteDocumentResponseBody().getXId());
        assertEquals("1", protoResponse.getDeleteDocumentResponseBody().getXPrimaryTerm());
        assertEquals("2", protoResponse.getDeleteDocumentResponseBody().getXSeqNo());
        assertEquals("3", protoResponse.getDeleteDocumentResponseBody().getXVersion());
        assertEquals("deleted", protoResponse.getDeleteDocumentResponseBody().getResult());
    }

    public void testToProtoDeletedAndNotFound() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Test successful deletion
        DeleteResponse deletedResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 3L, true);
        DeleteDocumentResponse deletedProto = DeleteDocumentResponseProtoUtils.toProto(deletedResponse);
        assertEquals("deleted", deletedProto.getDeleteDocumentResponseBody().getResult());

        // Test document not found
        DeleteResponse notFoundResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 1L, false);
        DeleteDocumentResponse notFoundProto = DeleteDocumentResponseProtoUtils.toProto(notFoundResponse);
        assertEquals("not_found", notFoundProto.getDeleteDocumentResponseBody().getResult());
    }

    public void testToProtoVersionBoundaries() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Test minimum version
        DeleteResponse response1 = new DeleteResponse(shardId, "test-id", 1L, 0L, 1L, true);
        DeleteDocumentResponse proto1 = DeleteDocumentResponseProtoUtils.toProto(response1);
        assertEquals("0", proto1.getDeleteDocumentResponseBody().getXSeqNo());
        assertEquals("1", proto1.getDeleteDocumentResponseBody().getXVersion());

        // Test large version numbers
        DeleteResponse response2 = new DeleteResponse(shardId, "test-id", Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, true);
        DeleteDocumentResponse proto2 = DeleteDocumentResponseProtoUtils.toProto(response2);
        assertEquals(String.valueOf(Long.MAX_VALUE), proto2.getDeleteDocumentResponseBody().getXPrimaryTerm());
        assertEquals(String.valueOf(Long.MAX_VALUE), proto2.getDeleteDocumentResponseBody().getXSeqNo());
        assertEquals(String.valueOf(Long.MAX_VALUE), proto2.getDeleteDocumentResponseBody().getXVersion());
    }

    public void testToProtoSpecialCharactersInFields() {
        ShardId shardId = new ShardId("test-index-with-dashes_and_underscores.and.dots", "test-uuid-123", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "test:id/with\\special@characters#and$symbols%", 1L, 2L, 3L, true);

        DeleteDocumentResponse protoResponse = DeleteDocumentResponseProtoUtils.toProto(deleteResponse);

        assertEquals("test-index-with-dashes_and_underscores.and.dots", protoResponse.getDeleteDocumentResponseBody().getXIndex());
        assertEquals("test:id/with\\special@characters#and$symbols%", protoResponse.getDeleteDocumentResponseBody().getXId());
    }

    public void testToProtoUnicodeCharacters() {
        ShardId shardId = new ShardId("测试索引", "测试UUID", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "测试文档ID", 1L, 2L, 3L, true);

        DeleteDocumentResponse protoResponse = DeleteDocumentResponseProtoUtils.toProto(deleteResponse);

        assertEquals("测试索引", protoResponse.getDeleteDocumentResponseBody().getXIndex());
        assertEquals("测试文档ID", protoResponse.getDeleteDocumentResponseBody().getXId());
    }

    public void testToProtoConsistency() {
        ShardId shardId = new ShardId("consistency-test", "uuid-123", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "doc-456", 5L, 10L, 15L, true);

        DeleteDocumentResponse proto1 = DeleteDocumentResponseProtoUtils.toProto(deleteResponse);
        DeleteDocumentResponse proto2 = DeleteDocumentResponseProtoUtils.toProto(deleteResponse);

        // Test that multiple conversions yield identical results
        assertEquals(proto1.getDeleteDocumentResponseBody().getXIndex(), proto2.getDeleteDocumentResponseBody().getXIndex());
        assertEquals(proto1.getDeleteDocumentResponseBody().getXId(), proto2.getDeleteDocumentResponseBody().getXId());
        assertEquals(proto1.getDeleteDocumentResponseBody().getXPrimaryTerm(), proto2.getDeleteDocumentResponseBody().getXPrimaryTerm());
        assertEquals(proto1.getDeleteDocumentResponseBody().getXSeqNo(), proto2.getDeleteDocumentResponseBody().getXSeqNo());
        assertEquals(proto1.getDeleteDocumentResponseBody().getXVersion(), proto2.getDeleteDocumentResponseBody().getXVersion());
        assertEquals(proto1.getDeleteDocumentResponseBody().getResult(), proto2.getDeleteDocumentResponseBody().getResult());
    }

    public void testToProtoNullDeleteResponse() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> DeleteDocumentResponseProtoUtils.toProto(null)
        );
        assertEquals("DeleteResponse cannot be null", exception.getMessage());
    }

    public void testToProtoShardIdHandling() {
        // Test different shard configurations
        ShardId shardId1 = new ShardId("index1", "uuid1", 0);
        DeleteResponse response1 = new DeleteResponse(shardId1, "id1", 1L, 1L, 1L, true);
        DeleteDocumentResponse proto1 = DeleteDocumentResponseProtoUtils.toProto(response1);
        assertEquals("index1", proto1.getDeleteDocumentResponseBody().getXIndex());

        ShardId shardId2 = new ShardId("index2", "uuid2", 5);
        DeleteResponse response2 = new DeleteResponse(shardId2, "id2", 2L, 2L, 2L, false);
        DeleteDocumentResponse proto2 = DeleteDocumentResponseProtoUtils.toProto(response2);
        assertEquals("index2", proto2.getDeleteDocumentResponseBody().getXIndex());
    }

    public void testToProtoResultMapping() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Test deleted result
        DeleteResponse deletedResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 3L, true);
        DeleteDocumentResponse deletedProto = DeleteDocumentResponseProtoUtils.toProto(deletedResponse);
        assertEquals("deleted", deletedProto.getDeleteDocumentResponseBody().getResult());

        // Test not_found result
        DeleteResponse notFoundResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 1L, false);
        DeleteDocumentResponse notFoundProto = DeleteDocumentResponseProtoUtils.toProto(notFoundResponse);
        assertEquals("not_found", notFoundProto.getDeleteDocumentResponseBody().getResult());
    }

    public void testToProtoResponseStructure() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 3L, true);

        DeleteDocumentResponse protoResponse = DeleteDocumentResponseProtoUtils.toProto(deleteResponse);

        // Verify the response structure
        assertNotNull(protoResponse);
        assertNotNull(protoResponse.getDeleteDocumentResponseBody());

        // Verify all required fields are present
        assertFalse(protoResponse.getDeleteDocumentResponseBody().getXIndex().isEmpty());
        assertFalse(protoResponse.getDeleteDocumentResponseBody().getXId().isEmpty());
        // Note: These are primitive fields, not objects, so no isEmpty() method
    }

    public void testToProtoVersionConstraintScenarios() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Test scenario where document was already deleted (version conflict)
        DeleteResponse conflictResponse = new DeleteResponse(shardId, "test-id", 3L, 5L, 2L, false);
        DeleteDocumentResponse conflictProto = DeleteDocumentResponseProtoUtils.toProto(conflictResponse);
        assertEquals("not_found", conflictProto.getDeleteDocumentResponseBody().getResult());
        assertEquals("3", conflictProto.getDeleteDocumentResponseBody().getXPrimaryTerm());
        assertEquals("5", conflictProto.getDeleteDocumentResponseBody().getXSeqNo());
        assertEquals("2", conflictProto.getDeleteDocumentResponseBody().getXVersion());

        // Test successful deletion with high version numbers
        DeleteResponse successResponse = new DeleteResponse(shardId, "test-id", 10L, 20L, 15L, true);
        DeleteDocumentResponse successProto = DeleteDocumentResponseProtoUtils.toProto(successResponse);
        assertEquals("deleted", successProto.getDeleteDocumentResponseBody().getResult());
        assertEquals("10", successProto.getDeleteDocumentResponseBody().getXPrimaryTerm());
        assertEquals("20", successProto.getDeleteDocumentResponseBody().getXSeqNo());
        assertEquals("15", successProto.getDeleteDocumentResponseBody().getXVersion());
    }

    public void testToProtoMultipleShardScenarios() {
        // Test responses from different shards of the same index
        ShardId shard0 = new ShardId("multi-shard-index", "uuid-123", 0);
        ShardId shard1 = new ShardId("multi-shard-index", "uuid-123", 1);
        ShardId shard2 = new ShardId("multi-shard-index", "uuid-123", 2);

        DeleteResponse response0 = new DeleteResponse(shard0, "doc-0", 1L, 1L, 1L, true);
        DeleteResponse response1 = new DeleteResponse(shard1, "doc-1", 1L, 2L, 1L, true);
        DeleteResponse response2 = new DeleteResponse(shard2, "doc-2", 1L, 3L, 1L, false);

        DeleteDocumentResponse proto0 = DeleteDocumentResponseProtoUtils.toProto(response0);
        DeleteDocumentResponse proto1 = DeleteDocumentResponseProtoUtils.toProto(response1);
        DeleteDocumentResponse proto2 = DeleteDocumentResponseProtoUtils.toProto(response2);

        // All should have the same index name
        assertEquals("multi-shard-index", proto0.getDeleteDocumentResponseBody().getXIndex());
        assertEquals("multi-shard-index", proto1.getDeleteDocumentResponseBody().getXIndex());
        assertEquals("multi-shard-index", proto2.getDeleteDocumentResponseBody().getXIndex());

        // But different document IDs and results
        assertEquals("doc-0", proto0.getDeleteDocumentResponseBody().getXId());
        assertEquals("doc-1", proto1.getDeleteDocumentResponseBody().getXId());
        assertEquals("doc-2", proto2.getDeleteDocumentResponseBody().getXId());

        assertEquals("deleted", proto0.getDeleteDocumentResponseBody().getResult());
        assertEquals("deleted", proto1.getDeleteDocumentResponseBody().getResult());
        assertEquals("not_found", proto2.getDeleteDocumentResponseBody().getResult());
    }

    public void testToProtoEdgeCaseVersions() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Test with version 0 (should not happen in practice but test boundary)
        DeleteResponse response1 = new DeleteResponse(shardId, "test-id", 1L, 1L, 0L, true);
        DeleteDocumentResponse proto1 = DeleteDocumentResponseProtoUtils.toProto(response1);
        assertEquals("0", proto1.getDeleteDocumentResponseBody().getXVersion());

        // Test with negative sequence number (UNASSIGNED_SEQ_NO = -2)
        DeleteResponse response2 = new DeleteResponse(shardId, "test-id", 1L, -2L, 1L, false);
        DeleteDocumentResponse proto2 = DeleteDocumentResponseProtoUtils.toProto(response2);
        assertEquals("-2", proto2.getDeleteDocumentResponseBody().getXSeqNo());
    }
}

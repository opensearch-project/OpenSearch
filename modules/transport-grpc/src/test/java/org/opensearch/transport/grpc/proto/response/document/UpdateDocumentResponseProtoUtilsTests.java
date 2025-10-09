/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document;

import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.get.GetResult;
import org.opensearch.protobufs.UpdateDocumentResponse;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for UpdateDocumentResponseProtoUtils.
 */
public class UpdateDocumentResponseProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoBasicFields() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);

        UpdateDocumentResponse protoResponse = UpdateDocumentResponseProtoUtils.toProto(updateResponse);

        assertEquals("test-index", protoResponse.getUpdateDocumentResponseBody().getXIndex());
        assertEquals("test-id", protoResponse.getUpdateDocumentResponseBody().getXId());
        assertEquals("1", protoResponse.getUpdateDocumentResponseBody().getXPrimaryTerm());
        assertEquals("2", protoResponse.getUpdateDocumentResponseBody().getXSeqNo());
        assertEquals("3", protoResponse.getUpdateDocumentResponseBody().getXVersion());
        assertEquals("updated", protoResponse.getUpdateDocumentResponseBody().getResult());
    }

    public void testToProtoAllResults() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Test UPDATED result
        UpdateResponse updatedResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);
        UpdateDocumentResponse updatedProto = UpdateDocumentResponseProtoUtils.toProto(updatedResponse);
        assertEquals("updated", updatedProto.getUpdateDocumentResponseBody().getResult());

        // Test CREATED result
        UpdateResponse createdResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 1L, UpdateResponse.Result.CREATED);
        UpdateDocumentResponse createdProto = UpdateDocumentResponseProtoUtils.toProto(createdResponse);
        assertEquals("created", createdProto.getUpdateDocumentResponseBody().getResult());

        // Test DELETED result
        UpdateResponse deletedResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 4L, UpdateResponse.Result.DELETED);
        UpdateDocumentResponse deletedProto = UpdateDocumentResponseProtoUtils.toProto(deletedResponse);
        assertEquals("deleted", deletedProto.getUpdateDocumentResponseBody().getResult());

        // Test NOOP result
        UpdateResponse noopResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.NOOP);
        UpdateDocumentResponse noopProto = UpdateDocumentResponseProtoUtils.toProto(noopResponse);
        assertEquals("noop", noopProto.getUpdateDocumentResponseBody().getResult());
    }

    public void testToProtoWithGetResult() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        GetResult getResult = mock(GetResult.class);
        when(getResult.isExists()).thenReturn(true);
        when(getResult.getIndex()).thenReturn("test-index");
        when(getResult.getId()).thenReturn("test-id");
        when(getResult.getVersion()).thenReturn(5L);

        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);
        updateResponse.setGetResult(getResult);

        UpdateDocumentResponse protoResponse = UpdateDocumentResponseProtoUtils.toProto(updateResponse);

        assertEquals("test-index", protoResponse.getUpdateDocumentResponseBody().getXIndex());
        assertEquals("test-id", protoResponse.getUpdateDocumentResponseBody().getXId());
        assertEquals("updated", protoResponse.getUpdateDocumentResponseBody().getResult());
        // Note: GetResult conversion is skipped in current implementation
    }

    public void testToProtoVersionBoundaries() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Test minimum version
        UpdateResponse response1 = new UpdateResponse(shardId, "test-id", 1L, 0L, 1L, UpdateResponse.Result.CREATED);
        UpdateDocumentResponse proto1 = UpdateDocumentResponseProtoUtils.toProto(response1);
        assertEquals("0", proto1.getUpdateDocumentResponseBody().getXSeqNo());
        assertEquals("1", proto1.getUpdateDocumentResponseBody().getXVersion());

        // Test large version numbers
        UpdateResponse response2 = new UpdateResponse(
            shardId,
            "test-id",
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            UpdateResponse.Result.UPDATED
        );
        UpdateDocumentResponse proto2 = UpdateDocumentResponseProtoUtils.toProto(response2);
        assertEquals(String.valueOf(Long.MAX_VALUE), proto2.getUpdateDocumentResponseBody().getXPrimaryTerm());
        assertEquals(String.valueOf(Long.MAX_VALUE), proto2.getUpdateDocumentResponseBody().getXSeqNo());
        assertEquals(String.valueOf(Long.MAX_VALUE), proto2.getUpdateDocumentResponseBody().getXVersion());
    }

    public void testToProtoSpecialCharactersInFields() {
        ShardId shardId = new ShardId("test-index-with-dashes_and_underscores.and.dots", "test-uuid-123", 0);
        UpdateResponse updateResponse = new UpdateResponse(
            shardId,
            "test:id/with\\special@characters#and$symbols%",
            1L,
            2L,
            3L,
            UpdateResponse.Result.UPDATED
        );

        UpdateDocumentResponse protoResponse = UpdateDocumentResponseProtoUtils.toProto(updateResponse);

        assertEquals("test-index-with-dashes_and_underscores.and.dots", protoResponse.getUpdateDocumentResponseBody().getXIndex());
        assertEquals("test:id/with\\special@characters#and$symbols%", protoResponse.getUpdateDocumentResponseBody().getXId());
    }

    public void testToProtoUnicodeCharacters() {
        ShardId shardId = new ShardId("测试索引", "测试UUID", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "测试文档ID", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);

        UpdateDocumentResponse protoResponse = UpdateDocumentResponseProtoUtils.toProto(updateResponse);

        assertEquals("测试索引", protoResponse.getUpdateDocumentResponseBody().getXIndex());
        assertEquals("测试文档ID", protoResponse.getUpdateDocumentResponseBody().getXId());
    }

    public void testToProtoConsistency() {
        ShardId shardId = new ShardId("consistency-test", "uuid-123", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "doc-456", 5L, 10L, 15L, UpdateResponse.Result.UPDATED);

        UpdateDocumentResponse proto1 = UpdateDocumentResponseProtoUtils.toProto(updateResponse);
        UpdateDocumentResponse proto2 = UpdateDocumentResponseProtoUtils.toProto(updateResponse);

        // Test that multiple conversions yield identical results
        assertEquals(proto1.getUpdateDocumentResponseBody().getXIndex(), proto2.getUpdateDocumentResponseBody().getXIndex());
        assertEquals(proto1.getUpdateDocumentResponseBody().getXId(), proto2.getUpdateDocumentResponseBody().getXId());
        assertEquals(proto1.getUpdateDocumentResponseBody().getXPrimaryTerm(), proto2.getUpdateDocumentResponseBody().getXPrimaryTerm());
        assertEquals(proto1.getUpdateDocumentResponseBody().getXSeqNo(), proto2.getUpdateDocumentResponseBody().getXSeqNo());
        assertEquals(proto1.getUpdateDocumentResponseBody().getXVersion(), proto2.getUpdateDocumentResponseBody().getXVersion());
        assertEquals(proto1.getUpdateDocumentResponseBody().getResult(), proto2.getUpdateDocumentResponseBody().getResult());
    }

    public void testToProtoNullUpdateResponse() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> UpdateDocumentResponseProtoUtils.toProto(null)
        );
        assertEquals("UpdateResponse cannot be null", exception.getMessage());
    }

    public void testToProtoShardIdHandling() {
        // Test different shard configurations
        ShardId shardId1 = new ShardId("index1", "uuid1", 0);
        UpdateResponse response1 = new UpdateResponse(shardId1, "id1", 1L, 1L, 1L, UpdateResponse.Result.CREATED);
        UpdateDocumentResponse proto1 = UpdateDocumentResponseProtoUtils.toProto(response1);
        assertEquals("index1", proto1.getUpdateDocumentResponseBody().getXIndex());

        ShardId shardId2 = new ShardId("index2", "uuid2", 5);
        UpdateResponse response2 = new UpdateResponse(shardId2, "id2", 2L, 2L, 2L, UpdateResponse.Result.UPDATED);
        UpdateDocumentResponse proto2 = UpdateDocumentResponseProtoUtils.toProto(response2);
        assertEquals("index2", proto2.getUpdateDocumentResponseBody().getXIndex());
    }

    public void testToProtoResultMapping() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Verify all result types are properly mapped
        for (UpdateResponse.Result result : UpdateResponse.Result.values()) {
            UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, result);
            UpdateDocumentResponse protoResponse = UpdateDocumentResponseProtoUtils.toProto(updateResponse);

            String expectedResult = result.getLowercase();
            assertEquals(expectedResult, protoResponse.getUpdateDocumentResponseBody().getResult());
        }
    }

    public void testToProtoResponseStructure() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);

        UpdateDocumentResponse protoResponse = UpdateDocumentResponseProtoUtils.toProto(updateResponse);

        // Verify the response structure
        assertNotNull(protoResponse);
        assertNotNull(protoResponse.getUpdateDocumentResponseBody());

        // Verify all required fields are present
        assertFalse(protoResponse.getUpdateDocumentResponseBody().getXIndex().isEmpty());
        assertFalse(protoResponse.getUpdateDocumentResponseBody().getXId().isEmpty());
        // Note: These are primitive fields, not objects, so no isEmpty() method
    }
}

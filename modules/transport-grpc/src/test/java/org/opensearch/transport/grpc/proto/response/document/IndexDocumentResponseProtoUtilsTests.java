/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.protobufs.IndexDocumentResponse;
import org.opensearch.protobufs.Result;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for IndexDocumentResponseProtoUtils.
 */
public class IndexDocumentResponseProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoBasicFields() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1L, 2L, 3L, true);

        IndexDocumentResponse protoResponse = IndexDocumentResponseProtoUtils.toProto(indexResponse);

        assertTrue(protoResponse.hasIndexDocumentResponseBody());
        assertEquals("test-index", protoResponse.getIndexDocumentResponseBody().getXIndex());
        assertEquals("test-id", protoResponse.getIndexDocumentResponseBody().getXId());
        assertEquals(1L, protoResponse.getIndexDocumentResponseBody().getXSeqNo());
        assertEquals(2L, protoResponse.getIndexDocumentResponseBody().getXPrimaryTerm());
        assertEquals(3L, protoResponse.getIndexDocumentResponseBody().getXVersion());
        assertEquals(Result.RESULT_CREATED, protoResponse.getIndexDocumentResponseBody().getResult());
    }

    public void testToProtoWithUpdatedResult() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1L, 2L, 3L, false);

        IndexDocumentResponse protoResponse = IndexDocumentResponseProtoUtils.toProto(indexResponse);

        assertEquals(Result.RESULT_UPDATED, protoResponse.getIndexDocumentResponseBody().getResult());
    }

    public void testToProtoWithShardInfo() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1L, 2L, 3L, true);

        // Set shard info
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(2, 1, new ReplicationResponse.ShardInfo.Failure[0]);
        indexResponse.setShardInfo(shardInfo);

        IndexDocumentResponse protoResponse = IndexDocumentResponseProtoUtils.toProto(indexResponse);

        assertTrue(protoResponse.hasIndexDocumentResponseBody());
        // Note: ShardInfo conversion is skipped in current implementation
        // This test verifies the method doesn't fail when ShardInfo is present
    }

    public void testErrorProtoThrowsException() {
        Exception testException = new RuntimeException("Test error");

        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> IndexDocumentResponseProtoUtils.toErrorProto(testException, org.opensearch.core.rest.RestStatus.BAD_REQUEST)
        );
        assertEquals("Use GrpcErrorHandler.convertToGrpcError() instead", exception.getMessage());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document.bulk;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.get.GetResult;
import org.opensearch.protobufs.Item;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulkItemResponseProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithIndexResponse() throws IOException {
        // Create a ShardId
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Create a ShardInfo with no failures
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3, new ReplicationResponse.ShardInfo.Failure[0]);

        // Create an IndexResponse
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1, 2, 3, true);
        indexResponse.setShardInfo(shardInfo);

        // Create a BulkItemResponse with the IndexResponse
        BulkItemResponse bulkItemResponse = new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, indexResponse);

        // Convert to protobuf Item
        Item item = BulkItemResponseProtoUtils.toProto(bulkItemResponse);

        // Verify the result
        assertNotNull("Item should not be null", item);
        assertTrue("Item should have index field set", item.hasIndex());
        assertEquals("Index should match", "test-index", item.getIndex().getIndex());
        assertEquals("Id should match", "test-id", item.getIndex().getId().getString());
        assertEquals("Version should match", indexResponse.getVersion(), item.getIndex().getVersion());
        assertEquals("Result should match", DocWriteResponse.Result.CREATED.getLowercase(), item.getIndex().getResult());
    }

    public void testToProtoWithCreateResponse() throws IOException {
        // Create a ShardId
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Create a ShardInfo with no failures
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3, new ReplicationResponse.ShardInfo.Failure[0]);

        // Create an IndexResponse
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1, 2, 3, true);
        indexResponse.setShardInfo(shardInfo);

        // Create a BulkItemResponse with the IndexResponse and CREATE op type
        BulkItemResponse bulkItemResponse = new BulkItemResponse(0, DocWriteRequest.OpType.CREATE, indexResponse);

        // Convert to protobuf Item
        Item item = BulkItemResponseProtoUtils.toProto(bulkItemResponse);

        // Verify the result
        assertNotNull("Item should not be null", item);
        assertTrue("Item should have create field set", item.hasCreate());
        assertEquals("Index should match", "test-index", item.getCreate().getIndex());
        assertEquals("Id should match", "test-id", item.getCreate().getId().getString());
        assertEquals("Version should match", indexResponse.getVersion(), item.getCreate().getVersion());
        assertEquals("Result should match", DocWriteResponse.Result.CREATED.getLowercase(), item.getCreate().getResult());
    }

    public void testToProtoWithDeleteResponse() throws IOException {
        // Create a ShardId
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Create a ShardInfo with no failures
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3, new ReplicationResponse.ShardInfo.Failure[0]);

        // Create a DeleteResponse
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "test-id", 1, 2, 3, true);
        deleteResponse.setShardInfo(shardInfo);

        // Create a BulkItemResponse with the DeleteResponse
        BulkItemResponse bulkItemResponse = new BulkItemResponse(0, DocWriteRequest.OpType.DELETE, deleteResponse);

        // Convert to protobuf Item
        Item item = BulkItemResponseProtoUtils.toProto(bulkItemResponse);

        // Verify the result
        assertNotNull("Item should not be null", item);
        assertTrue("Item should have delete field set", item.hasDelete());
        assertEquals("Index should match", "test-index", item.getDelete().getIndex());
        assertEquals("Id should match", "test-id", item.getDelete().getId().getString());
        assertEquals("Version should match", deleteResponse.getVersion(), item.getDelete().getVersion());
        assertEquals("Result should match", DocWriteResponse.Result.DELETED.getLowercase(), item.getDelete().getResult());
    }

    public void testToProtoWithUpdateResponse() throws IOException {
        // Create a ShardId
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Create a ShardInfo with no failures
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3, new ReplicationResponse.ShardInfo.Failure[0]);

        // Create an UpdateResponse
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1, 2, 3, DocWriteResponse.Result.UPDATED);
        updateResponse.setShardInfo(shardInfo);

        // Create a BulkItemResponse with the UpdateResponse
        BulkItemResponse bulkItemResponse = new BulkItemResponse(0, DocWriteRequest.OpType.UPDATE, updateResponse);

        // Convert to protobuf Item
        Item item = BulkItemResponseProtoUtils.toProto(bulkItemResponse);

        // Verify the result
        assertNotNull("Item should not be null", item);
        assertTrue("Item should have update field set", item.hasUpdate());
        assertEquals("Index should match", "test-index", item.getUpdate().getIndex());
        assertEquals("Id should match", "test-id", item.getUpdate().getId().getString());
        assertEquals("Version should match", updateResponse.getVersion(), item.getUpdate().getVersion());
        assertEquals("Result should match", DocWriteResponse.Result.UPDATED.getLowercase(), item.getUpdate().getResult());
    }

    public void testToProtoWithUpdateResponseAndGetResult() throws IOException {
        // Create a ShardId
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Create a ShardInfo with no failures
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3, new ReplicationResponse.ShardInfo.Failure[0]);

        // Create a GetResult
        Map<String, DocumentField> sourceMap = new HashMap<>();
        sourceMap.put("field1", new DocumentField("field1", List.of("value1")));
        sourceMap.put("field2", new DocumentField("field1", List.of(42)));

        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            0,
            1,
            1,
            true,
            new BytesArray("{\"field1\":\"value1\",\"field2\":42}".getBytes(StandardCharsets.UTF_8)),
            sourceMap,
            null
        );

        // Create an UpdateResponse with GetResult
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1, 2, 3, DocWriteResponse.Result.UPDATED);
        updateResponse.setShardInfo(shardInfo);
        updateResponse.setGetResult(getResult);

        // Create a BulkItemResponse with the UpdateResponse
        BulkItemResponse bulkItemResponse = new BulkItemResponse(0, DocWriteRequest.OpType.UPDATE, updateResponse);

        // Convert to protobuf Item
        Item item = BulkItemResponseProtoUtils.toProto(bulkItemResponse);

        // Verify the result
        assertNotNull("Item should not be null", item);
        assertTrue("Item should have update field set", item.hasUpdate());
        assertEquals("Index should match", "test-index", item.getUpdate().getIndex());
        assertEquals("Id should match", "test-id", item.getUpdate().getId().getString());
        assertEquals("Version should match", 1, item.getUpdate().getVersion());
        assertEquals("Result should match", DocWriteResponse.Result.UPDATED.getLowercase(), item.getUpdate().getResult());

        // Verify GetResult fields
        assertTrue("Get field should be set", item.getUpdate().hasGet());
        assertEquals("Get index should match", "test-index", item.getUpdate().getIndex());
        assertEquals("Get id should match", "test-id", item.getUpdate().getId().getString());
        assertTrue("Get found should be true", item.getUpdate().getGet().getFound());
    }

    public void testToProtoWithFailure() throws IOException {
        // Create a failure
        Exception exception = new IOException("Test IO exception");
        BulkItemResponse.Failure failure = new BulkItemResponse.Failure(
            "test-index",
            "test-id",
            exception,
            RestStatus.INTERNAL_SERVER_ERROR
        );

        // Create a BulkItemResponse with the failure
        BulkItemResponse bulkItemResponse = new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, failure);

        // Convert to protobuf Item
        Item item = BulkItemResponseProtoUtils.toProto(bulkItemResponse);

        // Verify the result
        assertNotNull("Item should not be null", item);
        assertTrue("Item should have index field set", item.hasIndex());
        assertEquals("Index should match", "test-index", item.getIndex().getIndex());
        assertEquals("Id should match", "test-id", item.getIndex().getId().getString());
        assertEquals("Status should match", RestStatus.INTERNAL_SERVER_ERROR.getStatus(), item.getIndex().getStatus());

        // Verify error
        assertTrue("Error should be set", item.getIndex().hasError());
        assertTrue("Error reason should contain exception message", item.getIndex().getError().getReason().contains("Test IO exception"));
    }

    public void testToProtoWithNullResponse() throws IOException {
        // Call toProto with null, should throw NullPointerException
        expectThrows(NullPointerException.class, () -> BulkItemResponseProtoUtils.toProto(null));
    }
}

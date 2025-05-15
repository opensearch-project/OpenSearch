/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.response;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugin.transport.grpc.proto.response.document.bulk.BulkResponseProtoUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class BulkResponseProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithSuccessfulResponse() throws IOException {
        // Create a successful BulkResponse
        BulkItemResponse[] responses = new BulkItemResponse[1];
        Index index = new Index("test-index", "_na_");
        ShardId shardId = new ShardId(index, 1);
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1, 1, 1, true);
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo();
        indexResponse.setShardInfo(shardInfo);
        responses[0] = new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, indexResponse);

        BulkResponse bulkResponse = new BulkResponse(responses, 100);

        // Convert to Protocol Buffer
        org.opensearch.protobufs.BulkResponse protoResponse = BulkResponseProtoUtils.toProto(bulkResponse);

        // Verify the conversion
        assertEquals("Should have the correct took time", 100, protoResponse.getBulkResponseBody().getTook());
        assertFalse("Should not have errors", protoResponse.getBulkResponseBody().getErrors());
        assertEquals("Should have 1 item", 1, protoResponse.getBulkResponseBody().getItemsCount());

        // Verify the item response
        org.opensearch.protobufs.Item item = protoResponse.getBulkResponseBody().getItems(0);
        org.opensearch.protobufs.ResponseItem responseItem = item.getIndex();
        assertEquals("Should have the correct index", "test-index", responseItem.getIndex());
        assertEquals("Should have the correct id", "test-id", responseItem.getId().getString());
        assertEquals("Should have the correct status", 201, responseItem.getStatus());
    }

    public void testToProtoWithFailedResponse() throws IOException {
        // Create a failed BulkResponse
        BulkItemResponse[] responses = new BulkItemResponse[1];
        Exception exception = new Exception("Test failure");
        responses[0] = new BulkItemResponse(
            0,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("test-index", "test-id", exception)
        );

        BulkResponse bulkResponse = new BulkResponse(responses, 100);

        // Convert to Protocol Buffer
        org.opensearch.protobufs.BulkResponse protoResponse = BulkResponseProtoUtils.toProto(bulkResponse);

        // Verify the conversion
        assertEquals("Should have the correct took time", 100, protoResponse.getBulkResponseBody().getTook());
        assertTrue("Should have errors", protoResponse.getBulkResponseBody().getErrors());
        assertEquals("Should have 1 item", 1, protoResponse.getBulkResponseBody().getItemsCount());

        // Verify the item response
        org.opensearch.protobufs.Item item = protoResponse.getBulkResponseBody().getItems(0);
        org.opensearch.protobufs.ResponseItem responseItem = item.getIndex();
        assertEquals("Should have the correct index", "test-index", responseItem.getIndex());
        assertEquals("Should have the correct id", "test-id", responseItem.getId().getString());
        assertTrue("Should have error", responseItem.getError().getReason().length() > 0);
    }

    public void testToProtoWithIngestTook() throws IOException {
        // Create a BulkResponse with ingest took time
        BulkItemResponse[] responses = new BulkItemResponse[1];
        Index index = new Index("test-index", "_na_");
        ShardId shardId = new ShardId(index, 1);
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1, 1, 1, true);
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo();
        indexResponse.setShardInfo(shardInfo);
        responses[0] = new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, indexResponse);

        // Set ingest took time to 50ms
        BulkResponse bulkResponse = new BulkResponse(responses, 100, 50);

        // Convert to Protocol Buffer
        org.opensearch.protobufs.BulkResponse protoResponse = BulkResponseProtoUtils.toProto(bulkResponse);

        // Verify the conversion
        assertEquals("Should have the correct took time", 100, protoResponse.getBulkResponseBody().getTook());
        assertEquals("Should have the correct ingest took time", 50, protoResponse.getBulkResponseBody().getIngestTook());
    }
}

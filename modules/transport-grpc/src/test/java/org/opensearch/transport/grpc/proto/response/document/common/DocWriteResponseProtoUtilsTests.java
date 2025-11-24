/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document.common;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.protobufs.ResponseItem;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DocWriteResponseProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithIndexResponse() throws IOException {
        // Create a ShardId
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Create a ShardInfo with no failures
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3, new ReplicationResponse.ShardInfo.Failure[0]);

        // Create an IndexResponse
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1, 2, 3, true);
        indexResponse.setShardInfo(shardInfo);
        indexResponse.setForcedRefresh(true);

        // Convert to protobuf ResponseItem.Builder
        ResponseItem.Builder responseItemBuilder = DocWriteResponseProtoUtils.toProto(indexResponse);

        // Verify the result
        assertNotNull("ResponseItem.Builder should not be null", responseItemBuilder);

        // Build the ResponseItem to verify its contents
        ResponseItem responseItem = responseItemBuilder.build();

        // Verify basic fields
        assertEquals("Index should match", "test-index", responseItem.getXIndex());
        assertEquals("Id should match", "test-id", responseItem.getXId());
        assertEquals("Version should match", indexResponse.getVersion(), responseItem.getXVersion());
        assertEquals("Result should match", DocWriteResponse.Result.CREATED.getLowercase(), responseItem.getResult());
        assertTrue("ForcedRefresh should be true", responseItem.getForcedRefresh());

        // Verify sequence number and primary term
        assertEquals("SeqNo should match", indexResponse.getSeqNo(), responseItem.getXSeqNo());
        assertEquals("PrimaryTerm should match", indexResponse.getPrimaryTerm(), responseItem.getXPrimaryTerm());

        // Verify ShardInfo
        assertNotNull("ShardInfo should not be null", responseItem.getXShards());
        assertEquals("Total shards should match", 5, responseItem.getXShards().getTotal());
        assertEquals("Successful shards should match", 3, responseItem.getXShards().getSuccessful());
        assertEquals("Failed shards should match", indexResponse.getShardInfo().getFailed(), responseItem.getXShards().getFailed());
    }

    public void testToProtoWithEmptyId() throws IOException {
        // Create a ShardId
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Create a ShardInfo with no failures
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3, new ReplicationResponse.ShardInfo.Failure[0]);

        // Create an IndexResponse with empty ID
        IndexResponse indexResponse = new IndexResponse(shardId, "", 1, 2, 3, true);
        indexResponse.setShardInfo(shardInfo);

        // Convert to protobuf ResponseItem.Builder
        ResponseItem.Builder responseItemBuilder = DocWriteResponseProtoUtils.toProto(indexResponse);

        // Verify the result
        assertNotNull("ResponseItem.Builder should not be null", responseItemBuilder);

        // Build the ResponseItem to verify its contents
        ResponseItem responseItem = responseItemBuilder.build();

        // Verify ID is set to null value
        assertFalse("Id should not be set", responseItem.hasXId());
    }

    public void testToProtoWithNoSeqNo() throws IOException {
        // Create a ShardId
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Create a ShardInfo with no failures
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3, new ReplicationResponse.ShardInfo.Failure[0]);

        // Create an IndexResponse with negative sequence number (unassigned)
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", -1, 1, 3, true);
        indexResponse.setShardInfo(shardInfo);

        // Convert to protobuf ResponseItem.Builder
        ResponseItem.Builder responseItemBuilder = DocWriteResponseProtoUtils.toProto(indexResponse);

        // Verify the result
        assertNotNull("ResponseItem.Builder should not be null", responseItemBuilder);

        // Build the ResponseItem to verify its contents
        ResponseItem responseItem = responseItemBuilder.build();

        // Verify sequence number and primary term are not set
        assertFalse("SeqNo should not be set", responseItem.hasXSeqNo());
        assertFalse("PrimaryTerm should not be set", responseItem.hasXPrimaryTerm());
    }

    public void testToProtoWithNullResponse() throws IOException {
        // Call toProto with null, should throw NullPointerException
        expectThrows(NullPointerException.class, () -> DocWriteResponseProtoUtils.toProto(null));
    }
}

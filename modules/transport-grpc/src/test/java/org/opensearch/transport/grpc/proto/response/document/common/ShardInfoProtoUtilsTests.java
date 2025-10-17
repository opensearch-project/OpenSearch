/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document.common;

import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.protobufs.ShardInfo;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShardInfoProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithNoFailures() throws IOException {
        // Create a ShardInfo with no failures
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3, new ReplicationResponse.ShardInfo.Failure[0]);

        // Convert to protobuf ShardInfo
        ShardInfo protoShardInfo = ShardInfoProtoUtils.toProto(shardInfo);

        // Verify the result
        assertNotNull("ShardInfo should not be null", protoShardInfo);
        assertEquals("Total should match", 5, protoShardInfo.getTotal());
        assertEquals("Successful should match", 3, protoShardInfo.getSuccessful());
        assertEquals("Failed should match", shardInfo.getFailed(), protoShardInfo.getFailed());
        assertEquals("Failures list should be empty", 0, protoShardInfo.getFailuresCount());
    }

    public void testToProtoWithFailures() throws IOException {
        // Create failures
        List<ReplicationResponse.ShardInfo.Failure> failuresList = new ArrayList<>();
        ShardId shardId = new ShardId(new Index("index1", "1"), 1);
        ShardId shardId2 = new ShardId(new Index("index2", "2"), 2);

        // Add a failure with an exception
        Exception exception1 = new IOException("Test IO exception");
        failuresList.add(new ReplicationResponse.ShardInfo.Failure(shardId, "node0", exception1, RestStatus.INTERNAL_SERVER_ERROR, true));

        // Add another failure with a different exception
        Exception exception2 = new IllegalArgumentException("Test argument exception");
        failuresList.add(new ReplicationResponse.ShardInfo.Failure(shardId2, "node1", exception2, RestStatus.BAD_REQUEST, false));

        // Create a ShardInfo with failures
        ReplicationResponse.ShardInfo.Failure[] failures = failuresList.toArray(new ReplicationResponse.ShardInfo.Failure[0]);
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3, failures);

        // Convert to protobuf ShardInfo
        ShardInfo protoShardInfo = ShardInfoProtoUtils.toProto(shardInfo);

        // Verify the result
        assertNotNull("ShardInfo should not be null", protoShardInfo);
        assertEquals("Total should match", 5, protoShardInfo.getTotal());
        assertEquals("Successful should match", 3, protoShardInfo.getSuccessful());
        assertEquals("Failed should match", 2, protoShardInfo.getFailed());
        assertEquals("Failures list should have 2 entries", 2, protoShardInfo.getFailuresCount());

        // Verify first failure
        assertEquals("First failure index should match", "index1", protoShardInfo.getFailures(0).getIndex());
        assertEquals("First failure shard should match", 1, protoShardInfo.getFailures(0).getShard());
        assertTrue(
            "First failure reason should contain exception message",
            protoShardInfo.getFailures(0).getReason().getReason().contains("Test IO exception")
        );
        assertEquals("First failure status should match", "INTERNAL_SERVER_ERROR", protoShardInfo.getFailures(0).getStatus());
        assertTrue("First failure primary flag should be true", protoShardInfo.getFailures(0).getPrimary());

        // Verify second failure
        assertEquals("Second failure index should match", "index2", protoShardInfo.getFailures(1).getIndex());
        assertEquals("Second failure shard should match", 2, protoShardInfo.getFailures(1).getShard());
        assertTrue(
            "Second failure reason should contain exception message",
            protoShardInfo.getFailures(1).getReason().getReason().contains("Test argument exception")
        );
        assertEquals("Second failure status should match", "BAD_REQUEST", protoShardInfo.getFailures(1).getStatus());
        assertFalse("Second failure primary flag should be false", protoShardInfo.getFailures(1).getPrimary());
    }

    public void testToProtoWithNullShardInfo() throws IOException {
        // Call toProto with null, should throw NullPointerException
        expectThrows(NullPointerException.class, () -> ShardInfoProtoUtils.toProto(null));
    }
}

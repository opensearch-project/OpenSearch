/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.protobufs.ShardFailure;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.snapshots.SnapshotShardFailure;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class ShardOperationFailedExceptionProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithShardSearchFailure() throws IOException {

        // Create a SearchShardTarget with a nodeId
        ShardId shardId = new ShardId("test_index", "_na_", 1);
        SearchShardTarget searchShardTarget = new SearchShardTarget("test_node", shardId, null, null);

        // Create a ShardSearchFailure
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(new Exception("fake exception"), searchShardTarget);

        // Call the method under test
        ShardFailure protoFailure = ShardOperationFailedExceptionProtoUtils.toProto(shardSearchFailure);

        // Verify the result
        assertNotNull("Proto failure should not be null", protoFailure);
        assertEquals("Index should match", "test_index", protoFailure.getIndex());
        assertEquals("Shard ID should match", 1, protoFailure.getShard());
        assertEquals("Node ID should match", "test_node", protoFailure.getNode());
    }

    public void testToProtoWithSnapshotShardFailure() throws IOException {

        // Create a SearchShardTarget with a nodeId
        ShardId shardId = new ShardId("test_index", "_na_", 2);

        // Create a SnapshotShardFailure
        SnapshotShardFailure shardSearchFailure = new SnapshotShardFailure("test_node", shardId, "Snapshot failed");

        // Call the method under test
        ShardFailure protoFailure = ShardOperationFailedExceptionProtoUtils.toProto(shardSearchFailure);

        // Verify the result
        assertNotNull("Proto failure should not be null", protoFailure);
        assertEquals("Index should match", "test_index", protoFailure.getIndex());
        assertEquals("Shard ID should match", 2, protoFailure.getShard());
        assertEquals("Node ID should match", "test_node", protoFailure.getNode());
        assertEquals("Status should match", "INTERNAL_SERVER_ERROR", protoFailure.getStatus());
    }

    public void testToProtoWithDefaultShardOperationFailedException() throws IOException {
        // Create a mock DefaultShardOperationFailedException
        DefaultShardOperationFailedException defaultShardOperationFailedException = new DefaultShardOperationFailedException(
            "test_index",
            3,
            new RuntimeException("Test exception")
        );

        // Call the method under test
        ShardFailure protoFailure = ShardOperationFailedExceptionProtoUtils.toProto(defaultShardOperationFailedException);

        // Verify the result
        assertNotNull("Proto failure should not be null", protoFailure);
        assertEquals("Index should match", "test_index", protoFailure.getIndex());
        assertEquals("Shard ID should match", 3, protoFailure.getShard());
        assertEquals("Status should match", "INTERNAL_SERVER_ERROR", protoFailure.getStatus());
    }

    public void testToProtoWithReplicationResponseShardInfoFailure() throws IOException {
        // Create a mock ReplicationResponse.ShardInfo.Failure
        ShardId shardId = new ShardId("test_index", "_na_", 4);
        ReplicationResponse.ShardInfo.Failure replicationResponseFailure = new ReplicationResponse.ShardInfo.Failure(
            shardId,
            "test_node",
            new RuntimeException("Test exception"),
            RestStatus.INTERNAL_SERVER_ERROR,
            true
        );

        // Call the method under test
        ShardFailure protoFailure = ShardOperationFailedExceptionProtoUtils.toProto(replicationResponseFailure);

        // Verify the result
        assertNotNull("Proto failure should not be null", protoFailure);
        assertEquals("Index should match", "test_index", protoFailure.getIndex());
        assertEquals("Shard ID should match", 4, protoFailure.getShard());
        assertTrue("Primary should be true", protoFailure.getPrimary());
        assertEquals("Node ID should match", "test_node", protoFailure.getNode());
        assertEquals("Status should match", "INTERNAL_SERVER_ERROR", protoFailure.getStatus());
    }

    public void testToProtoWithUnsupportedShardOperationFailedException() {
        // Create a mock ShardOperationFailedException that is not one of the supported types
        ShardOperationFailedException mockFailure = mock(ShardOperationFailedException.class);

        // Call the method under test, should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> ShardOperationFailedExceptionProtoUtils.toProto(mockFailure)
        );

        assertTrue(
            "Exception message should mention unsupported ShardOperationFailedException",
            exception.getMessage().contains("Unsupported ShardOperationFailedException")
        );
    }
}

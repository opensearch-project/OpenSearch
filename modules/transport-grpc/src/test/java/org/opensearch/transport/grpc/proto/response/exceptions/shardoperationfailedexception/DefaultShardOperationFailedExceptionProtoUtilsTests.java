/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.protobufs.ShardFailure;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DefaultShardOperationFailedExceptionProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithDefaultShardOperationFailedException() throws IOException {
        // Create a real DefaultShardOperationFailedException
        DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(
            "test-index",
            1,
            new RuntimeException("Test cause")
        );

        // Call the method under test
        ShardFailure shardFailure = DefaultShardOperationFailedExceptionProtoUtils.toProto(exception);

        // Verify the result
        assertNotNull("ShardFailure should not be null", shardFailure);
        assertEquals("Shard ID should match", 1, shardFailure.getShard());
        assertEquals("Index should match", "test-index", shardFailure.getIndex());
        assertEquals("Status should match", RestStatus.INTERNAL_SERVER_ERROR.name(), shardFailure.getStatus());
        assertNotNull("Reason should not be null", shardFailure.getReason());
    }

    public void testToProtoWithAddIndexBlockResponseFailure() throws IOException {
        // Create a real AddIndexBlockResponse.AddBlockShardResult.Failure
        AddIndexBlockResponse.AddBlockShardResult.Failure exception = new AddIndexBlockResponse.AddBlockShardResult.Failure(
            "test-index-2",
            2,
            new RuntimeException("Test cause 2"),
            "node-1"
        );

        // Call the method under test
        ShardFailure shardFailure = DefaultShardOperationFailedExceptionProtoUtils.toProto(exception);

        // Verify the result
        assertNotNull("ShardFailure should not be null", shardFailure);
        assertEquals("Shard ID should match", 2, shardFailure.getShard());
        assertEquals("Index should match", "test-index-2", shardFailure.getIndex());
        assertEquals("Status should match", RestStatus.INTERNAL_SERVER_ERROR.name(), shardFailure.getStatus());
        assertEquals("Node should match", "node-1", shardFailure.getNode());
        assertNotNull("Reason should not be null", shardFailure.getReason());
    }

    public void testToProtoWithIndicesShardStoresResponseFailure() throws IOException {
        // Create a real IndicesShardStoresResponse.Failure
        IndicesShardStoresResponse.Failure exception = new IndicesShardStoresResponse.Failure(
            "node-2",
            "test-index-3",
            3,
            new RuntimeException("Test cause 3")
        );

        // Call the method under test
        ShardFailure shardFailure = DefaultShardOperationFailedExceptionProtoUtils.toProto(exception);

        // Verify the result
        assertNotNull("ShardFailure should not be null", shardFailure);
        assertEquals("Shard ID should match", 3, shardFailure.getShard());
        assertEquals("Index should match", "test-index-3", shardFailure.getIndex());
        assertEquals("Status should match", RestStatus.INTERNAL_SERVER_ERROR.name(), shardFailure.getStatus());
        assertEquals("Node should match", "node-2", shardFailure.getNode());
        assertNotNull("Reason should not be null", shardFailure.getReason());
    }

    public void testToProtoWithCloseIndexResponseFailure() throws IOException {
        // Create a real CloseIndexResponse.ShardResult.Failure
        CloseIndexResponse.ShardResult.Failure exception = new CloseIndexResponse.ShardResult.Failure(
            "test-index-4",
            4,
            new RuntimeException("Test cause 4"),
            "node-3"
        );

        // Call the method under test
        ShardFailure shardFailure = DefaultShardOperationFailedExceptionProtoUtils.toProto(exception);

        // Verify the result
        assertNotNull("ShardFailure should not be null", shardFailure);
        assertEquals("Shard ID should match", 4, shardFailure.getShard());
        assertEquals("Index should match", "test-index-4", shardFailure.getIndex());
        assertEquals("Status should match", RestStatus.INTERNAL_SERVER_ERROR.name(), shardFailure.getStatus());
        assertEquals("Node should match", "node-3", shardFailure.getNode());
        assertNotNull("Reason should not be null", shardFailure.getReason());
    }

    public void testToProtoWithNullNodeId() throws IOException {
        // Create a real AddIndexBlockResponse.AddBlockShardResult.Failure with null nodeId
        AddIndexBlockResponse.AddBlockShardResult.Failure exception = new AddIndexBlockResponse.AddBlockShardResult.Failure(
            "test-index-5",
            5,
            new RuntimeException("Test cause 5"),
            null // null nodeId
        );

        // Call the method under test
        ShardFailure shardFailure = DefaultShardOperationFailedExceptionProtoUtils.toProto(exception);

        // Verify the result
        assertNotNull("ShardFailure should not be null", shardFailure);
        assertEquals("Shard ID should match", 5, shardFailure.getShard());
        assertEquals("Index should match", "test-index-5", shardFailure.getIndex());
        assertEquals("Status should match", RestStatus.INTERNAL_SERVER_ERROR.name(), shardFailure.getStatus());
        assertFalse("Node should not be set", shardFailure.hasNode());
        assertNotNull("Reason should not be null", shardFailure.getReason());
    }

    public void testToProtoWithNull() throws IOException {
        ShardFailure shardFailure = DefaultShardOperationFailedExceptionProtoUtils.toProto(null);
        assertNotNull("ShardFailure should not be null", shardFailure);
        assertFalse("Index should not be set", shardFailure.hasIndex());
        assertFalse("Status should not be set", shardFailure.hasStatus());
        assertFalse("Node should not be set", shardFailure.hasNode());
        assertFalse("Reason should not be set", shardFailure.hasReason());
    }
}

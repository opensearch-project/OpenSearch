/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.document.bulk;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.protobufs.WaitForActiveShards;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.plugin.transport.grpc.proto.request.document.bulk.ActiveShardCountProtoUtils.getActiveShardCount;

public class ActiveShardCountProtoUtilsTests extends OpenSearchTestCase {

    public void testGetActiveShardCountWithNoWaitForActiveShards() {
        // Create a BulkRequest
        BulkRequest bulkRequest = new BulkRequest();

        // Create a protobuf BulkRequest with no wait_for_active_shards
        org.opensearch.protobufs.BulkRequest protoRequest = org.opensearch.protobufs.BulkRequest.newBuilder().build();

        BulkRequest result = getActiveShardCount(bulkRequest, protoRequest);

        // Verify the result
        assertSame("Should return the same BulkRequest instance", bulkRequest, result);
        assertEquals("Should have default active shard count", ActiveShardCount.DEFAULT, result.waitForActiveShards());
    }

    public void testGetActiveShardCountWithWaitForActiveShardsAll() {
        // Create a BulkRequest
        BulkRequest bulkRequest = new BulkRequest();

        // Create a protobuf BulkRequest with wait_for_active_shards = ALL (value 1)
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder()
            .setWaitForActiveShardOptionsValue(1) // WAIT_FOR_ACTIVE_SHARD_OPTIONS_ALL = 1
            .build();

        org.opensearch.protobufs.BulkRequest protoRequest = org.opensearch.protobufs.BulkRequest.newBuilder()
            .setWaitForActiveShards(waitForActiveShards)
            .build();

        BulkRequest result = getActiveShardCount(bulkRequest, protoRequest);

        // Verify the result
        assertSame("Should return the same BulkRequest instance", bulkRequest, result);
        assertEquals("Should have ALL active shard count", ActiveShardCount.ALL, result.waitForActiveShards());
    }

    public void testGetActiveShardCountWithWaitForActiveShardsDefault() {
        // Create a BulkRequest
        BulkRequest bulkRequest = new BulkRequest();

        // Create a protobuf BulkRequest with wait_for_active_shards = DEFAULT (value 2)
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder()
            .setWaitForActiveShardOptionsValue(2) // WAIT_FOR_ACTIVE_SHARD_OPTIONS_DEFAULT = 2
            .build();

        org.opensearch.protobufs.BulkRequest protoRequest = org.opensearch.protobufs.BulkRequest.newBuilder()
            .setWaitForActiveShards(waitForActiveShards)
            .build();

        BulkRequest result = getActiveShardCount(bulkRequest, protoRequest);

        // Verify the result
        assertSame("Should return the same BulkRequest instance", bulkRequest, result);
        assertEquals("Should have DEFAULT active shard count", ActiveShardCount.DEFAULT, result.waitForActiveShards());
    }

    public void testGetActiveShardCountWithWaitForActiveShardsUnspecified() {
        // Create a BulkRequest
        BulkRequest bulkRequest = new BulkRequest();

        // Create a protobuf BulkRequest with wait_for_active_shards = UNSPECIFIED (value 0)
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder()
            .setWaitForActiveShardOptionsValue(0) // WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED = 0
            .build();

        org.opensearch.protobufs.BulkRequest protoRequest = org.opensearch.protobufs.BulkRequest.newBuilder()
            .setWaitForActiveShards(waitForActiveShards)
            .build();

        expectThrows(UnsupportedOperationException.class, () -> getActiveShardCount(bulkRequest, protoRequest));
    }

    public void testGetActiveShardCountWithWaitForActiveShardsInt32() {
        // Create a BulkRequest
        BulkRequest bulkRequest = new BulkRequest();

        // Create a protobuf BulkRequest with wait_for_active_shards = 2
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder().setInt32Value(2).build();

        org.opensearch.protobufs.BulkRequest protoRequest = org.opensearch.protobufs.BulkRequest.newBuilder()
            .setWaitForActiveShards(waitForActiveShards)
            .build();

        BulkRequest result = getActiveShardCount(bulkRequest, protoRequest);

        // Verify the result
        assertSame("Should return the same BulkRequest instance", bulkRequest, result);
        assertEquals("Should have active shard count of 2", ActiveShardCount.from(2), result.waitForActiveShards());
    }

    public void testGetActiveShardCountWithWaitForActiveShardsNoCase() {
        // Create a BulkRequest
        BulkRequest bulkRequest = new BulkRequest();

        // Create a protobuf BulkRequest with wait_for_active_shards but no case set
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder().build();

        org.opensearch.protobufs.BulkRequest protoRequest = org.opensearch.protobufs.BulkRequest.newBuilder()
            .setWaitForActiveShards(waitForActiveShards)
            .build();

        // Call getActiveShardCount, should throw UnsupportedOperationException
        expectThrows(UnsupportedOperationException.class, () -> getActiveShardCount(bulkRequest, protoRequest));
    }
}

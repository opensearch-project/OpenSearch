/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.document.bulk;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.protobufs.WaitForActiveShards;
import org.opensearch.test.OpenSearchTestCase;

public class ActiveShardCountProtoUtilsTests extends OpenSearchTestCase {

    public void testGetActiveShardCountWithNoWaitForActiveShards() {

        ActiveShardCount result = ActiveShardCountProtoUtils.parseProto(WaitForActiveShards.newBuilder().build());

        // Verify the result
        assertEquals("Should have default active shard count", ActiveShardCount.DEFAULT, result);
    }

    public void testGetActiveShardCountWithWaitForActiveShardsAll() {
        // Create a protobuf BulkRequest with wait_for_active_shards = ALL (value 1)
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder()
            .setWaitForActiveShardOptionsValue(1) // WAIT_FOR_ACTIVE_SHARD_OPTIONS_ALL = 1
            .build();

        ActiveShardCount result = ActiveShardCountProtoUtils.parseProto(waitForActiveShards);

        // Verify the result
        assertEquals("Should have ALL active shard count", ActiveShardCount.ALL, result);
    }

    public void testGetActiveShardCountWithWaitForActiveShardsDefault() {

        // Create a protobuf BulkRequest with wait_for_active_shards = DEFAULT (value 2)
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder()
            .setWaitForActiveShardOptionsValue(2) // WAIT_FOR_ACTIVE_SHARD_OPTIONS_DEFAULT = 2
            .build();

        ActiveShardCount result = ActiveShardCountProtoUtils.parseProto(waitForActiveShards);

        // Verify the result
        assertEquals("Should have DEFAULT active shard count", ActiveShardCount.DEFAULT, result);
    }

    public void testGetActiveShardCountWithWaitForActiveShardsUnspecified() {
        // Create a protobuf BulkRequest with wait_for_active_shards = UNSPECIFIED (value 0)
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder()
            .setWaitForActiveShardOptionsValue(0) // WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED = 0
            .build();

        expectThrows(UnsupportedOperationException.class, () -> ActiveShardCountProtoUtils.parseProto(waitForActiveShards));
    }

    public void testGetActiveShardCountWithWaitForActiveShardsInt32() {

        // Create a protobuf BulkRequest with wait_for_active_shards = 2
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder().setInt32Value(2).build();

        ActiveShardCount result = ActiveShardCountProtoUtils.parseProto(waitForActiveShards);

        // Verify the result
        assertEquals("Should have active shard count of 2", ActiveShardCount.from(2), result);
    }

    public void testGetActiveShardCountWithWaitForActiveShardsNoCase() {
        // Create a protobuf BulkRequest with wait_for_active_shards but no case set
        WaitForActiveShards waitForActiveShards = WaitForActiveShards.newBuilder().build();

        ActiveShardCount result = ActiveShardCountProtoUtils.parseProto(waitForActiveShards);

        // Verify the result
        assertEquals("Should have DEFAULT active shard count", ActiveShardCount.DEFAULT, result);
    }
}

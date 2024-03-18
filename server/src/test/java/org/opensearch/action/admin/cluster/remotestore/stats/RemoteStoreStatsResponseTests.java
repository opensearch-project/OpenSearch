/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Map;

import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.compareStatsResponse;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createEmptyTranslogStats;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createShardRouting;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForNewPrimary;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForNewReplica;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createStatsForRemoteStoreRestoredPrimary;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createTranslogStats;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class RemoteStoreStatsResponseTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_store_stats_test");
        shardId = new ShardId("index", "uuid", 0);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testSerializationForPrimary() throws Exception {
        RemoteSegmentTransferTracker.Stats mockSegmentTrackerStats = createStatsForNewPrimary(shardId);
        RemoteTranslogTransferTracker.Stats mockTranslogTrackerStats = createTranslogStats(shardId);
        ShardRouting primaryShardRouting = createShardRouting(shardId, true);
        RemoteStoreStats primaryShardStats = new RemoteStoreStats(mockSegmentTrackerStats, mockTranslogTrackerStats, primaryShardRouting);
        RemoteStoreStatsResponse statsResponse = new RemoteStoreStatsResponse(
            new RemoteStoreStats[] { primaryShardStats },
            1,
            1,
            0,
            new ArrayList<DefaultShardOperationFailedException>()
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        statsResponse.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonResponseObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();
        Map<String, Object> metadataShardsObject = (Map<String, Object>) jsonResponseObject.get("_shards");
        assertEquals(metadataShardsObject.get("total"), 1);
        assertEquals(metadataShardsObject.get("successful"), 1);
        assertEquals(metadataShardsObject.get("failed"), 0);
        Map<String, Object> indicesObject = (Map<String, Object>) jsonResponseObject.get("indices");
        assertTrue(indicesObject.containsKey("index"));
        Map<String, Object> shardsObject = (Map) ((Map) indicesObject.get("index")).get("shards");
        ArrayList<Map<String, Object>> perShardNumberObject = (ArrayList<Map<String, Object>>) shardsObject.get("0");
        assertEquals(perShardNumberObject.size(), 1);
        Map<String, Object> perShardCopyObject = perShardNumberObject.get(0);
        compareStatsResponse(perShardCopyObject, mockSegmentTrackerStats, mockTranslogTrackerStats, primaryShardRouting);
    }

    public void testSerializationForBothPrimaryAndReplica() throws Exception {
        RemoteSegmentTransferTracker.Stats mockPrimarySegmentTrackerStats = createStatsForNewPrimary(shardId);
        RemoteSegmentTransferTracker.Stats mockReplicaSegmentTrackerStats = createStatsForNewReplica(shardId);
        RemoteTranslogTransferTracker.Stats mockPrimaryTranslogTrackerStats = createTranslogStats(shardId);
        RemoteTranslogTransferTracker.Stats mockReplicaTranslogTrackerStats = createEmptyTranslogStats(shardId);
        ShardRouting primaryShardRouting = createShardRouting(shardId, true);
        ShardRouting replicaShardRouting = createShardRouting(shardId, false);
        RemoteStoreStats primaryShardStats = new RemoteStoreStats(
            mockPrimarySegmentTrackerStats,
            mockPrimaryTranslogTrackerStats,
            primaryShardRouting
        );
        RemoteStoreStats replicaShardStats = new RemoteStoreStats(
            mockReplicaSegmentTrackerStats,
            mockReplicaTranslogTrackerStats,
            replicaShardRouting
        );
        RemoteStoreStatsResponse statsResponse = new RemoteStoreStatsResponse(
            new RemoteStoreStats[] { primaryShardStats, replicaShardStats },
            2,
            2,
            0,
            new ArrayList<DefaultShardOperationFailedException>()
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        statsResponse.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonResponseObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();
        Map<String, Object> metadataShardsObject = (Map<String, Object>) jsonResponseObject.get("_shards");
        assertEquals(2, metadataShardsObject.get("total"));
        assertEquals(2, metadataShardsObject.get("successful"));
        assertEquals(0, metadataShardsObject.get("failed"));
        Map<String, Object> indicesObject = (Map<String, Object>) jsonResponseObject.get("indices");
        assertTrue(indicesObject.containsKey("index"));
        Map<String, Object> shardsObject = (Map) ((Map) indicesObject.get("index")).get("shards");
        ArrayList<Map<String, Object>> perShardNumberObject = (ArrayList<Map<String, Object>>) shardsObject.get("0");
        assertEquals(2, perShardNumberObject.size());
        perShardNumberObject.forEach(shardObject -> {
            boolean isPrimary = (boolean) ((Map) shardObject.get(RemoteStoreStats.Fields.ROUTING)).get(
                RemoteStoreStats.RoutingFields.PRIMARY
            );
            if (isPrimary) {
                compareStatsResponse(shardObject, mockPrimarySegmentTrackerStats, mockPrimaryTranslogTrackerStats, primaryShardRouting);
            } else {
                compareStatsResponse(shardObject, mockReplicaSegmentTrackerStats, mockReplicaTranslogTrackerStats, replicaShardRouting);
            }
        });
    }

    public void testSerializationForBothRemoteStoreRestoredPrimaryAndReplica() throws Exception {
        RemoteSegmentTransferTracker.Stats mockPrimarySegmentTrackerStats = createStatsForRemoteStoreRestoredPrimary(shardId);
        RemoteSegmentTransferTracker.Stats mockReplicaSegmentTrackerStats = createStatsForNewReplica(shardId);
        RemoteTranslogTransferTracker.Stats mockPrimaryTranslogTrackerStats = createTranslogStats(shardId);
        RemoteTranslogTransferTracker.Stats mockReplicaTranslogTrackerStats = createEmptyTranslogStats(shardId);
        ShardRouting primaryShardRouting = createShardRouting(shardId, true);
        ShardRouting replicaShardRouting = createShardRouting(shardId, false);
        RemoteStoreStats primaryShardStats = new RemoteStoreStats(
            mockPrimarySegmentTrackerStats,
            mockPrimaryTranslogTrackerStats,
            primaryShardRouting
        );
        RemoteStoreStats replicaShardStats = new RemoteStoreStats(
            mockReplicaSegmentTrackerStats,
            mockReplicaTranslogTrackerStats,
            replicaShardRouting
        );
        RemoteStoreStatsResponse statsResponse = new RemoteStoreStatsResponse(
            new RemoteStoreStats[] { primaryShardStats, replicaShardStats },
            2,
            2,
            0,
            new ArrayList<DefaultShardOperationFailedException>()
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        statsResponse.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonResponseObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();
        Map<String, Object> metadataShardsObject = (Map<String, Object>) jsonResponseObject.get("_shards");
        assertEquals(2, metadataShardsObject.get("total"));
        assertEquals(2, metadataShardsObject.get("successful"));
        assertEquals(0, metadataShardsObject.get("failed"));
        Map<String, Object> indicesObject = (Map<String, Object>) jsonResponseObject.get("indices");
        assertTrue(indicesObject.containsKey("index"));
        Map<String, Object> shardsObject = (Map) ((Map) indicesObject.get("index")).get("shards");
        ArrayList<Map<String, Object>> perShardNumberObject = (ArrayList<Map<String, Object>>) shardsObject.get("0");
        assertEquals(2, perShardNumberObject.size());
        perShardNumberObject.forEach(shardObject -> {
            boolean isPrimary = (boolean) ((Map) shardObject.get(RemoteStoreStats.Fields.ROUTING)).get(
                RemoteStoreStats.RoutingFields.PRIMARY
            );
            if (isPrimary) {
                compareStatsResponse(shardObject, mockPrimarySegmentTrackerStats, mockPrimaryTranslogTrackerStats, primaryShardRouting);
            } else {
                compareStatsResponse(shardObject, mockReplicaSegmentTrackerStats, mockReplicaTranslogTrackerStats, replicaShardRouting);
            }
        });
    }
}

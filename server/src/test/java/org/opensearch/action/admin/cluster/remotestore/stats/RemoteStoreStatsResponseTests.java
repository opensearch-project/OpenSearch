/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Map;

import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.compareStatsResponse;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createPressureTrackerStats;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createShardRouting;
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

    public void testSerialization() throws Exception {
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = createPressureTrackerStats(shardId);
        ShardRouting primaryShardRouting = createShardRouting(shardId, true);
        RemoteStoreStats primaryShardStats = new RemoteStoreStats(pressureTrackerStats, primaryShardRouting);
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
        Map<String, Object> shardsObject = (Map)((Map) indicesObject.get("index")).get("shards");
        ArrayList<Map<String, Object>> perShardNumberObject = (ArrayList<Map<String, Object>>) shardsObject.get("0");
        assertEquals(perShardNumberObject.size(), 1);
        compareStatsResponse(perShardNumberObject.get(0), pressureTrackerStats, primaryShardRouting);
    }
}

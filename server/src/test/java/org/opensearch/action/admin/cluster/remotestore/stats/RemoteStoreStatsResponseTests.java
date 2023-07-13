/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Map;

import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.compareStatsResponse;
import static org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsTestHelper.createPressureTrackerStats;
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
        RemoteStoreStats stats = new RemoteStoreStats(pressureTrackerStats);
        RemoteStoreStatsResponse statsResponse = new RemoteStoreStatsResponse(
            new RemoteStoreStats[] { stats },
            1,
            1,
            0,
            new ArrayList<DefaultShardOperationFailedException>()
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        statsResponse.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonResponseObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();

        ArrayList<Map<String, Object>> statsObjectArray = (ArrayList<Map<String, Object>>) jsonResponseObject.get("stats");
        assertEquals(statsObjectArray.size(), 1);
        Map<String, Object> statsObject = statsObjectArray.get(0);
        Map<String, Object> shardsObject = (Map<String, Object>) jsonResponseObject.get("_shards");

        assertEquals(shardsObject.get("total"), 1);
        assertEquals(shardsObject.get("successful"), 1);
        assertEquals(shardsObject.get("failed"), 0);
        compareStatsResponse(statsObject, pressureTrackerStats);
    }
}

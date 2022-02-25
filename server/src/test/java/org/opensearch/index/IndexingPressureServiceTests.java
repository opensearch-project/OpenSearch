/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.junit.Before;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.bulk.BulkItemRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Requests;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.stats.IndexingPressurePerShardStats;
import org.opensearch.index.stats.IndexingPressureStats;
import org.opensearch.test.OpenSearchTestCase;

public class IndexingPressureServiceTests extends OpenSearchTestCase {

    private final Settings settings = Settings.builder()
        .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB")
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
        .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
        .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "20ms")
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
        .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 100)
        .build();

    private ClusterSettings clusterSettings;
    private ClusterService clusterService;

    @Before
    public void beforeTest() {
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterService = new ClusterService(settings, clusterSettings, null);
    }

    public void testCoordinatingOperationForShardIndexingPressure() {
        IndexingPressureService service = new IndexingPressureService(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        BulkItemRequest[] items = new BulkItemRequest[1];
        DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        items[0] = new BulkItemRequest(0, writeRequest);
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, WriteRequest.RefreshPolicy.NONE, items);
        Releasable releasable = service.markCoordinatingOperationStarted(shardId, bulkShardRequest::ramBytesUsed, false);

        IndexingPressurePerShardStats shardStats = service.shardStats(CommonStatsFlags.ALL).getIndexingPressureShardStats(shardId);
        assertEquals(bulkShardRequest.ramBytesUsed(), shardStats.getCurrentCoordinatingBytes());
        releasable.close();
    }

    public void testCoordinatingOperationForIndexingPressure() {
        IndexingPressureService service = new IndexingPressureService(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        Settings.Builder updated = Settings.builder();
        clusterSettings.updateDynamicSettings(
            Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false).build(),
            Settings.builder().put(settings),
            updated,
            getTestClass().getName()
        );
        clusterSettings.applySettings(updated.build());

        BulkRequest bulkRequest = new BulkRequest();
        Releasable releasable = service.markCoordinatingOperationStarted(bulkRequest::ramBytesUsed, false);
        IndexingPressurePerShardStats shardStats = service.shardStats(CommonStatsFlags.ALL).getIndexingPressureShardStats(shardId);
        assertNull(shardStats);
        IndexingPressureStats nodeStats = service.nodeStats();
        assertEquals(bulkRequest.ramBytesUsed(), nodeStats.getCurrentCoordinatingBytes());
        releasable.close();
    }

    public void testPrimaryOperationForShardIndexingPressure() {
        IndexingPressureService service = new IndexingPressureService(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);

        Releasable releasable = service.markPrimaryOperationStarted(shardId, 1024, false);

        IndexingPressurePerShardStats shardStats = service.shardStats(CommonStatsFlags.ALL).getIndexingPressureShardStats(shardId);
        assertEquals(1024, shardStats.getCurrentPrimaryBytes());
        releasable.close();
    }

    public void testPrimaryOperationForIndexingPressure() {
        IndexingPressureService service = new IndexingPressureService(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        Settings.Builder updated = Settings.builder();
        clusterSettings.updateDynamicSettings(
            Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false).build(),
            Settings.builder().put(settings),
            updated,
            getTestClass().getName()
        );
        clusterSettings.applySettings(updated.build());

        Releasable releasable = service.markPrimaryOperationStarted(shardId, 1024, false);

        IndexingPressurePerShardStats shardStats = service.shardStats(CommonStatsFlags.ALL).getIndexingPressureShardStats(shardId);
        assertNull(shardStats);
        IndexingPressureStats nodeStats = service.nodeStats();
        assertEquals(1024, nodeStats.getCurrentPrimaryBytes());
        releasable.close();
    }

    public void testLocalPrimaryOperationForShardIndexingPressure() {
        IndexingPressureService service = new IndexingPressureService(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);

        Releasable releasable = service.markPrimaryOperationLocalToCoordinatingNodeStarted(shardId, 1024);

        IndexingPressurePerShardStats shardStats = service.shardStats(CommonStatsFlags.ALL).getIndexingPressureShardStats(shardId);
        assertEquals(1024, shardStats.getCurrentPrimaryBytes());
        releasable.close();
    }

    public void testLocalPrimaryOperationForIndexingPressure() {
        IndexingPressureService service = new IndexingPressureService(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        Settings.Builder updated = Settings.builder();
        clusterSettings.updateDynamicSettings(
            Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false).build(),
            Settings.builder().put(settings),
            updated,
            getTestClass().getName()
        );
        clusterSettings.applySettings(updated.build());

        Releasable releasable = service.markPrimaryOperationLocalToCoordinatingNodeStarted(shardId, 1024);

        IndexingPressurePerShardStats shardStats = service.shardStats(CommonStatsFlags.ALL).getIndexingPressureShardStats(shardId);
        assertNull(shardStats);
        IndexingPressureStats nodeStats = service.nodeStats();
        assertEquals(1024, nodeStats.getCurrentPrimaryBytes());
        releasable.close();
    }

    public void testReplicaOperationForShardIndexingPressure() {
        IndexingPressureService service = new IndexingPressureService(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);

        Releasable releasable = service.markReplicaOperationStarted(shardId, 1024, false);

        IndexingPressurePerShardStats shardStats = service.shardStats(CommonStatsFlags.ALL).getIndexingPressureShardStats(shardId);
        assertEquals(1024, shardStats.getCurrentReplicaBytes());
        releasable.close();
    }

    public void testReplicaOperationForIndexingPressure() {
        IndexingPressureService service = new IndexingPressureService(settings, clusterService);
        Index index = new Index("IndexName", "UUID");
        ShardId shardId = new ShardId(index, 0);
        Settings.Builder updated = Settings.builder();
        clusterSettings.updateDynamicSettings(
            Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false).build(),
            Settings.builder().put(settings),
            updated,
            getTestClass().getName()
        );
        clusterSettings.applySettings(updated.build());

        Releasable releasable = service.markReplicaOperationStarted(shardId, 1024, false);

        IndexingPressurePerShardStats shardStats = service.shardStats(CommonStatsFlags.ALL).getIndexingPressureShardStats(shardId);
        assertNull(shardStats);
        IndexingPressureStats nodeStats = service.nodeStats();
        assertEquals(1024, nodeStats.getCurrentReplicaBytes());
        releasable.close();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.opensearch.index.SegmentReplicationPressureService.MAX_REPLICATION_TIME_SETTING;
import static org.opensearch.index.SegmentReplicationPressureService.SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED;

public class SegmentReplicationPressureServiceTests extends OpenSearchIndexLevelReplicationTestCase {

    private static ShardStateAction shardStateAction = Mockito.mock(ShardStateAction.class);
    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .put(SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED.getKey(), true)
        .put(MAX_REPLICATION_TIME_SETTING.getKey(), TimeValue.timeValueSeconds(5))
        .build();

    public void testIsSegrepLimitBreached() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primaryShard = shards.getPrimary();
            SegmentReplicationPressureService service = buildPressureService(settings, primaryShard);

            indexInBatches(5, shards, primaryShard);

            SegmentReplicationStats segmentReplicationStats = service.nodeStats();
            Map<ShardId, SegmentReplicationPerGroupStats> shardStats = segmentReplicationStats.getShardStats();
            assertEquals(1, shardStats.size());
            SegmentReplicationPerGroupStats groupStats = shardStats.get(primaryShard.shardId());
            assertEquals(0, groupStats.getRejectedRequestCount());
            Set<SegmentReplicationShardStats> replicas = groupStats.getReplicaStats();
            assertEquals(1, replicas.size());
            SegmentReplicationShardStats replicaStats = replicas.stream().findFirst().get();
            assertEquals(5, replicaStats.getCheckpointsBehindCount());

            assertBusy(
                () -> expectThrows(OpenSearchRejectedExecutionException.class, () -> service.isSegrepLimitBreached(primaryShard.shardId())),
                30,
                TimeUnit.SECONDS
            );
            assertBusy(
                () -> expectThrows(OpenSearchRejectedExecutionException.class, () -> service.isSegrepLimitBreached(primaryShard.shardId())),
                30,
                TimeUnit.SECONDS
            );

            // let shard catch up
            replicateSegments(primaryShard, shards.getReplicas());

            segmentReplicationStats = service.nodeStats();
            shardStats = segmentReplicationStats.getShardStats();
            assertEquals(1, shardStats.size());
            groupStats = shardStats.get(primaryShard.shardId());
            assertEquals(2, groupStats.getRejectedRequestCount());
            replicas = groupStats.getReplicaStats();
            assertEquals(1, replicas.size());
            replicaStats = replicas.stream().findFirst().get();
            assertEquals(0, replicaStats.getCheckpointsBehindCount());

            service.isSegrepLimitBreached(primaryShard.shardId());
        }
    }

    public void testIsSegrepLimitBreached_onlyCheckpointLimitBreached() throws Exception {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .build();

        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primaryShard = shards.getPrimary();
            SegmentReplicationPressureService service = buildPressureService(settings, primaryShard);

            indexInBatches(5, shards, primaryShard);

            Set<SegmentReplicationShardStats> replicationStats = primaryShard.getReplicationStats();
            assertEquals(1, replicationStats.size());
            SegmentReplicationShardStats shardStats = replicationStats.stream().findFirst().get();
            assertEquals(5, shardStats.getCheckpointsBehindCount());

            service.isSegrepLimitBreached(primaryShard.shardId());

            replicateSegments(primaryShard, shards.getReplicas());
            service.isSegrepLimitBreached(primaryShard.shardId());
            final SegmentReplicationStats segmentReplicationStats = service.nodeStats();
            assertEquals(0, segmentReplicationStats.getShardStats().get(primaryShard.shardId()).getRejectedRequestCount());
        }
    }

    public void testIsSegrepLimitBreached_onlyTimeLimitBreached() throws Exception {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .build();

        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primaryShard = shards.getPrimary();
            final SegmentReplicationPressureService service = buildPressureService(settings, primaryShard);

            indexInBatches(1, shards, primaryShard);

            assertBusy(() -> {
                Set<SegmentReplicationShardStats> replicationStats = primaryShard.getReplicationStats();
                assertEquals(1, replicationStats.size());
                SegmentReplicationShardStats shardStats = replicationStats.stream().findFirst().get();
                assertTrue(shardStats.getCurrentReplicationTimeMillis() > TimeValue.timeValueSeconds(5).millis());
            });

            service.isSegrepLimitBreached(primaryShard.shardId());
            replicateSegments(primaryShard, shards.getReplicas());
            service.isSegrepLimitBreached(primaryShard.shardId());
            final SegmentReplicationStats segmentReplicationStats = service.nodeStats();
            assertEquals(0, segmentReplicationStats.getShardStats().get(primaryShard.shardId()).getRejectedRequestCount());
        }
    }

    public void testIsSegrepLimitBreached_underStaleNodeLimit() throws Exception {
        try (ReplicationGroup shards = createGroup(3, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primaryShard = shards.getPrimary();
            indexInBatches(5, shards, primaryShard);
            SegmentReplicationPressureService service = buildPressureService(settings, primaryShard);

            assertBusy(() -> {
                Set<SegmentReplicationShardStats> replicationStats = primaryShard.getReplicationStats();
                assertEquals(3, replicationStats.size());
                SegmentReplicationShardStats shardStats = replicationStats.stream().findFirst().get();
                assertTrue(shardStats.getCurrentReplicationTimeMillis() > TimeValue.timeValueSeconds(5).millis());
            });

            expectThrows(OpenSearchRejectedExecutionException.class, () -> service.isSegrepLimitBreached(primaryShard.shardId()));

            SegmentReplicationStats segmentReplicationStats = service.nodeStats();
            assertEquals(1, segmentReplicationStats.getShardStats().get(primaryShard.shardId()).getRejectedRequestCount());

            // update one replica. 2/3 stale.
            final List<IndexShard> replicas = shards.getReplicas();
            replicateSegments(primaryShard, asList(replicas.get(0)));

            expectThrows(OpenSearchRejectedExecutionException.class, () -> service.isSegrepLimitBreached(primaryShard.shardId()));

            segmentReplicationStats = service.nodeStats();
            assertEquals(2, segmentReplicationStats.getShardStats().get(primaryShard.shardId()).getRejectedRequestCount());

            // update second replica - 1/3 stale - should not throw.
            replicateSegments(primaryShard, asList(replicas.get(1)));
            service.isSegrepLimitBreached(primaryShard.shardId());

            // catch up all.
            replicateSegments(primaryShard, shards.getReplicas());
            service.isSegrepLimitBreached(primaryShard.shardId());
        }
    }

    public void testFailStaleReplicaTask() throws Exception {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(MAX_REPLICATION_TIME_SETTING.getKey(), TimeValue.timeValueMillis(10))
            .build();

        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primaryShard = shards.getPrimary();
            SegmentReplicationPressureService service = buildPressureService(settings, primaryShard);

            // index docs in batches without refreshing
            indexInBatches(5, shards, primaryShard);

            // assert that replica shard is few checkpoints behind primary
            Set<SegmentReplicationShardStats> replicationStats = primaryShard.getReplicationStats();
            assertEquals(1, replicationStats.size());
            SegmentReplicationShardStats shardStats = replicationStats.stream().findFirst().get();
            assertEquals(5, shardStats.getCheckpointsBehindCount());

            // call the background task
            service.getFailStaleReplicaTask().runInternal();

            // verify that remote shard failed method is called which fails the replica shards falling behind.
            verify(shardStateAction, times(1)).remoteShardFailed(any(), anyString(), anyLong(), anyBoolean(), anyString(), any(), any());
            replicateSegments(primaryShard, shards.getReplicas());
        }
    }

    private int indexInBatches(int count, ReplicationGroup shards, IndexShard primaryShard) throws Exception {
        int totalDocs = 0;
        for (int i = 0; i < count; i++) {
            int numDocs = randomIntBetween(100, 200);
            totalDocs += numDocs;
            shards.indexDocs(numDocs);
            primaryShard.refresh("Test");
        }
        return totalDocs;
    }

    private SegmentReplicationPressureService buildPressureService(Settings settings, IndexShard primaryShard) {
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        when(indicesService.iterator()).thenAnswer((Answer<Iterator<IndexService>>) invocation -> List.of(indexService).iterator());
        when(indexService.iterator()).thenAnswer((Answer<Iterator<IndexShard>>) invocation -> List.of(primaryShard).iterator());
        when(indicesService.indexService(primaryShard.shardId().getIndex())).thenReturn(indexService);
        when(indexService.getShard(primaryShard.shardId().id())).thenReturn(primaryShard);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));

        return new SegmentReplicationPressureService(settings, clusterService, indicesService, shardStateAction, mock(ThreadPool.class));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.autoforcemerge;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.node.Node;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.common.util.concurrent.OpenSearchExecutors.NODE_PROCESSORS_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class AutoForceMergeManagerIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME_1 = "test-auto-forcemerge-one";
    private static final String INDEX_NAME_2 = "test-auto-forcemerge-two";
    private static final int NUM_DOCS_IN_BULK = 1000;
    private static final int INGESTION_COUNT = 3;
    private static final String SCHEDULER_INTERVAL = "1s";
    private static final String TRANSLOG_AGE = "1s";
    private static final String MERGE_DELAY = "1s";
    private static final Integer SEGMENT_COUNT = 1;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ByteSizeValue cacheSize = new ByteSizeValue(16, ByteSizeUnit.GB);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .put(NODE_PROCESSORS_SETTING.getKey(), 32)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), SCHEDULER_INTERVAL)
            .put(ForceMergeManagerSettings.TRANSLOG_AGE_AUTO_FORCE_MERGE.getKey(), TRANSLOG_AGE)
            .put(ForceMergeManagerSettings.SEGMENT_COUNT_FOR_AUTO_FORCE_MERGE.getKey(), SEGMENT_COUNT)
            .put(ForceMergeManagerSettings.MERGE_DELAY_BETWEEN_SHARDS_FOR_AUTO_FORCE_MERGE.getKey(), MERGE_DELAY)
            .build();
    }

    @SuppressForbidden(reason = "Sleeping longer than scheduled interval")
    public void testAutoForceMergeFeatureFlagDisabled() throws InterruptedException, ExecutionException {

        Settings clusterSettings = Settings.builder()
            .put(super.nodeSettings(0))
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SETTING.getKey(), false)
            .build();
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode(clusterSettings);
        String dataNode = internalTestCluster.startDataOnlyNodes(1, clusterSettings).getFirst();
        internalCluster().startWarmOnlyNodes(1, clusterSettings).getFirst();

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME_1).setSettings(settings).get());

        // Each ingestion request creates a segment here
        for (int i = 0; i < INGESTION_COUNT; i++) {
            indexBulk(INDEX_NAME_1, NUM_DOCS_IN_BULK);
            flushAndRefresh(INDEX_NAME_1);
        }
        IndexShard shard = getIndexShard(dataNode, INDEX_NAME_1);
        assertNotNull(shard);

        // Before stats
        SegmentsStats segmentsStatsBefore = shard.segmentStats(false, false);

        // This is to make sure auto force merge action gets triggered multiple times ang gets successful at least once.
        Thread.sleep(TimeValue.parseTimeValue(SCHEDULER_INTERVAL, "test").getMillis() * 3);
        // refresh to clear old segments
        flushAndRefresh(INDEX_NAME_1);

        // After stats
        SegmentsStats segmentsStatsAfter = shard.segmentStats(false, false);
        assertEquals(segmentsStatsBefore.getCount(), segmentsStatsAfter.getCount());

        // Deleting the index (so that ref count drops to zero for all the files) and then pruning the cache to clear it to avoid any file
        // leaks
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME_1).get());
    }

    @SuppressForbidden(reason = "Sleeping longer than scheduled interval")
    public void testAutoForceMergeTriggeringWithOneShardOfNonWarmCandidate() throws Exception {
        Settings clusterSettings = Settings.builder()
            .put(super.nodeSettings(0))
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SETTING.getKey(), true)
            .build();
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode(clusterSettings);
        String dataNode = internalTestCluster.startDataOnlyNodes(1, clusterSettings).getFirst();
        internalCluster().startWarmOnlyNodes(1, clusterSettings).getFirst();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_AUTO_FORCE_MERGES_ENABLED.getKey(), false)
            .build();
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME_1).setSettings(settings).get());
        for (int i = 0; i < INGESTION_COUNT; i++) {
            indexBulk(INDEX_NAME_1, NUM_DOCS_IN_BULK);
            flushAndRefresh(INDEX_NAME_1);
        }
        IndexShard shard = getIndexShard(dataNode, INDEX_NAME_1);
        assertNotNull(shard);
        SegmentsStats segmentsStatsBefore = shard.segmentStats(false, false);
        Thread.sleep(TimeValue.parseTimeValue(SCHEDULER_INTERVAL, "test").getMillis() * 3);
        SegmentsStats segmentsStatsAfter = shard.segmentStats(false, false);
        assertEquals(segmentsStatsBefore.getCount(), segmentsStatsAfter.getCount());
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME_1).get());
    }

    public void testAutoForceMergeTriggeringBasicWithOneShard() throws Exception {
        Settings clusterSettings = Settings.builder()
            .put(super.nodeSettings(0))
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SETTING.getKey(), true)
            .build();
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode(clusterSettings);
        String dataNode = internalTestCluster.startDataOnlyNodes(1, clusterSettings).getFirst();
        internalCluster().startWarmOnlyNodes(1, clusterSettings).getFirst();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME_1).setSettings(settings).get());
        for (int i = 0; i < INGESTION_COUNT; i++) {
            indexBulk(INDEX_NAME_1, NUM_DOCS_IN_BULK);
            flushAndRefresh(INDEX_NAME_1);
        }
        IndexShard shard = getIndexShard(dataNode, INDEX_NAME_1);
        assertNotNull(shard);
        SegmentsStats segmentsStatsBefore = shard.segmentStats(false, false);
        waitUntil(() -> shard.segmentStats(false, false).getCount() == SEGMENT_COUNT, 1, TimeUnit.MINUTES);
        SegmentsStats segmentsStatsAfter = shard.segmentStats(false, false);
        assertTrue((int) segmentsStatsBefore.getCount() > segmentsStatsAfter.getCount());
        assertEquals((int) SEGMENT_COUNT, segmentsStatsAfter.getCount());
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME_1).get());
    }

    public void testAutoForceMergeTriggeringBasicWithFiveShardsOfTwoIndex() throws Exception {

        Settings clusterSettings = Settings.builder()
            .put(super.nodeSettings(0))
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SETTING.getKey(), true)
            .build();
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode(clusterSettings);
        String dataNode = internalTestCluster.startDataOnlyNodes(1, clusterSettings).getFirst();
        internalCluster().startWarmOnlyNodes(1, clusterSettings).getFirst();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME_1)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .get()
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME_2)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .get()
        );
        for (int i = 0; i < INGESTION_COUNT; i++) {
            indexBulk(INDEX_NAME_1, NUM_DOCS_IN_BULK);
            flushAndRefresh(INDEX_NAME_1);
        }
        IndexShard shard1 = getIndexShardFromShardId(dataNode, INDEX_NAME_1, 0);
        IndexShard shard2 = getIndexShardFromShardId(dataNode, INDEX_NAME_1, 1);
        IndexShard shard3 = getIndexShardFromShardId(dataNode, INDEX_NAME_1, 2);
        assertNotNull(shard1);
        assertNotNull(shard2);
        assertNotNull(shard3);
        for (int i = 0; i < INGESTION_COUNT; i++) {
            indexBulk(INDEX_NAME_2, NUM_DOCS_IN_BULK);
            flushAndRefresh(INDEX_NAME_2);
        }
        IndexShard shard4 = getIndexShardFromShardId(dataNode, INDEX_NAME_2, 0);
        IndexShard shard5 = getIndexShardFromShardId(dataNode, INDEX_NAME_2, 1);
        assertNotNull(shard4);
        assertNotNull(shard5);

        SegmentsStats segmentsStatsForShard1Before = shard1.segmentStats(false, false);
        SegmentsStats segmentsStatsForShard2Before = shard2.segmentStats(false, false);
        SegmentsStats segmentsStatsForShard3Before = shard3.segmentStats(false, false);
        SegmentsStats segmentsStatsForShard4Before = shard4.segmentStats(false, false);
        SegmentsStats segmentsStatsForShard5Before = shard5.segmentStats(false, false);
        AtomicLong totalSegmentsBefore = new AtomicLong(
            segmentsStatsForShard1Before.getCount() + segmentsStatsForShard2Before.getCount() + segmentsStatsForShard3Before.getCount()
                + segmentsStatsForShard4Before.getCount() + segmentsStatsForShard5Before.getCount()
        );
        assertTrue(totalSegmentsBefore.get() > 5);
        waitUntil(() -> shard1.segmentStats(false, false).getCount() == SEGMENT_COUNT, 1, TimeUnit.MINUTES);
        waitUntil(() -> shard2.segmentStats(false, false).getCount() == SEGMENT_COUNT, 1, TimeUnit.MINUTES);
        waitUntil(() -> shard3.segmentStats(false, false).getCount() == SEGMENT_COUNT, 1, TimeUnit.MINUTES);
        waitUntil(() -> shard4.segmentStats(false, false).getCount() == SEGMENT_COUNT, 1, TimeUnit.MINUTES);
        waitUntil(() -> shard5.segmentStats(false, false).getCount() == SEGMENT_COUNT, 1, TimeUnit.MINUTES);
        SegmentsStats segmentsStatsForShard1After = shard1.segmentStats(false, false);
        SegmentsStats segmentsStatsForShard2After = shard2.segmentStats(false, false);
        SegmentsStats segmentsStatsForShard3After = shard3.segmentStats(false, false);
        SegmentsStats segmentsStatsForShard4After = shard4.segmentStats(false, false);
        SegmentsStats segmentsStatsForShard5After = shard5.segmentStats(false, false);
        AtomicLong totalSegmentsAfter = new AtomicLong(
            segmentsStatsForShard1After.getCount() + segmentsStatsForShard2After.getCount() + segmentsStatsForShard3After.getCount()
                + segmentsStatsForShard4After.getCount() + segmentsStatsForShard5After.getCount()
        );
        assertTrue(totalSegmentsBefore.get() > totalSegmentsAfter.get());
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME_1).get());
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME_2).get());
    }
}

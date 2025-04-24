/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autoforcemerge;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.node.AutoForceMergeManager;
import org.opensearch.node.Node;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.InternalTestCluster;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.*;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class AutoForceMergeManagerIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "test-auto-forcemerge";
    private static final int NUM_DOCS_IN_BULK = 1000;
    private static final int INGESTION_COUNT = 3;
    private static final String SCHEDULER_FREQUENCY = "3s";
    private static final Integer SEGMENT_COUNT = 1;

    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ByteSizeValue cacheSize = new ByteSizeValue(16, ByteSizeUnit.GB);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("cluster.remote_store.state.enabled", true)
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .put(AutoForceMergeManager.AUTO_FORCE_MERGE_SCHEDULER_FREQUENCY.getKey(), SCHEDULER_FREQUENCY)
            .put(AutoForceMergeManager.WAIT_BETWEEN_AUTO_FORCE_MERGE_SHARDS.getKey(), "1s")
            .put(AutoForceMergeManager.CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 80.0)
            .put(AutoForceMergeManager.JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 70.0)
            .put(AutoForceMergeManager.FORCE_MERGE_THREADS_THRESHOLD_COUNT_FOR_AUTO_FORCE_MERGE.getKey(), 1)
            .put(AutoForceMergeManager.SEGMENT_COUNT_THRESHOLD_FOR_AUTO_FORCE_MERGE.getKey(), SEGMENT_COUNT)
            .build();
    }

    public void testAutoForceMergeTriggeringBasic() throws Exception {
        // Cluster setup
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode();
        String dataNode = internalTestCluster.startDataOnlyNodes(1).getFirst();
        internalCluster().startWarmOnlyNodes(1).getFirst();

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get());

        // Each ingestion request creates a segment here
        for (int i = 0; i < INGESTION_COUNT; i++) {
            indexBulk(INDEX_NAME, NUM_DOCS_IN_BULK);
            flushAndRefresh(INDEX_NAME);
        }
        IndexShard shard = getIndexShard(dataNode, INDEX_NAME);
        assertNotNull(shard);

        // Before stats
        SegmentsStats segmentsStatsBefore = shard.segmentStats(false, false);
        assertTrue(segmentsStatsBefore.getCount() > SEGMENT_COUNT);

        // This is to make sure auto force merge action gets triggered multiple times ang gets successful at least once.
        Thread.sleep(TimeValue.parseTimeValue(SCHEDULER_FREQUENCY, "test").getMillis() * 3);
        // refresh to clear old segments
        flushAndRefresh(INDEX_NAME);

        // After stats
        SegmentsStats segmentsStatsAfter = shard.segmentStats(false, false);
        assertEquals((int) SEGMENT_COUNT, segmentsStatsAfter.getCount());

        // Deleting the index (so that ref count drops to zero for all the files) and then pruning the cache to clear it to avoid any file
        // leaks
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME).get());
    }
}


/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchReplicaReplicationIT extends SegmentReplicationBaseIT {

    private static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected Path absolutePath;

    private Boolean useRemoteStore;

    @Before
    public void randomizeRemoteStoreEnabled() {
        useRemoteStore = randomBoolean();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (useRemoteStore) {
            if (absolutePath == null) {
                absolutePath = randomRepoPath().toAbsolutePath();
            }
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(remoteStoreClusterSettings(REPOSITORY_NAME, absolutePath))
                .build();
        }
        return super.nodeSettings(nodeOrdinal);
    }

    @After
    public void teardown() {
        if (useRemoteStore) {
            clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
        }
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, true).build();
    }

    public void testReplication() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final int docCount = 10;
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        waitForSearchableDocs(docCount, primary, replica);
    }

    public void testSegmentReplicationStatsResponseWithSearchReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final List<String> nodes = internalCluster().startDataOnlyNodes(2);
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .put("number_of_search_only_replicas", 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureGreen(INDEX_NAME);

        final int docCount = 5;
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        waitForSearchableDocs(docCount, nodes);

        SegmentReplicationStatsResponse segmentReplicationStatsResponse = dataNodeClient().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .setDetailed(true)
            .execute()
            .actionGet();

        // Verify the number of indices
        assertEquals(1, segmentReplicationStatsResponse.getReplicationStats().size());
        // Verify total shards
        assertEquals(2, segmentReplicationStatsResponse.getTotalShards());
        // Verify the number of primary shards
        assertEquals(1, segmentReplicationStatsResponse.getReplicationStats().get(INDEX_NAME).size());

        SegmentReplicationPerGroupStats perGroupStats = segmentReplicationStatsResponse.getReplicationStats().get(INDEX_NAME).get(0);
        Set<SegmentReplicationShardStats> replicaStats = perGroupStats.getReplicaStats();
        // Verify the number of replica stats
        assertEquals(1, replicaStats.size());
        for (SegmentReplicationShardStats replicaStat : replicaStats) {
            assertNotNull(replicaStat.getCurrentReplicationState());
        }
    }
}

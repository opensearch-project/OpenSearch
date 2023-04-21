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
import org.opensearch.index.IndexModule;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.Arrays;

import static org.opensearch.indices.IndicesService.CLUSTER_SETTING_REPLICATION_TYPE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationClusterSettingIT extends OpenSearchIntegTestCase {

    protected static final String INDEX_NAME = "test-idx-1";
    private static final String SYSTEM_INDEX_NAME = ".test-system-index";
    protected static final int SHARD_COUNT = 1;
    protected static final int REPLICA_COUNT = 1;

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true").build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(CLUSTER_SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(
                new SystemIndexDescriptor(SYSTEM_INDEX_NAME, "System index for [" + getTestClass().getName() + ']')
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(SegmentReplicationClusterSettingIT.TestPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testReplicationWithSegmentReplicationClusterSetting() throws Exception {

        boolean isSystemIndex = randomBoolean();
        String indexName = isSystemIndex ? SYSTEM_INDEX_NAME : INDEX_NAME;

        // Starting two nodes with primary and replica shards respectively.
        final String primaryNode = internalCluster().startNode();
        createIndex(indexName);
        ensureYellowAndNoInitializingShards(indexName);
        final String replicaNode = internalCluster().startNode();
        ensureGreen(indexName);

        final int initialDocCount = scaledRandomIntBetween(20, 30);
        for (int i = 0; i < initialDocCount; i++) {
            client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }

        refresh(indexName);
        assertBusy(() -> {
            assertHitCount(client(replicaNode).prepareSearch(indexName).setSize(0).setPreference("_only_local").get(), initialDocCount);
        });

        SegmentReplicationStatsResponse segmentReplicationStatsResponse = client().admin()
            .indices()
            .prepareSegmentReplicationStats(indexName)
            .execute()
            .actionGet();
        if (isSystemIndex) {
            // Verify that Segment Replication did not happen on the replica shard.
            assertNull(segmentReplicationStatsResponse.getReplicationStats().get(indexName));
        } else {
            // Verify that Segment Replication happened on the replica shard.
            assertFalse(segmentReplicationStatsResponse.getReplicationStats().get(indexName).get(0).getReplicaStats().isEmpty());
        }
    }

    public void testIndexReplicationSettingOverridesClusterSetting() throws Exception {
        // Starting two nodes with primary and replica shards respectively.
        final String primaryNode = internalCluster().startNode();
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                // we want to override cluster replication setting by passing a index replication setting
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
        ).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(20, 30);
        for (int i = 0; i < initialDocCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }

        refresh(INDEX_NAME);
        assertBusy(() -> {
            assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
        });

        SegmentReplicationStatsResponse segmentReplicationStatsResponse = client().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .execute()
            .actionGet();
        // Verify that Segment Replication did not happen on the replica shard.
        assertNull(segmentReplicationStatsResponse.getReplicationStats().get(INDEX_NAME));
    }
}

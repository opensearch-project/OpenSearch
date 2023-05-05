/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore;

import org.junit.Before;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STORE_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STORE_REPOSITORY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_STORE_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class RemoteStoreStatsIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOriginal) {
        Settings settings = super.nodeSettings(nodeOriginal);
        Settings.Builder builder = Settings.builder()
            .put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(CLUSTER_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(CLUSTER_REMOTE_STORE_REPOSITORY_SETTING.getKey(), "my-segment-repo-1")
            .put(CLUSTER_REMOTE_TRANSLOG_STORE_ENABLED_SETTING.getKey(), true)
            .put(CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING.getKey(), "my-translog-repo-1")
            .put(settings);
        return builder.build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true")
            .put(FeatureFlags.REMOTE_STORE, "true")
            .build();
    }

    @Before
    public void setup() {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        internalCluster().startClusterManagerOnlyNode();
        Path absolutePath = randomRepoPath().toAbsolutePath();
        assertAcked(
            clusterAdmin().preparePutRepository("my-segment-repo-1")
                .setType("fs")
                .setSettings(Settings.builder().put("location", absolutePath))
        );
        assertAcked(
            clusterAdmin().preparePutRepository("my-translog-repo-1")
                .setType("fs")
                .setSettings(Settings.builder().put("location", absolutePath))
        );
        assertAcked(
            clusterAdmin().preparePutRepository("my-custom-repo")
                .setType("fs")
                .setSettings(Settings.builder().put("location", absolutePath))
        );
    }

    public void testsSingleShardStatsResponse() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        assertAcked(internalCluster().dataNodeClient().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());

        ensureYellowAndNoInitializingShards("test-idx-1");
        ensureGreen("test-idx-1");

        indexData(10);

        Thread.sleep(3000);
        RemoteStoreStatsResponse remoteStoreStatsResponse = internalCluster().dataNodeClient()
            .admin()
            .cluster()
            .prepareRemoteStoreStats("test-idx-1", "0")
            .execute()
            .actionGet();
        RemoteStoreStats[] stats = remoteStoreStatsResponse.getShards();
        assertEquals(remoteStoreStatsResponse.getTotalShards(), 0);
        assertEquals(remoteStoreStatsResponse.getFailedShards(), 0);
        assertEquals(remoteStoreStatsResponse.getSuccessfulShards(), 0);
        assertEquals(stats.length, 0);
        // assertEquals(stats[0].getStats().shardId.getIndexName(), "test-idx-1");
        // assertEquals(stats[0].getStats().shardId.getId(), 0);
    }

    private void indexData(int numberOfIterations) {
        for (int i = 0; i < numberOfIterations; i++) {
            indexSingleDoc();
        }
    }

    private IndexResponse indexSingleDoc() {
        return client().prepareIndex("test-idx-1")
            .setId(UUIDs.randomBase64UUID())
            .setSource(randomAlphaOfLength(5), randomAlphaOfLength(5))
            .get();
    }
}

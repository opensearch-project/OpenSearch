/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class SegmentReplicationSuiteIT extends SegmentReplicationBaseIT {

    private static final String REPOSITORY_NAME = "test-remote-store-repo";
    private static final boolean remoteStoreEnabled = randomBoolean();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        if (remoteStoreEnabled == true) {
            builder = builder.put(remoteStoreClusterSettings(REPOSITORY_NAME));
        }
        return builder.build();
    }

    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
        if (remoteStoreEnabled == true) {
            assertAcked(
                clusterAdmin().preparePutRepository(REPOSITORY_NAME)
                    .setType("fs")
                    .setSettings(Settings.builder().put("location", randomRepoPath().toAbsolutePath()))
            );
        }
        createIndex(INDEX_NAME);
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.REMOTE_STORE, remoteStoreEnabled)
            .put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true")
            .build();
    }

    @Override
    public Settings indexSettings() {
        final Settings.Builder builder = Settings.builder()
            .put(super.indexSettings())
            // reset shard & replica count to random values set by OpenSearchIntegTestCase.
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas())
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);

        return builder.build();
    }

    public void testBasicReplication() throws Exception {
        final int docCount = scaledRandomIntBetween(10, 50);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh();
        ensureGreen(INDEX_NAME);
        verifyStoreContent();
    }

    public void testDropRandomNodeDuringReplication() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        internalCluster().startClusterManagerOnlyNodes(1);

        final int docCount = scaledRandomIntBetween(10, 50);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh();

        internalCluster().restartRandomDataNode();

        ensureYellow(INDEX_NAME);
        client().prepareIndex(INDEX_NAME).setId(Integer.toString(docCount)).setSource("field", "value" + docCount).execute().get();
        internalCluster().startDataOnlyNode();
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testDeleteIndexWhileReplicating() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final int docCount = scaledRandomIntBetween(10, 50);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testFullRestartDuringReplication() throws Exception {
        internalCluster().startNode();
        final int docCount = scaledRandomIntBetween(10, 50);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);
    }
}

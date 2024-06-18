/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.util.concurrent.TimeUnit;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoteReplicaRecoveryIT extends MigrationBaseTestCase {

    protected int maximumNumberOfShards() {
        return 1;
    }

    protected int maximumNumberOfReplicas() {
        return 1;
    }

    protected int minimumNumberOfReplicas() {
        return 1;
    }

    /*
    Brings up new replica copies on remote and docrep nodes, when primary is on a remote node
    Live indexing is happening meanwhile
    */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/13473")
    public void testReplicaRecovery() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        String primaryNode = internalCluster().startNode();
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // create shard with 0 replica and 1 shard
        client().admin().indices().prepareCreate("test").setSettings(indexSettings()).setMapping("field", "type=text").get();
        String replicaNode = internalCluster().startNode();
        ensureGreen("test");
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService("test");
        asyncIndexingService.startIndexing();

        refresh("test");

        // add remote node in mixed mode cluster
        setAddRemote(true);
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        internalCluster().startNode();
        internalCluster().validateClusterFormed();

        // identify the primary
        logger.info("-->  relocating primary from {} to {} ", primaryNode, remoteNode);
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand("test", 0, primaryNode, remoteNode))
            .execute()
            .actionGet();

        waitForRelocation();
        logger.info("-->  relocation of primary from docrep to remote  complete");

        logger.info("--> getting up the new replicas now to doc rep node as well as remote node ");
        // Increase replica count to 3
        client().admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest("test").settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 3)
                        .put("index.routing.allocation.exclude._name", remoteNode)
                        .build()
                )
            )
            .get();

        waitForRelocation();
        asyncIndexingService.stopIndexing();
        refresh("test");

        // segrep lag should be zero
        assertBusy(() -> {
            SegmentReplicationStatsResponse segmentReplicationStatsResponse = dataNodeClient().admin()
                .indices()
                .prepareSegmentReplicationStats("test")
                .setDetailed(true)
                .execute()
                .actionGet();
            SegmentReplicationPerGroupStats perGroupStats = segmentReplicationStatsResponse.getReplicationStats().get("test").get(0);
            assertEquals(segmentReplicationStatsResponse.getReplicationStats().size(), 1);
            perGroupStats.getReplicaStats().stream().forEach(e -> assertEquals(e.getCurrentReplicationLagMillis(), 0));
        }, 20, TimeUnit.SECONDS);

        OpenSearchAssertions.assertHitCount(
            client().prepareSearch("test").setTrackTotalHits(true).get(),
            asyncIndexingService.getIndexedDocs()
        );
        OpenSearchAssertions.assertHitCount(
            client().prepareSearch("test")
                .setTrackTotalHits(true)// extra paranoia ;)
                .setQuery(QueryBuilders.termQuery("auto", true))
                .get(),
            asyncIndexingService.getIndexedDocs()
        );

    }
}

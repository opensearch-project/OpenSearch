/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

        AtomicInteger numAutoGenDocs = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);
        Thread indexingThread = getThread(finished, numAutoGenDocs);

        refresh("test");

        // add remote node in mixed mode cluster
        setAddRemote(true);
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        String remoteNode2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        // identify the primary

        Thread.sleep(RandomNumbers.randomIntBetween(random(), 0, 2000));
        logger.info("-->  relocating primary from {} to {} ", primaryNode, remoteNode);
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand("test", 0, primaryNode, remoteNode))
            .execute()
            .actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(60))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();

        assertEquals(0, clusterHealthResponse.getRelocatingShards());
        logger.info("-->  relocation of primary from docrep to remote  complete");
        Thread.sleep(RandomNumbers.randomIntBetween(random(), 0, 2000));

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

        client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(60))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .execute()
            .actionGet();
        logger.info("-->  replica  is up now on another docrep now as well as remote node");

        assertEquals(0, clusterHealthResponse.getRelocatingShards());

        Thread.sleep(RandomNumbers.randomIntBetween(random(), 0, 2000));

        // Stop replicas on docrep now.
        // ToDo : Remove once we have dual replication enabled
        client().admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest("test").settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put("index.routing.allocation.exclude._name", primaryNode + "," + replicaNode)
                        .build()
                )
            )
            .get();

        finished.set(true);
        indexingThread.join();
        refresh("test");
        OpenSearchAssertions.assertHitCount(client().prepareSearch("test").setTrackTotalHits(true).get(), numAutoGenDocs.get());
        OpenSearchAssertions.assertHitCount(
            client().prepareSearch("test")
                .setTrackTotalHits(true)// extra paranoia ;)
                .setQuery(QueryBuilders.termQuery("auto", true))
                // .setPreference("_prefer_nodes:" + (remoteNode+ "," + remoteNode2))
                .get(),
            numAutoGenDocs.get()
        );

    }

    private Thread getThread(AtomicBoolean finished, AtomicInteger numAutoGenDocs) {
        Thread indexingThread = new Thread(() -> {
            while (finished.get() == false && numAutoGenDocs.get() < 100) {
                IndexResponse indexResponse = client().prepareIndex("test").setId("id").setSource("field", "value").get();
                assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
                DeleteResponse deleteResponse = client().prepareDelete("test", "id").get();
                assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
                client().prepareIndex("test").setSource("auto", true).get();
                numAutoGenDocs.incrementAndGet();
                logger.info("Indexed {} docs here", numAutoGenDocs.get());
            }
        });
        indexingThread.start();
        return indexingThread;
    }

}

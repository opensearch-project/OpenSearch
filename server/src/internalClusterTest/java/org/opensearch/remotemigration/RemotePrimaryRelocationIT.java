/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemotePrimaryRelocationIT extends MigrationBaseTestCase {

    private final String INDEX_NAME = "primary-relocation";

    protected int maximumNumberOfShards() {
        return 1;
    }

    protected int maximumNumberOfReplicas() {
        return 0;
    }

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return asList(MockTransportService.TestPlugin.class);
    }

    public void testRemotePrimaryRelocation() throws Exception {
        List<String> docRepNodes = internalCluster().startNodes(2);
        Client client = internalCluster().client(docRepNodes.get(0));
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // create shard with 1 replica and 1 shard
        Settings settings = super.indexSettings();
        settings = Settings.builder().put(settings).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).setMapping("field", "type=text").get();
        ensureGreen(INDEX_NAME);

        AtomicInteger numAutoGenDocs = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);
        AsyncIndexingService asyncIndexingService = new AsyncIndexingService(INDEX_NAME);
        asyncIndexingService.startIndexing();
        refresh(INDEX_NAME);

        // add remote node in mixed mode cluster
        setAddRemote(true);
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        String remoteNode2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        // assert repo gets registered
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { REPOSITORY_NAME });
        GetRepositoriesResponse getRepositoriesResponse = client.admin().cluster().getRepositories(gr).actionGet();
        assertEquals(1, getRepositoriesResponse.repositories().size());

        // Index some more docs
        int currentDoc = numAutoGenDocs.get();
        int finalCurrentDoc1 = currentDoc;
        waitUntil(() -> numAutoGenDocs.get() > finalCurrentDoc1 + 100);

        ByteSizeValue shardSize = client().admin().indices().prepareStats(INDEX_NAME).execute().actionGet().getShards()[0].getStats()
            .getStore()
            .size();
        logger.info("Shard size after migration is {}", shardSize);
        slowDownRecovery(shardSize);

        // Change direction to remote store
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        logger.info("-->  relocating from {} to {} ", docRepNodes, remoteNode);
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, primaryNodeName(INDEX_NAME), remoteNode))
            .execute()
            .actionGet();
        waitForRelocation();
        assertEquals(remoteNode, primaryNodeName(INDEX_NAME));

        // Index some more docs
        currentDoc = numAutoGenDocs.get();
        int finalCurrentDoc = currentDoc;
        waitUntil(() -> numAutoGenDocs.get() > finalCurrentDoc + 5);

        // increase recovery speed a bit to account for current size
        shardSize = client().admin().indices().prepareStats(INDEX_NAME).execute().actionGet().getShards()[0].getStats().getStore().size();
        slowDownRecovery(shardSize);

        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, remoteNode, remoteNode2))
            .execute()
            .actionGet();
        waitForRelocation();
        assertEquals(remoteNode2, primaryNodeName(INDEX_NAME));
        logger.info("-->  relocation from remote to remote  complete");

        finished.set(true);
        asyncIndexingService.stopIndexing();
        refresh(INDEX_NAME);
        OpenSearchAssertions.assertHitCount(
            client().prepareSearch(INDEX_NAME).setTrackTotalHits(true).get(),
            asyncIndexingService.getIndexedDocs()
        );
        OpenSearchAssertions.assertHitCount(
            client().prepareSearch(INDEX_NAME)
                .setTrackTotalHits(true)// extra paranoia ;)
                .setQuery(QueryBuilders.termQuery("auto", true))
                .get(),
            asyncIndexingService.getSingleIndexedDocs()
        );
    }

    public void testMixedModeRelocation_RemoteSeedingFail() throws Exception {
        String docRepNode = internalCluster().startNode();
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // create shard with 0 replica and 1 shard
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping("field", "type=text").get();
        ensureGreen(INDEX_NAME);

        AsyncIndexingService asyncIndexingService = new AsyncIndexingService(INDEX_NAME);
        asyncIndexingService.startIndexing();

        refresh(INDEX_NAME);

        // add remote node in mixed mode cluster
        setAddRemote(true);
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        setFailRate(REPOSITORY_NAME, 100);
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(RecoverySettings.INDICES_INTERNAL_REMOTE_UPLOAD_TIMEOUT.getKey(), "10s"))
            .get();

        // Change direction to remote store
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        logger.info("--> relocating from {} to {} ", docRepNode, remoteNode);
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, docRepNode, remoteNode))
            .execute()
            .actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(5))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();

        assertTrue(clusterHealthResponse.getRelocatingShards() == 1);
        // waiting more than waitForRemoteStoreSync's sleep time of 30 sec to deterministically fail
        Thread.sleep(40000);

        ClusterHealthRequest healthRequest = Requests.clusterHealthRequest()
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(true);
        ClusterHealthResponse actionGet = client().admin().cluster().health(healthRequest).actionGet();
        assertEquals(actionGet.getRelocatingShards(), 0);
        assertEquals(docRepNode, primaryNodeName(INDEX_NAME));

        asyncIndexingService.stopIndexing();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(RecoverySettings.INDICES_INTERNAL_REMOTE_UPLOAD_TIMEOUT.getKey(), (String) null))
            .get();
    }
}

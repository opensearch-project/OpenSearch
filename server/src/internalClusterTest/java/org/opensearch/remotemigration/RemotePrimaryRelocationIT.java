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
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemotePrimaryRelocationIT extends MigrationBaseTestCase {
    protected int maximumNumberOfShards() {
        return 1;
    }

    // ToDo : Fix me when we support migration of replicas
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return asList(MockTransportService.TestPlugin.class);
    }

    public void testMixedModeRelocation() throws Exception {
        String docRepNode = internalCluster().startNode();
        Client client = internalCluster().client(docRepNode);
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // create shard with 0 replica and 1 shard
        client().admin().indices().prepareCreate("test").setSettings(indexSettings()).setMapping("field", "type=text").get();
        ensureGreen("test");

        AtomicInteger numAutoGenDocs = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);
        Thread indexingThread = getIndexingThread(finished, numAutoGenDocs);

        refresh("test");

        // add remote node in mixed mode cluster
        addRemote = true;
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        String remoteNode2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        // assert repo gets registered
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { REPOSITORY_NAME });
        GetRepositoriesResponse getRepositoriesResponse = client.admin().cluster().getRepositories(gr).actionGet();
        assertEquals(1, getRepositoriesResponse.repositories().size());

        // Index some more docs
        int currentDoc = numAutoGenDocs.get();
        int finalCurrentDoc1 = currentDoc;
        waitUntil(() -> numAutoGenDocs.get() > finalCurrentDoc1 + 5);

        logger.info("-->  relocating from {} to {} ", docRepNode, remoteNode);
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, docRepNode, remoteNode)).execute().actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(60))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();

        assertEquals(0, clusterHealthResponse.getRelocatingShards());
        assertEquals(remoteNode, primaryNodeName("test"));
        logger.info("-->  relocation from docrep to remote  complete");

        // Index some more docs
        currentDoc = numAutoGenDocs.get();
        int finalCurrentDoc = currentDoc;
        waitUntil(() -> numAutoGenDocs.get() > finalCurrentDoc + 5);

        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand("test", 0, remoteNode, remoteNode2))
            .execute()
            .actionGet();
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(60))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();

        assertEquals(0, clusterHealthResponse.getRelocatingShards());
        assertEquals(remoteNode2, primaryNodeName("test"));

        logger.info("-->  relocation from remote to remote  complete");

        finished.set(true);
        indexingThread.join();
        refresh("test");
        OpenSearchAssertions.assertHitCount(client().prepareSearch("test").setTrackTotalHits(true).get(), numAutoGenDocs.get());
        OpenSearchAssertions.assertHitCount(
            client().prepareSearch("test")
                .setTrackTotalHits(true)// extra paranoia ;)
                .setQuery(QueryBuilders.termQuery("auto", true))
                .get(),
            numAutoGenDocs.get()
        );

    }

    public void testMixedModeRelocation_RemoteSeedingFail() throws Exception {
        String docRepNode = internalCluster().startNode();
        Client client = internalCluster().client(docRepNode);
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // create shard with 0 replica and 1 shard
        client().admin().indices().prepareCreate("test").setSettings(indexSettings()).setMapping("field", "type=text").get();
        ensureGreen("test");

        AtomicInteger numAutoGenDocs = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);
        Thread indexingThread = getIndexingThread(finished, numAutoGenDocs);

        refresh("test");

        // add remote node in mixed mode cluster
        addRemote = true;
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        // assert repo gets registered
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { REPOSITORY_NAME });
        GetRepositoriesResponse getRepositoriesResponse = client.admin().cluster().getRepositories(gr).actionGet();
        assertEquals(1, getRepositoriesResponse.repositories().size());

        setFailRate(REPOSITORY_NAME, 100);

        logger.info("--> relocating from {} to {} ", docRepNode, remoteNode);
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, docRepNode, remoteNode)).execute().actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(5))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();

        assertTrue(clusterHealthResponse.getRelocatingShards() == 1);
        setFailRate(REPOSITORY_NAME, 0);
        Thread.sleep(RandomNumbers.randomIntBetween(random(), 0, 2000));
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(45))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();
        assertTrue(clusterHealthResponse.getRelocatingShards() == 0);
        logger.info("--> remote to remote relocation complete");
        finished.set(true);
        indexingThread.join();
        refresh("test");
        OpenSearchAssertions.assertHitCount(client().prepareSearch("test").setTrackTotalHits(true).get(), numAutoGenDocs.get());
        OpenSearchAssertions.assertHitCount(
            client().prepareSearch("test")
                .setTrackTotalHits(true)// extra paranoia ;)
                .setQuery(QueryBuilders.termQuery("auto", true))
                .get(),
            numAutoGenDocs.get()
        );
    }

    private static Thread getIndexingThread(AtomicBoolean finished, AtomicInteger numAutoGenDocs) {
        Thread indexingThread = new Thread(() -> {
            while (finished.get() == false && numAutoGenDocs.get() < 10_000) {
                IndexResponse indexResponse = client().prepareIndex("test").setId("id").setSource("field", "value").get();
                assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
                DeleteResponse deleteResponse = client().prepareDelete("test", "id").get();
                assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
                client().prepareIndex("test").setSource("auto", true).get();
                numAutoGenDocs.incrementAndGet();
            }
        });
        indexingThread.start();
        return indexingThread;
    }
}

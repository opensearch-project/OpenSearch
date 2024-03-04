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
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;
import org.opensearch.test.junit.annotations.TestLogging;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemotePrimaryRelocationIT extends MigrationBaseTestCase {
    private static final int RELOCATION_COUNT = 1;

    protected int maximumNumberOfShards() {
        return 1;
    }

    protected int maximumNumberOfReplicas() {
        return 0;
    }

    @TestLogging(reason = "Getting trace logs from replication package", value = "org.opensearch.index.translog.RemoteFsTranslog:TRACE,org.opensearch.index.shard.RemoteStoreRefreshListener:TRACE,"
        + "org.opensearch.indices.recovery:TRACE,"
        + "org.opensearch.index.shard.IndexShard:TRACE")
    public void testMixedModeRelocation() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        List<String> cmNodes = internalCluster().startNodes(1);
        Client client = internalCluster().client(cmNodes.get(0));
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // create shard with 0 replica and 1 shard
        client().admin().indices().prepareCreate("test").setSettings(indexSettings()).setMapping("field", "type=text").get();
        ensureGreen("test");

        AtomicInteger numAutoGenDocs = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);
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

        for (int i = 0; i < RELOCATION_COUNT; i++) {
            Thread.sleep(RandomNumbers.randomIntBetween(random(), 0, 2000));
            logger.info("--> [iteration {}] relocating from {} to {} ", i, cmNodes.get(0), remoteNode);
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand("test", 0, cmNodes.get(0), remoteNode))
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
            logger.info("--> [iteration {}] relocation complete", i);
            Thread.sleep(RandomNumbers.randomIntBetween(random(), 0, 2000));

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
        }
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
}

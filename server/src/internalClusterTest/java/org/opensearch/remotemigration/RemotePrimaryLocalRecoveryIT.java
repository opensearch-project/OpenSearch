/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemotePrimaryLocalRecoveryIT extends MigrationBaseTestCase {
    public void testLocalRecoveryRollingRestart() throws Exception {
        String docRepNode = internalCluster().startNode();
        Client client = internalCluster().client(docRepNode);

        // create shard with 0 replica and 1 shard
        client().admin().indices().prepareCreate("idx1").setSettings(indexSettings()).setMapping("field", "type=text").get();
        ensureGreen("idx1");

        AtomicInteger numAutoGenDocs = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);
        Thread indexingThread = getIndexingThread(finished, numAutoGenDocs);
        refresh("idx1");

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // add remote node in mixed mode cluster
        addRemote = true;
        String remoteNode = internalCluster().startNode();
        internalCluster().validateClusterFormed();

        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // rolling restart
        internalCluster().rollingRestart(new InternalTestCluster.RestartCallback());
        ensureStableCluster(2);
        ensureGreen("idx1");
        assertEquals(internalCluster().size(), 2);

        // Index some more docs
        int currentDoc = numAutoGenDocs.get();
        int finalCurrentDoc = currentDoc;
        waitUntil(() -> numAutoGenDocs.get() > finalCurrentDoc + 5);

        logger.info("-->  relocating from {} to {} ", docRepNode, remoteNode);
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("idx1", 0, docRepNode, remoteNode)).execute().actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(60))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();
        assertEquals(0, clusterHealthResponse.getRelocatingShards());
        assertEquals(remoteNode, primaryNodeName("idx1"));

        OpenSearchAssertions.assertHitCount(client().prepareSearch("idx1").setTrackTotalHits(true).get(), numAutoGenDocs.get());
        OpenSearchAssertions.assertHitCount(
            client().prepareSearch("idx1").setTrackTotalHits(true).setQuery(QueryBuilders.termQuery("auto", true)).get(),
            numAutoGenDocs.get()
        );
    }

    private static Thread getIndexingThread(AtomicBoolean finished, AtomicInteger numAutoGenDocs) {
        Thread indexingThread = new Thread(() -> {
            while (finished.get() == false && numAutoGenDocs.get() < 10_000) {
                IndexResponse indexResponse = client().prepareIndex("idx1").setId("id").setSource("field", "value").get();
                assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
                DeleteResponse deleteResponse = client().prepareDelete("idx1", "id").get();
                assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
                client().prepareIndex("idx1").setSource("auto", true).get();
                numAutoGenDocs.incrementAndGet();
            }
        });
        indexingThread.start();
        return indexingThread;
    }
}

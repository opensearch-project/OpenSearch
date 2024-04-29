/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.List;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteMigrationAllocationDeciderIT extends MigrationBaseTestCase {

    // When the primary is on doc rep node, existing replica copy can get allocated on excluded docrep node.
    public void testFilterAllocationSkipsReplica() throws IOException {
        addRemote = false;
        List<String> docRepNodes = internalCluster().startNodes(3);
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0")
                .build()
        );
        ensureGreen("test");

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        assertTrue(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", String.join(",", docRepNodes)))
                .execute()
                .actionGet()
                .isAcknowledged()
        );
        internalCluster().stopRandomDataNode();
        ensureGreen("test");
    }

    // When the primary is on remote node, new replica copy shouldn't get allocated on an excluded docrep node.
    public void testFilterAllocationSkipsReplicaOnExcludedNode() throws IOException {
        addRemote = false;
        List<String> nodes = internalCluster().startNodes(2);
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0")
                .build()
        );
        ensureGreen("test");
        addRemote = true;

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        String remoteNode = internalCluster().startNode();

        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand("test", 0, primaryNodeName("test"), remoteNode))
            .execute()
            .actionGet();
        client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout(TimeValue.timeValueSeconds(60))
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .execute()
            .actionGet();
        assertEquals(remoteNode, primaryNodeName("test"));

        assertTrue(
            internalCluster().client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", String.join(",", nodes)))
                .execute()
                .actionGet()
                .isAcknowledged()
        );
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName("test")));
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setTimeout(TimeValue.timeValueSeconds(2))
            .execute()
            .actionGet();
        assertTrue(clusterHealthResponse.isTimedOut());
        ensureYellow("test");
    }
}

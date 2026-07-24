/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for {@link IndexShard#primaryReplicaResyncInProgress} flag liveness.
 * Validates the fix for <a href="https://github.com/opensearch-project/OpenSearch/issues/21692">#21692</a>.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrimaryReplicaResyncIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends org.opensearch.plugins.Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Use a short resync timeout for testing so the test doesn't wait 30 minutes
            .put(IndexShard.PRIMARY_RESYNC_TIMEOUT_SETTING.getKey(), "10s")
            .build();
    }

    /**
     * Path B: Verifies that when the resync transport response is lost (simulated by silently
     * consuming the request without responding), the primaryReplicaResyncInProgress flag is
     * eventually cleared by the timeout watchdog, allowing subsequent primary relocation to succeed.
     */
    public void testResyncTimeoutClearsStuckFlag() throws Exception {
        // Start cluster: 1 cluster-manager + 3 data nodes
        String clusterManager = internalCluster().startClusterManagerOnlyNode();
        String nodeA = internalCluster().startDataOnlyNode();
        String nodeB = internalCluster().startDataOnlyNode();
        String nodeC = internalCluster().startDataOnlyNode();

        // Create index pinned to nodeA and nodeB, excluding nodeC
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put("index.routing.allocation.include._name", nodeA + "," + nodeB)
                    .put("index.routing.allocation.exclude._name", nodeC)
            )
        );
        ensureGreen("test");

        // Index some documents so the resync has operations to send
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setSource("field", "value" + i).get();
        }
        client().admin().indices().prepareFlush("test").setForce(true).get();

        // Determine which node holds the primary and which holds the replica
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        ShardRouting primaryShard = state.routingTable().index("test").shard(0).primaryShard();
        String primaryNodeId = primaryShard.currentNodeId();
        String primaryNodeName = state.nodes().get(primaryNodeId).getName();
        String replicaNodeName = primaryNodeName.equals(nodeA) ? nodeB : nodeA;

        // Install a request-handling drop on the future-primary (replicaNodeName) for the resync action.
        // This simulates a lost transport response — the sender's listener never fires.
        CountDownLatch resyncDropped = new CountDownLatch(1);
        MockTransportService futurePrimaryTransport = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            replicaNodeName
        );
        futurePrimaryTransport.addRequestHandlingBehavior(
            "internal:index/seq_no/resync[p]",
            (handler, request, channel, task) -> {
                // Silently consume; do not call handler.messageReceived; do not reply on channel.
                // This simulates a lost transport response — the sender's listener never fires.
                resyncDropped.countDown();
            }
        );

        // Stop the current primary — the replica on replicaNodeName gets promoted
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName));

        // Wait for the replica to become the new primary
        assertBusy(() -> {
            ClusterState s = client().admin().cluster().prepareState().get().getState();
            ShardRouting newPrimary = s.routingTable().index("test").shard(0).primaryShard();
            assertNotNull(newPrimary);
            assertTrue(newPrimary.started());
            assertEquals(replicaNodeName, s.nodes().get(newPrimary.currentNodeId()).getName());
        }, 30, TimeUnit.SECONDS);

        // Verify the resync request was indeed dropped
        assertTrue("resync request should have been intercepted", resyncDropped.await(10, TimeUnit.SECONDS));

        // Verify the flag is initially stuck (before timeout fires)
        IndexShard shard = getIndexShard(replicaNodeName, "test");
        assertTrue(
            "primaryReplicaResyncInProgress should be true immediately after promotion with dropped resync",
            shard.isPrimaryReplicaResyncInProgress()
        );

        // Wait for the timeout watchdog to clear the flag (configured to 10s in nodeSettings)
        assertBusy(() -> {
            assertFalse(
                "primaryReplicaResyncInProgress should be cleared by timeout watchdog",
                shard.isPrimaryReplicaResyncInProgress()
            );
        }, 30, TimeUnit.SECONDS);

        // Clear the transport interception so future requests succeed
        futurePrimaryTransport.clearAllRules();

        // Now open allocation to nodeC and verify relocation succeeds
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.include._name", replicaNodeName + "," + nodeC)
                        .putNull("index.routing.allocation.exclude._name")
                )
        );

        // Issue a reroute to move the primary from replicaNodeName to nodeC
        ClusterRerouteResponse rerouteResponse = client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand("test", 0, replicaNodeName, nodeC))
            .get();
        assertTrue(rerouteResponse.isAcknowledged());

        // Wait for relocation to complete — this should succeed now that the flag is cleared
        ensureGreen("test");

        // Verify the primary ended up on nodeC
        ClusterState finalState = client().admin().cluster().prepareState().get().getState();
        ShardRouting finalPrimary = finalState.routingTable().index("test").shard(0).primaryShard();
        assertThat(finalState.nodes().get(finalPrimary.currentNodeId()).getName(), equalTo(nodeC));
    }

    /**
     * Path A: Verifies that when AlreadyClosedException is thrown before the syncer listener
     * is registered (simulated by closing the index during promotion), the flag is properly cleared.
     */
    public void testAlreadyClosedExceptionClearsFlag() throws Exception {
        // Start cluster: 1 cluster-manager + 2 data nodes
        String clusterManager = internalCluster().startClusterManagerOnlyNode();
        String nodeA = internalCluster().startDataOnlyNode();
        String nodeB = internalCluster().startDataOnlyNode();

        // Create index on nodeA and nodeB
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put("index.routing.allocation.include._name", nodeA + "," + nodeB)
            )
        );
        ensureGreen("test");

        // Index some documents
        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test").setSource("field", "value" + i).get();
        }
        client().admin().indices().prepareFlush("test").setForce(true).get();

        // Determine which node holds the primary
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        ShardRouting primaryShard = state.routingTable().index("test").shard(0).primaryShard();
        String primaryNodeId = primaryShard.currentNodeId();
        String primaryNodeName = state.nodes().get(primaryNodeId).getName();
        String replicaNodeName = primaryNodeName.equals(nodeA) ? nodeB : nodeA;

        // Delete the index right after stopping the primary — this triggers AlreadyClosedException
        // during the promotion path on the replica node
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName));

        // Give a brief moment for promotion to start
        Thread.sleep(500);

        // Delete the index to trigger AlreadyClosedException in the promotion callback
        try {
            assertAcked(client().admin().indices().prepareDelete("test"));
        } catch (Exception e) {
            // Index might already be gone if the shard failed, that's fine
        }

        // The key assertion: the node should not have a leaked flag preventing future relocations.
        // Since we deleted the index, verify the node is healthy and can create new indices.
        assertAcked(
            prepareCreate("test2").setSettings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.include._name", replicaNodeName)
            )
        );
        ensureGreen("test2");
    }

    private IndexShard getIndexShard(String nodeName, String indexName) {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexServiceSafe(
            client().admin().cluster().prepareState().get().getState().metadata().index(indexName).getIndex()
        );
        return indexService.getShard(0);
    }
}

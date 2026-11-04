/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.OpenSearchException;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
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
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Verifies that a replica promoted to primary can still be relocated when its primary-replica resync does not complete
 * normally. See <a href="https://github.com/opensearch-project/OpenSearch/issues/21692">#21692</a>.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrimaryReplicaResyncIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "test";
    private static final int NUM_DOCS = 100;
    private static final String RESYNC_PRIMARY_ACTION = "internal:index/seq_no/resync" + TransportReplicationAction.PRIMARY_ACTION_SUFFIX;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    /**
     * The resync request on the promoted primary is swallowed without a reply, which is equivalent to a lost transport
     * response: neither onResponse nor onFailure ever fires. Relocation must be refused while the resync is pending and
     * must succeed once the resync timeout clears the flag.
     */
    public void testPrimaryRelocationSucceedsAfterResyncTimeout() throws Exception {
        final Nodes nodes = startNodesAndCreateIndex();
        // set dynamically so that the test also covers updates of the setting through the cluster settings API
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(RecoverySettings.INDICES_PRIMARY_RESYNC_TIMEOUT_SETTING.getKey(), "20s"))
        );

        final CountDownLatch resyncDropped = new CountDownLatch(1);
        final MockTransportService futurePrimaryTransport = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            nodes.replica
        );
        futurePrimaryTransport.addRequestHandlingBehavior(RESYNC_PRIMARY_ACTION, (handler, request, channel, task) -> {
            // neither handle the request nor reply on the channel
            resyncDropped.countDown();
        });

        promoteReplica(nodes);
        assertTrue("resync request was not intercepted", resyncDropped.await(30, TimeUnit.SECONDS));
        final IndexShard promotedShard = getIndexShard(nodes.replica);
        assertTrue(promotedShard.isPrimaryReplicaResyncInProgress());

        // relocation is refused while the resync is pending: the relocation target fails and the primary stays put
        moveToTarget(nodes);
        assertBusy(() -> assertPrimaryStartedOn(nodes.replica), 30, TimeUnit.SECONDS);
        assertTrue(promotedShard.isPrimaryReplicaResyncInProgress());

        // the resync timeout clears the flag
        assertBusy(() -> assertFalse(promotedShard.isPrimaryReplicaResyncInProgress()), 60, TimeUnit.SECONDS);
        futurePrimaryTransport.clearAllRules();

        moveToTarget(nodes);
        assertRelocatedToTarget(nodes);
    }

    /**
     * The resync request on the promoted primary fails. The flag must be cleared by onFailure, the shard is failed and
     * re-allocated, and the primary can then be relocated.
     */
    public void testPrimaryRelocationSucceedsAfterResyncFailure() throws Exception {
        final Nodes nodes = startNodesAndCreateIndex();

        final CountDownLatch resyncFailed = new CountDownLatch(1);
        final MockTransportService futurePrimaryTransport = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            nodes.replica
        );
        futurePrimaryTransport.addRequestHandlingBehavior(RESYNC_PRIMARY_ACTION, (handler, request, channel, task) -> {
            channel.sendResponse(new OpenSearchException("simulated primary-replica resync failure"));
            resyncFailed.countDown();
        });

        final long initialPrimaryTerm = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX).primaryTerm(0);
        promoteReplica(nodes);
        assertTrue("resync request was not intercepted", resyncFailed.await(30, TimeUnit.SECONDS));
        futurePrimaryTransport.clearAllRules();

        // The failed shard is re-allocated from its local store on the same node, with the flag cleared. It keeps its
        // allocation id, so tell the new shard apart from the promoted one by its primary term: one bump for the promotion
        // and another for the re-allocation of the failed primary.
        assertBusy(() -> {
            assertPrimaryStartedOn(nodes.replica);
            final IndexShard shard = getIndexShard(nodes.replica);
            assertThat(shard.getOperationPrimaryTerm(), greaterThanOrEqualTo(initialPrimaryTerm + 2));
            assertFalse(shard.isPrimaryReplicaResyncInProgress());
        }, 60, TimeUnit.SECONDS);

        moveToTarget(nodes);
        assertRelocatedToTarget(nodes);
    }

    /**
     * Starts a cluster-manager node and three data nodes, and creates a one-shard index with one replica on the first two
     * data nodes. The third data node is the relocation target.
     */
    private Nodes startNodesAndCreateIndex() {
        internalCluster().startClusterManagerOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();
        final String target = internalCluster().startDataOnlyNode();

        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put("index.routing.allocation.include._name", nodeA + "," + nodeB)
                    // reassign the copy lost with the stopped node immediately instead of after the default 1m delay
                    .put("index.unassigned.node_left.delayed_timeout", "0")
            )
        );
        ensureGreen(INDEX);
        for (int i = 0; i < NUM_DOCS; i++) {
            client().prepareIndex(INDEX).setSource("field", "value" + i).get();
        }
        refresh(INDEX);

        final String primary = primaryNodeName();
        return new Nodes(primary, primary.equals(nodeA) ? nodeB : nodeA, target);
    }

    /**
     * Stops the primary's node so that the replica is promoted, then allows the shard on the target node only. Zero
     * replicas keep the copy lost with the stopped node from claiming the target node before the primary moves there.
     */
    private void promoteReplica(Nodes nodes) throws Exception {
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodes.primary));
        assertBusy(() -> assertPrimaryStartedOn(nodes.replica), 30, TimeUnit.SECONDS);
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX)
                .setSettings(
                    Settings.builder()
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.include._name", nodes.replica + "," + nodes.target)
                )
        );
    }

    private void moveToTarget(Nodes nodes) {
        if (nodes.target.equals(primaryNodeName())) {
            return; // already moved there by a reroute of the allocator
        }
        assertTrue(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(INDEX, 0, nodes.replica, nodes.target))
                .get()
                .isAcknowledged()
        );
    }

    private void assertRelocatedToTarget(Nodes nodes) throws Exception {
        assertBusy(() -> assertPrimaryStartedOn(nodes.target), 60, TimeUnit.SECONDS);
        ensureGreen(INDEX);
        assertHitCount(client().prepareSearch(INDEX).setSize(0).get(), NUM_DOCS);
    }

    private String primaryNodeName() {
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        return state.nodes().get(state.routingTable().index(INDEX).shard(0).primaryShard().currentNodeId()).getName();
    }

    /**
     * Asserts that the primary is started on the given node. Routing and nodes are read from the same cluster
     * state and failures are reported as assertion errors, so this can be used inside {@link #assertBusy}.
     */
    private void assertPrimaryStartedOn(String nodeName) {
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final ShardRouting primary = state.routingTable().index(INDEX).shard(0).primaryShard();
        assertTrue("primary not started: " + primary, primary.started());
        final DiscoveryNode node = state.nodes().get(primary.currentNodeId());
        assertNotNull("primary on unknown node: " + primary, node);
        assertThat(node.getName(), equalTo(nodeName));
    }

    private IndexShard getIndexShard(String nodeName) {
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        final IndexService indexService = indicesService.indexService(resolveIndex(INDEX));
        assertNotNull("index not allocated on " + nodeName, indexService);
        final IndexShard shard = indexService.getShardOrNull(0);
        assertNotNull("shard not allocated on " + nodeName, shard);
        return shard;
    }

    private static final class Nodes {
        final String primary;
        final String replica;
        final String target;

        Nodes(String primary, String replica, String target) {
            this.primary = primary;
            this.replica = replica;
            this.target = target;
        }
    }
}

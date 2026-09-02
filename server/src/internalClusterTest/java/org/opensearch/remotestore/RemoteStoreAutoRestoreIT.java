/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.coordination.FollowersChecker;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.translog.Translog;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * End-to-end coverage for the auto-restore trigger ({@code BaseGatewayShardAllocator#maybeAutoRestoreFromRemoteStore}):
 * when no valid copy of a fenced, remote-backed primary survives on any live node - zero replicas losing its one
 * node, or N replicas losing every copy-holding node - the shard is re-pointed at a
 * {@link RecoverySource.RemoteStoreRecoverySource} inside the allocation round instead of being parked RED at
 * {@code NO_VALID_SHARD_COPY}, health stays RED while the restore hydrates (a hydrating primary cannot serve queries;
 * YELLOW is deferred until warm/searchable-remote shards can serve queries directly off the remote store) and
 * converges to GREEN without operator action, and the restored primary serves every
 * acknowledged operation. While an in-sync replica survives, promotion wins and the trigger stays out of the way. The
 * multi-writer safety of the sequence (a departed primary that is still alive behind a partition) rides on the fence
 * takeover verified in {@code FenceAutoRestore.tla} and shipped with the fence PR.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreAutoRestoreIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-auto-restore-idx";
    private static final String TOTAL_OPERATIONS = "total-operations";

    /** This suite manages fencing and auto-restore explicitly per index; ignore the suite-wide randomization. */
    @Override
    protected boolean remoteStoreFencingForAllIndices() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class)).collect(Collectors.toList());
    }

    /**
     * {@code REQUEST} durability pins the contract being asserted - every acknowledged operation is in the remote
     * translog, so the restored primary must serve all of them. The replica count is the test's failure-domain
     * parameter: the trigger itself is replica-count independent (it fires on the allocator's no-valid-copy proof,
     * which with replicas configured requires every copy-holding node to be gone).
     */
    private Settings autoRestoreIndexSettings(int replicaCount, boolean autoRestoreEnabled) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(replicaCount, 1))
            .put(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_STORE_AUTO_RESTORE_ENABLED, autoRestoreEnabled)
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST.name())
            // The trigger honors the node-left grace window before converting; zero it so these tests exercise
            // immediate conversion. testAutoRestoreWaitsOutNodeLeftDelay covers the non-zero window explicitly.
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
            .build();
    }

    /**
     * Observes every published cluster state on the cluster manager. The restore window is precisely the states in
     * which the primary carries a {@code REMOTE_STORE} recovery source (set on conversion, cleared on shard start);
     * the observer records that the window was entered at all - proof the trigger fired rather than some other
     * allocation path - and any state in that window whose computed index health was NOT RED. A hydrating primary
     * cannot serve queries, so health deliberately stays RED until the shard starts (see
     * {@code ClusterShardHealth#getInactivePrimaryHealth}); YELLOW would overstate availability.
     */
    private final class RestoreWindowObserver implements ClusterStateListener, AutoCloseable {
        private final AtomicBoolean sawRemoteStoreRecoverySource = new AtomicBoolean();
        private final List<String> nonRedStatesDuringRestore = new CopyOnWriteArrayList<>();
        private final ClusterService clusterService;

        RestoreWindowObserver() {
            this.clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getClusterManagerName());
            clusterService.addListener(this);
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            ClusterState state = event.state();
            IndexRoutingTable indexRoutingTable = state.routingTable().index(INDEX_NAME);
            if (indexRoutingTable == null) {
                return;
            }
            ShardRouting primary = indexRoutingTable.shard(0).primaryShard();
            if (primary == null || primary.recoverySource() == null) {
                return;
            }
            if (primary.recoverySource().getType() == RecoverySource.Type.REMOTE_STORE) {
                sawRemoteStoreRecoverySource.set(true);
                ClusterHealthStatus health = new ClusterIndexHealth(state.metadata().index(INDEX_NAME), indexRoutingTable).getStatus();
                if (health != ClusterHealthStatus.RED) {
                    nonRedStatesDuringRestore.add(
                        "state version [" + state.version() + "] health [" + health + "] primary [" + primary + "]"
                    );
                }
            }
        }

        void assertRestoredRedUntilStarted() {
            assertTrue(
                "the allocator should have re-pointed the lost primary at a REMOTE_STORE recovery source",
                sawRemoteStoreRecoverySource.get()
            );
            assertTrue(
                "index health must stay RED (a hydrating primary cannot serve queries) until the restored primary starts: "
                    + nonRedStatesDuringRestore,
                nonRedStatesDuringRestore.isEmpty()
            );
        }

        void assertTriggerNeverFired() {
            assertFalse(
                "the trigger must not fire while an in-sync replica is available for promotion",
                sawRemoteStoreRecoverySource.get()
            );
        }

        @Override
        public void close() {
            clusterService.removeListener(this);
        }
    }

    /**
     * The headline flow: kill the node holding a fenced zero-replica primary and assert the shard auto-restores from
     * the remote store - RED (deliberately, a hydrating primary cannot serve queries) for every published cluster
     * state in which the shard carries a {@code REMOTE_STORE} recovery source, GREEN once hydrated with no operator
     * action, all acknowledged operations present, and the index writable again.
     */
    public void testAutoRestoreOnNodeLoss() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createIndex(INDEX_NAME, autoRestoreIndexSettings(0, true));
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(randomIntBetween(2, 4), true, INDEX_NAME);
        String primaryNode = primaryNodeName(INDEX_NAME);

        try (RestoreWindowObserver observer = new RestoreWindowObserver()) {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));

            ensureGreen(TimeValue.timeValueSeconds(60), INDEX_NAME);

            observer.assertRestoredRedUntilStarted();
        }

        // Every acknowledged operation survived the node loss ...
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS)),
            30,
            TimeUnit.SECONDS
        );
        // ... and the restored primary accepts new writes.
        indexSingleDoc(INDEX_NAME);
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS) + 1),
            30,
            TimeUnit.SECONDS
        );
    }

    /**
     * The control: without {@code index.remote_store.auto_restore.enabled} the trigger must not fire - the shard is
     * parked at {@code NO_VALID_SHARD_COPY} with its {@code EXISTING_STORE} source intact (today's RED behavior), and
     * the operator's manual {@code _remotestore/_restore} path still recovers it.
     */
    public void testNoAutoRestoreWithoutSetting() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createIndex(INDEX_NAME, autoRestoreIndexSettings(0, false));
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(randomIntBetween(2, 4), true, INDEX_NAME);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));

        ensureRed(INDEX_NAME);
        assertBusy(() -> {
            ShardRouting primary = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .routingTable()
                .index(INDEX_NAME)
                .shard(0)
                .primaryShard();
            assertTrue("primary should stay parked unassigned without the auto-restore setting", primary.unassigned());
            assertEquals(RecoverySource.Type.EXISTING_STORE, primary.recoverySource().getType());
            assertEquals(UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY, primary.unassignedInfo().getLastAllocationStatus());
        }, 30, TimeUnit.SECONDS);

        // The manual operator path is unaffected by the (disabled) trigger.
        restore(randomBoolean(), INDEX_NAME);
        ensureGreen(TimeValue.timeValueSeconds(60), INDEX_NAME);
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS)),
            30,
            TimeUnit.SECONDS
        );
    }

    /**
     * The partitioned-writer variant - the scenario {@code FenceAutoRestore.tla} refutes for an unfenced trigger: the
     * old primary is not dead, only expelled from the cluster manager's view, and (on a shared filesystem repository)
     * can still reach the object store. The trigger fires on the membership view; the fence takeover at a strictly
     * higher term is what stops the departed writer acknowledging operations the restored copy will never see. The
     * test asserts the observable consequences end to end: the restore lands on the surviving node while the old
     * primary is alive, every operation acknowledged before the partition is served, the healed cluster converges
     * with the restored primary (the rejoined stale copy is dropped, not re-adopted), and the index stays writable.
     */
    public void testAutoRestoreWithPartitionedWriter() throws Exception {
        // Fast follower checks so the cluster manager expels the partitioned node quickly. Leader checks stay at
        // their defaults: a DISCONNECT disruption fails the isolated node's leader check immediately anyway, and
        // slowing them (as PrimaryTermValidationIT does to keep its stale primary deluded) would also delay the
        // node's post-heal rejoin, which this test does wait for.
        Settings fastFollowerChecks = Settings.builder()
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .build();
        internalCluster().startClusterManagerOnlyNode(fastFollowerChecks);
        internalCluster().startDataOnlyNodes(2, fastFollowerChecks);
        createIndex(INDEX_NAME, autoRestoreIndexSettings(0, true));
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(randomIntBetween(2, 4), true, INDEX_NAME);
        String primaryNode = primaryNodeName(INDEX_NAME);
        String clusterManagerNode = internalCluster().getClusterManagerName();
        String survivorDataNode = internalCluster().client(clusterManagerNode)
            .admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(node -> node.getName())
            .filter(name -> name.equals(primaryNode) == false)
            .findFirst()
            .orElseThrow();

        Set<String> liveSide = Stream.of(clusterManagerNode, survivorDataNode).collect(Collectors.toCollection(HashSet::new));
        Set<String> isolatedSide = Stream.of(primaryNode).collect(Collectors.toCollection(HashSet::new));
        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(liveSide, isolatedSide),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        // The cluster manager expels the isolated primary's node and the trigger restores the shard onto the
        // surviving data node while the old primary is still alive. All three facts - membership shrank to two,
        // the primary is STARTED, and it lives on the survivor - are asserted against the SAME observed cluster
        // state: a node-count probe that merely counts responders (e.g. nodes-info) passes while the disruption
        // blocks the isolated node but before the node-left state is applied, and a health-only probe can then
        // read the stale pre-removal GREEN.
        assertBusy(() -> {
            ClusterState state = client(clusterManagerNode).admin().cluster().prepareState().get().getState();
            assertEquals("isolated node should be expelled from the cluster manager's view", 2, state.nodes().getSize());
            ShardRouting primary = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
            assertTrue("primary should be restored and started, was " + primary, primary.started());
            assertEquals(survivorDataNode, state.nodes().get(primary.currentNodeId()).getName());
        }, 60, TimeUnit.SECONDS);

        // The recovery that produced the new primary was a remote-store restore, not any other allocation path.
        RecoveryResponse recoveries = client(clusterManagerNode).admin().indices().prepareRecoveries(INDEX_NAME).get();
        assertTrue(
            "the surviving node's primary should have recovered from the remote store",
            recoveries.shardRecoveryStates()
                .get(INDEX_NAME)
                .stream()
                .anyMatch(
                    recovery -> recovery.getPrimary()
                        && recovery.getRecoverySource().getType() == RecoverySource.Type.REMOTE_STORE
                        && survivorDataNode.equals(recovery.getTargetNode().getName())
                )
        );

        // No acknowledged write was lost to the takeover (the fence refused anything the departed writer tried after
        // the seal; anything it completed before the seal is in the restore point by the metadata-then-CAS ordering).
        client(clusterManagerNode).admin().indices().prepareRefresh(INDEX_NAME).get();
        assertBusy(
            () -> assertHitCount(client(clusterManagerNode).prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS)),
            30,
            TimeUnit.SECONDS
        );

        // Heal the partition: the stale copy on the rejoined node is dropped, not re-adopted, and the index keeps
        // serving and accepting writes.
        networkDisruption.stopDisrupting();
        internalCluster().clearDisruptionScheme();
        ensureStableCluster(3, TimeValue.timeValueSeconds(60));
        ensureGreen(TimeValue.timeValueSeconds(60), INDEX_NAME);

        indexSingleDoc(INDEX_NAME);
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS) + 1),
            30,
            TimeUnit.SECONDS
        );
    }

    /**
     * The trigger is replica-count independent: with a replica configured, the allocator only ever reaches
     * {@code NO_VALID_SHARD_COPY} when EVERY copy-holding node is gone (a surviving in-sync replica is promoted on
     * the failover path instead). Losing the replica's node and then the primary's node must therefore auto-restore
     * exactly as in the zero-replica case, after which the replica peer-recovers from the restored primary on a
     * newly joined node.
     */
    public void testAutoRestoreWithReplicaWhenAllCopiesLost() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);
        createIndex(INDEX_NAME, autoRestoreIndexSettings(1, true));
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(randomIntBetween(2, 4), true, INDEX_NAME);
        String primaryNode = primaryNodeName(INDEX_NAME);
        String replicaNode = replicaNodeName(INDEX_NAME);

        try (RestoreWindowObserver observer = new RestoreWindowObserver()) {
            // Replica's node first, so no promotion happens in between; then the primary's node - all copies gone.
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNode));
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));

            // Only one data node remains, so the replica cannot allocate: the settled state is YELLOW with the
            // restored primary STARTED and serving.
            ensureYellowAndNoInitializingShards(INDEX_NAME);
            assertBusy(() -> {
                ShardRouting primary = client().admin()
                    .cluster()
                    .prepareState()
                    .get()
                    .getState()
                    .routingTable()
                    .index(INDEX_NAME)
                    .shard(0)
                    .primaryShard();
                assertTrue("restored primary should be started, was " + primary, primary.started());
            }, 60, TimeUnit.SECONDS);

            observer.assertRestoredRedUntilStarted();
        }

        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS)),
            30,
            TimeUnit.SECONDS
        );

        // A new data node joins: the replica peer-recovers from the auto-restored primary and the index goes GREEN.
        internalCluster().startDataOnlyNode();
        ensureGreen(TimeValue.timeValueSeconds(60), INDEX_NAME);
        indexSingleDoc(INDEX_NAME);
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS) + 1),
            30,
            TimeUnit.SECONDS
        );
    }

    /**
     * The boundary from the other side: while an in-sync replica survives, losing the primary's node is handled by
     * replica promotion and the trigger must never fire - {@code DECIDERS_NO}/promotion territory is real data the
     * trigger would otherwise abandon. The primary must never carry a {@code REMOTE_STORE} recovery source.
     */
    public void testReplicaPromotionPreemptsAutoRestore() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createIndex(INDEX_NAME, autoRestoreIndexSettings(1, true));
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(randomIntBetween(2, 4), true, INDEX_NAME);
        String primaryNode = primaryNodeName(INDEX_NAME);
        String replicaNode = replicaNodeName(INDEX_NAME);

        try (RestoreWindowObserver observer = new RestoreWindowObserver()) {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));

            // The in-sync replica is promoted; with only one data node left the replica slot stays unassigned.
            ensureYellowAndNoInitializingShards(INDEX_NAME);
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting primary = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
                assertTrue("promoted primary should be started, was " + primary, primary.started());
                assertEquals(replicaNode, state.nodes().get(primary.currentNodeId()).getName());
            }, 60, TimeUnit.SECONDS);

            observer.assertTriggerNeverFired();
        }

        // The promoted primary serves every acknowledged operation (promotion replays the remote translog) and
        // accepts new writes.
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS)),
            30,
            TimeUnit.SECONDS
        );
        indexSingleDoc(INDEX_NAME);
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS) + 1),
            30,
            TimeUnit.SECONDS
        );
    }

    /**
     * The trigger honors the node-left grace window: with a non-zero
     * {@code index.unassigned.node_left.delayed_timeout}, the conversion is declined while the delay marker is live
     * (so a bouncing node can rejoin and recover its local copy - a converted primary has no rejoin-cancellation
     * path today), and fires on the reroute {@code DelayedAllocationService} schedules once the window expires.
     */
    public void testAutoRestoreWaitsOutNodeLeftDelay() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        Settings indexSettings = Settings.builder()
            .put(autoRestoreIndexSettings(0, true))
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(15))
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(randomIntBetween(2, 4), true, INDEX_NAME);
        String primaryNode = primaryNodeName(INDEX_NAME);

        try (RestoreWindowObserver observer = new RestoreWindowObserver()) {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));

            // Wait for the node-left state to apply, then assert the shard is parked with its EXISTING_STORE source
            // and a live delay marker. Deterministic: the conversion cannot legally happen before the 15s window
            // expires, and this assertBusy caps at 10s.
            assertBusy(() -> {
                ClusterState state = client(internalCluster().getClusterManagerName()).admin().cluster().prepareState().get().getState();
                ShardRouting primary = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
                assertTrue("primary should be parked during the node-left grace window, was " + primary, primary.unassigned());
                assertEquals(RecoverySource.Type.EXISTING_STORE, primary.recoverySource().getType());
                assertTrue("the node-left delay marker should be live", primary.unassignedInfo().isDelayed());
            }, 10, TimeUnit.SECONDS);
            assertFalse(observer.sawRemoteStoreRecoverySource.get());

            // Once the window expires, DelayedAllocationService's reroute clears the marker and the trigger fires.
            ensureGreen(TimeValue.timeValueSeconds(60), INDEX_NAME);
            observer.assertRestoredRedUntilStarted();
        }

        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS)),
            30,
            TimeUnit.SECONDS
        );
    }
}

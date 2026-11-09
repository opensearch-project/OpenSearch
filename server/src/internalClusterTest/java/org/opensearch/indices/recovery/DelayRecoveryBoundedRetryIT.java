/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.recovery;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Reproduces the "stuck recovery in INIT" incident at the cluster level and verifies the bound on delay retries.
 *
 * <p>The production trigger is a ghost in-sync allocation id: an id in {@code IndexMetadata.in_sync_allocations} with
 * no matching routing entry. Every write flags it stale via {@code ReplicationOperation.markUnavailableShardsAsStale};
 * the master removes it and reallocates a fresh replica with a brand-new allocation id; but the source primary's
 * replication-group routing table is only refreshed on cluster-state application, so during the propagation window
 * {@link RecoverySourceHandler#waitForAssignmentPropagate} cannot find the new target id and throws
 * {@link DelayRecoveryException}. Before the fix, {@code PeerRecoveryTargetService.onException} retried that exception
 * without any upper bound, pinning a recovery slot in INIT indefinitely and exhausting
 * {@code cluster.routing.allocation.node_concurrent_incoming_recoveries}.
 *
 * <p>This test simulates the symptom directly: the source primary's {@code START_RECOVERY} handler always throws
 * {@link DelayRecoveryException} (as if the target id never propagated). With the fix, the target-side
 * {@code PeerRecoveryTargetService.onException} bounds the consecutive delay retries to
 * {@link RecoveryTarget#MAX_DELAY_RETRIES} and then fails the recovery (releasing the slot). Without the fix the
 * recovery would loop forever; the test's {@code assertBusy} would time out waiting for the shard to leave INIT,
 * reproducing the stuck-INIT condition.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DelayRecoveryBoundedRetryIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "delay-recovery-bounded";
    // NOTE: intentionally a literal here (not RecoveryTarget.MAX_DELAY_RETRIES) so that this IT can be compiled and
    // run against the UNPATCHED RecoveryTarget / PeerRecoveryTargetService to reproduce the stuck-INIT incident.
    // The production constant in the patch is 60; keep this in sync if that changes.
    private static final int MAX_DELAY_RETRIES = 60;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    /**
     * The source primary keeps returning {@link DelayRecoveryException} for every {@code START_RECOVERY} attempt,
     * simulating a target allocation id that never propagates into the primary's routing table (the ghost-id
     * propagation window). Asserts the replica recovery is failed (becomes UNASSIGNED with a failure reason) after a
     * bounded number of retries, rather than looping forever in INITIALIZING.
     */
    public void testRecoveryFailsAfterBoundedDelayRetries() throws Exception {
        // Use a small retry delay so MAX_DELAY_RETRIES * retryDelay stays well under the assertBusy timeout.
        // Default retry_delay_state_sync is 500ms; with MAX_DELAY_RETRIES=60 that's ~30s. Setting 50ms => ~3s.
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.getKey(), "50ms")
            .build();

        final String clusterManager = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        final String nodeA = internalCluster().startDataOnlyNode(nodeSettings);
        final String nodeB = internalCluster().startDataOnlyNode(nodeSettings);

        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat("cluster did not form", health.isTimedOut(), is(false));

        // create index with 1 primary on nodeA, 0 replicas initially
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.require._name", nodeA)
                .build()
        );
        ensureGreen(INDEX_NAME);

        // index some docs so the primary has data
        final int numDocs = between(1, 50);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(Integer.toString(i))
                .setSource("{}", org.opensearch.core.xcontent.MediaTypeRegistry.JSON)
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // locate the primary node and install a START_RECOVERY handler that always throws DelayRecoveryException,
        // simulating the source primary never seeing the target allocation id in its routing table.
        final ClusterState preState = client().admin().cluster().prepareState().get().getState();
        final ShardId shardId = new ShardId(preState.metadata().index(INDEX_NAME).getIndex(), 0);
        final String primaryNodeName = primaryNodeName(preState, shardId);
        assertThat("primary must be on nodeA", primaryNodeName, equalTo(nodeA));

        final MockTransportService primaryTransport = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNodeName
        );
        final AtomicInteger startRecoveryAttempts = new AtomicInteger();
        primaryTransport.addRequestHandlingBehavior(PeerRecoverySourceService.Actions.START_RECOVERY, (handler, request, channel, task) -> {
            // Every START_RECOVERY attempt is answered with a DelayRecoveryException, exactly what
            // waitForAssignmentPropagate throws when the target allocation id is absent from the primary's routing
            // table. This is the symptom the ghost in-sync id produces during the cluster-state propagation window.
            startRecoveryAttempts.incrementAndGet();
            throw new DelayRecoveryException(
                "simulated stuck-INIT: target allocation id not yet in source primary routing table (ghost-id repro)"
            );
        });

        // bump replicas to 1 -> triggers a peer recovery from nodeA (primary) to nodeB (replica). The master will not
        // co-locate the replica with the primary, so it is assigned to nodeB and recovery starts.
        // With the handling behavior above, every START_RECOVERY returns DelayRecoveryException.
        // We also clear the index.routing.allocation.require._name filter (which pinned the primary to nodeA); without
        // clearing it, the replica would also be required to be on nodeA, but a replica cannot co-locate with its
        // primary, so the replica would stay UNASSIGNED forever and no peer recovery would start.
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).putNull("index.routing.allocation.require._name")
                )
        );

        // Wait for the replica to be assigned (INITIALIZING) and at least one START_RECOVERY attempt to fire BEFORE
        // disabling allocation. If we disable allocation immediately after the settings update, the master's reroute
        // for the new replica is blocked by allocation.enable=none, the replica stays UNASSIGNED(REPLICA_ADDED), no
        // peer recovery ever starts, and the test never exercises the bounded-retry path. Gating on INITIALIZING +
        // attempts>=1 ensures the recovery is in flight (the shard is already allocated, so allocation.disable only
        // affects post-failure re-allocation, not the in-flight recovery).
        assertBusy(() -> {
            final ClusterState state = client().admin().cluster().prepareState().get().getState();
            final ShardRouting replica = replicaRouting(state, shardId);
            assertNotNull("expected a replica routing entry to exist after bumping replicas", replica);
            assertThat(
                "expected replica to be INITIALIZING before disabling allocation, but was " + replica.state(),
                replica.state(),
                equalTo(ShardRoutingState.INITIALIZING)
            );
            assertTrue(
                "expected at least one START_RECOVERY attempt before disabling allocation, got " + startRecoveryAttempts.get(),
                startRecoveryAttempts.get() >= 1
            );
        }, 30, TimeUnit.SECONDS);

        // Now disable allocation so that once the bounded-retry recovery fails, the replica is NOT immediately
        // re-allocated by the master. Without this, the failed replica's unassigned reason flips from
        // ALLOCATION_FAILED to REPLICA_ADDED on the next reroute, racing with the assertion below. Allocation disable
        // does not affect the in-flight recovery (the shard is already INITIALIZING), only post-failure re-allocation.
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("cluster.routing.allocation.enable", "none"))
        );

        // With the fix: PeerRecoveryTargetService.onException bounds consecutive DelayRecoveryException retries to
        // MAX_DELAY_RETRIES and then calls onGoingRecoveries.fail(...). The replica shard must leave INITIALIZING
        // and become UNASSIGNED with ALLOCATION_FAILED (allocation is disabled, so it stays there). Without the fix
        // the shard stays INITIALIZING forever and this assertBusy times out, reproducing the stuck-INIT incident.
        assertBusy(() -> {
            // Gate the unassigned-reason assertion on the recovery loop having actually exhausted its bound.
            // Without this gate, assertBusy can race ahead of the first START_RECOVERY attempt and observe the
            // replica in its transient pre-reroute state, which is UNASSIGNED with reason REPLICA_ADDED (set by
            // IndexRoutingTable.addReplica when number_of_replicas was bumped). That transient state satisfies
            // state != INITIALIZING and unassigned(), but its reason is REPLICA_ADDED, not ALLOCATION_FAILED,
            // producing a spurious assertion failure. Requiring MAX_DELAY_RETRIES+1 attempts first guarantees the
            // bounded-retry fail path has fired and the master has recorded ALLOCATION_FAILED via localShardFailed.
            assertTrue(
                "expected at least "
                    + (MAX_DELAY_RETRIES + 1)
                    + " START_RECOVERY attempts before failing, got "
                    + startRecoveryAttempts.get(),
                startRecoveryAttempts.get() >= MAX_DELAY_RETRIES + 1
            );

            final ClusterState state = client().admin().cluster().prepareState().get().getState();
            final ShardRouting replica = replicaRouting(state, shardId);
            assertNotNull("expected a replica routing entry to exist", replica);
            assertTrue(
                "expected replica to have left INITIALIZING after bounded delay retries, but state was "
                    + replica.state()
                    + " (attempts so far: "
                    + startRecoveryAttempts.get()
                    + ")",
                replica.state() != ShardRoutingState.INITIALIZING
            );
            assertTrue("expected replica to be unassigned after recovery failure, but was " + replica.state(), replica.unassigned());
            assertNotNull("expected unassigned info on the failed replica", replica.unassignedInfo());
            assertThat(
                "expected ALLOCATION_FAILED reason, got " + replica.unassignedInfo().getReason(),
                replica.unassignedInfo().getReason(),
                equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED)
            );
        }, 60, TimeUnit.SECONDS);

        // Sanity upper bound: with a 50ms retry delay and a 60s assertBusy window, the count must stay reasonable.
        // (Without the fix this would grow unbounded until the test timed out.)
        assertTrue(
            "unexpectedly large number of attempts: " + startRecoveryAttempts.get(),
            startRecoveryAttempts.get() < MAX_DELAY_RETRIES + 50
        );

        // cleanup: clear the behavior, re-enable allocation, drop replicas, and let the cluster settle.
        primaryTransport.clearAllRules();
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("cluster.routing.allocation.enable", "all"))
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        ensureGreen(INDEX_NAME);
    }

    /** Returns the name of the node holding the primary for {@code shardId}, or null if not found. */
    private static String primaryNodeName(ClusterState state, ShardId shardId) {
        final ShardRouting primary = state.routingTable().shardRoutingTable(shardId).primaryShard();
        if (primary == null || primary.assignedToNode() == false) {
            return null;
        }
        return state.nodes().get(primary.currentNodeId()).getName();
    }

    /** Returns the (non-primary) replica routing entry for {@code shardId}, or null if none exists. */
    private static ShardRouting replicaRouting(ClusterState state, ShardId shardId) {
        for (ShardRouting sr : state.routingTable().shardRoutingTable(shardId)) {
            if (sr.primary() == false) {
                return sr;
            }
        }
        return null;
    }
}

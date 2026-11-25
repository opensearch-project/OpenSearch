/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.action.shard;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.opensearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalShardStateActionTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";

    public void testAFailedStartedPrimaryBecomesUnassigned() {
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        ShardRouting primary = shardTable(state).primaryShard();

        ClusterState failed = LocalShardStateAction.failShard(state, primary, "engine failure", new RuntimeException("boom"));

        ShardRouting unassigned = shardTable(failed).primaryShard();
        assertTrue(unassigned.unassigned());
        assertNull(unassigned.currentNodeId());
        assertEquals(RecoverySource.Type.EXISTING_STORE, unassigned.recoverySource().getType());
        UnassignedInfo info = unassigned.unassignedInfo();
        assertEquals(UnassignedInfo.Reason.ALLOCATION_FAILED, info.getReason());
        assertEquals(1, info.getNumFailedAllocations());
        assertTrue(info.getMessage(), info.getMessage().contains("engine failure"));
        assertEquals("boom", info.getFailure().getMessage());
        assertEquals("the other copy is untouched", shardTable(state).replicaShards().get(0), shardTable(failed).replicaShards().get(0));
    }

    public void testOnlyTheRoutingTableChanges() {
        // Primary terms and in-sync allocation ids belong to whatever builds the local cluster state.
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        ShardRouting primary = shardTable(state).primaryShard();

        ClusterState failed = LocalShardStateAction.failShard(state, primary, "engine failure", null);

        IndexMetadata before = state.metadata().index(INDEX);
        IndexMetadata after = failed.metadata().index(INDEX);
        assertEquals(before.primaryTerm(0), after.primaryTerm(0));
        assertEquals(before.inSyncAllocationIds(0), after.inSyncAllocationIds(0));
    }

    public void testAnInitializingCopyThatFailedBeforeCountsItsEarlierFailures() {
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        ShardRouting replica = shardTable(state).replicaShards().get(0);
        ShardRouting failedBefore = replica.moveToUnassigned(
            new UnassignedInfo(
                UnassignedInfo.Reason.ALLOCATION_FAILED,
                "earlier",
                null,
                2,
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                Set.of("some-other-node")
            )
        ).initialize(replica.currentNodeId(), null, -1);
        state = withCopy(state, replica, failedBefore);

        ClusterState failed = LocalShardStateAction.failShard(state, failedBefore, "recovery failed", null);

        UnassignedInfo info = shardTable(failed).replicaShards().get(0).unassignedInfo();
        assertEquals(3, info.getNumFailedAllocations());
        assertEquals(Set.of("some-other-node", replica.currentNodeId()), info.getFailedNodeIds());
    }

    public void testTheCopyIsFoundByAllocationIdNotByEquality() {
        // The copy failed while initializing, and the state has since marked it started. Removing by equality with the
        // routing that failed would remove nothing, and leave the copy stuck.
        ClusterState state = state(INDEX, true, ShardRoutingState.INITIALIZING);
        ShardRouting initializing = shardTable(state).primaryShard();
        ClusterState started = withCopy(state, initializing, initializing.moveToStarted());

        ClusterState failed = LocalShardStateAction.failShard(started, initializing, "failed", null);

        assertTrue(shardTable(failed).primaryShard().unassigned());
    }

    public void testAFailureForAnAllocationNoLongerInTheStateChangesNothing() {
        // Stale: the copy that failed has been replaced, and its successor must not be failed in its place.
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED);
        ShardRouting primary = shardTable(state).primaryShard();
        ShardRouting successor = primary.moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "earlier"))
            .initialize(primary.currentNodeId(), null, -1);
        ClusterState replaced = withCopy(state, primary, successor);
        assertNotEquals(primary.allocationId().getId(), shardTable(replaced).primaryShard().allocationId().getId());

        assertSame(replaced, LocalShardStateAction.failShard(replaced, primary, "stale", null));
    }

    public void testAFailureForAnIndexNoLongerInTheStateChangesNothing() {
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED);
        ShardRouting primary = shardTable(state).primaryShard();
        ClusterState withoutIndex = ClusterState.builder(state).routingTable(RoutingTable.EMPTY_ROUTING_TABLE).build();

        assertSame(withoutIndex, LocalShardStateAction.failShard(withoutIndex, primary, "deleted", null));
    }

    // A routing table holds a relocation as one entry: the RELOCATING source, from which the INITIALIZING target is
    // derived (IndexShardRoutingTable lists the target among its assigned shards, and getByAllocationId finds it).
    // So either end can be the copy that failed, and only the source is there to change.

    public void testAFailedRelocatingPrimaryIsUnassignedAndTheRelocationWithIt() {
        ClusterState state = state(INDEX, true, ShardRoutingState.RELOCATING);
        ShardRouting source = shardTable(state).primaryShard();

        ClusterState failed = LocalShardStateAction.failShard(state, source, "engine failure", null);

        IndexShardRoutingTable shard = shardTable(failed);
        assertEquals(shard.toString(), 1, shard.size());
        assertTrue(shard.primaryShard().unassigned());
        assertEquals("no target left recovering from it", 0, shard.getAllInitializingShards().size());
    }

    public void testAFailedRelocatingReplicaLeavesItsTargetRecoveringFromThePrimary() {
        // As RoutingNodes#failShard does: the target is a replica recovering like any other, and has no need of the source.
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED, ShardRoutingState.RELOCATING);
        ShardRouting source = shardTable(state).replicaShards().get(0);
        assertTrue(source.relocating());

        ClusterState failed = LocalShardStateAction.failShard(state, source, "engine failure", null);

        ShardRouting replica = shardTable(failed).replicaShards().get(0);
        assertTrue(replica.toString(), replica.initializing());
        assertNull("no longer a relocation target", replica.relocatingNodeId());
        assertEquals(source.relocatingNodeId(), replica.currentNodeId());
        assertEquals(source.getTargetRelocatingShard().allocationId().getId(), replica.allocationId().getId());
    }

    public void testAFailedRelocationTargetCancelsTheRelocation() {
        // The target is what the target node's IndicesClusterStateService sees and fails. It is not an entry in the table,
        // so failing it as if it were one added an unassigned primary beside the relocating one.
        ClusterState state = state(INDEX, true, ShardRoutingState.RELOCATING);
        ShardRouting source = shardTable(state).primaryShard();
        ShardRouting target = shardTable(state).getByAllocationId(source.allocationId().getRelocationId());
        assertTrue(target.isRelocationTarget());

        ClusterState failed = LocalShardStateAction.failShard(state, target, "recovery failed", null);

        IndexShardRoutingTable shard = shardTable(failed);
        assertEquals("nothing is left unassigned: " + shard, 1, shard.size());
        assertTrue(shard.primaryShard().started());
        assertEquals(source.currentNodeId(), shard.primaryShard().currentNodeId());
        assertEquals(source.allocationId().getId(), shard.primaryShard().allocationId().getId());
    }

    public void testAFailedPrimaryPromotesNothing() {
        // Where this departs from RoutingNodes#failShard on purpose. Promotion comes with a new primary term, and both
        // are for whatever builds the local cluster state to decide, not for this action to infer.
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        ShardRouting replica = shardTable(state).replicaShards().get(0);

        ClusterState failed = LocalShardStateAction.failShard(state, shardTable(state).primaryShard(), "engine failure", null);

        assertTrue(shardTable(failed).primaryShard().unassigned());
        assertEquals("the started replica is untouched, and still a replica", replica, shardTable(failed).replicaShards().get(0));
    }

    public void testAFailedPrimaryFailsTheReplicasRecoveringFromIt() {
        // RoutingNodes refuses a copy that is peer recovering from an unassigned primary ("shard is peer recovering but
        // primary is unassigned"), and it is built from every applied state.
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED);
        ShardRouting started = shardTable(state).replicaShards().stream().filter(ShardRouting::started).findFirst().orElseThrow();

        ClusterState failed = LocalShardStateAction.failShard(state, shardTable(state).primaryShard(), "engine failure", null);

        failed.getRoutingNodes();
        IndexShardRoutingTable shard = shardTable(failed);
        assertTrue(shard.primaryShard().unassigned());
        ShardRouting recovering = shard.replicaShards().stream().filter(copy -> copy.started() == false).findFirst().orElseThrow();
        assertTrue(recovering.unassigned());
        assertEquals(UnassignedInfo.Reason.PRIMARY_FAILED, recovering.unassignedInfo().getReason());
        assertTrue("a started replica is not recovering from it", shard.replicaShards().contains(started));
    }

    public void testAFailedPrimaryCancelsAReplicaRelocationRecoveringFromIt() {
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED, ShardRoutingState.RELOCATING);
        ShardRouting relocating = shardTable(state).replicaShards().get(0);

        ClusterState failed = LocalShardStateAction.failShard(state, shardTable(state).primaryShard(), "engine failure", null);

        failed.getRoutingNodes();
        ShardRouting replica = shardTable(failed).replicaShards().get(0);
        assertTrue("back where it was, and started: " + replica, replica.started());
        assertEquals(relocating.currentNodeId(), replica.currentNodeId());
        assertEquals(relocating.allocationId().getId(), replica.allocationId().getId());
    }

    // ---------------------------------------------------------------- shardStarted

    public void testAnInitializingCopyIsStarted() {
        ClusterState state = state(INDEX, true, ShardRoutingState.INITIALIZING);
        ShardRouting initializing = shardTable(state).primaryShard();

        ClusterState started = started(state, initializing, state.metadata().index(INDEX).primaryTerm(0));

        assertTrue(shardTable(started).primaryShard().started());
        assertEquals(initializing.allocationId().getId(), shardTable(started).primaryShard().allocationId().getId());
    }

    public void testAStartedMessageForACopyThatFailedSinceChangesNothing() {
        // The order IndicesClusterStateService can produce: recovery done and the started message queued, then the shard
        // fails and its failure is applied first. Removing by equality removed nothing and added a started copy beside the
        // unassigned one.
        ClusterState state = state(INDEX, true, ShardRoutingState.INITIALIZING);
        ShardRouting initializing = shardTable(state).primaryShard();
        ClusterState failed = LocalShardStateAction.failShard(state, initializing, "engine failure", null);

        ClusterState started = started(failed, initializing, failed.metadata().index(INDEX).primaryTerm(0));

        assertEquals(shardTable(failed), shardTable(started));
    }

    public void testAStartedMessageForACopyAlreadyStartedChangesNothing() {
        ClusterState state = state(INDEX, true, ShardRoutingState.INITIALIZING);
        ShardRouting initializing = shardTable(state).primaryShard();
        ClusterState once = started(state, initializing, state.metadata().index(INDEX).primaryTerm(0));

        ClusterState twice = started(once, initializing, state.metadata().index(INDEX).primaryTerm(0));

        assertEquals(shardTable(once), shardTable(twice));
    }

    public void testAStartedMessageFromAnEarlierPrimaryTermChangesNothing() {
        ClusterState state = state(INDEX, true, ShardRoutingState.INITIALIZING);
        ShardRouting initializing = shardTable(state).primaryShard();
        long term = state.metadata().index(INDEX).primaryTerm(0);
        ClusterState promoted = ClusterState.builder(state)
            .metadata(
                org.opensearch.cluster.metadata.Metadata.builder(state.metadata())
                    .put(IndexMetadata.builder(state.metadata().index(INDEX)).primaryTerm(0, term + 1))
            )
            .build();

        ClusterState started = started(promoted, initializing, term);

        assertTrue(shardTable(started).primaryShard().initializing());
    }

    public void testAStartedRelocationTargetReplacesItsSource() {
        // The target is derived from the relocating source rather than an entry of its own, so starting it means replacing
        // the source. Removing the target by equality removed nothing and left the relocation in place beside it.
        ClusterState state = state(INDEX, true, ShardRoutingState.RELOCATING);
        ShardRouting source = shardTable(state).primaryShard();
        ShardRouting target = shardTable(state).getByAllocationId(source.allocationId().getRelocationId());

        ClusterState started = started(state, target, state.metadata().index(INDEX).primaryTerm(0));

        IndexShardRoutingTable shard = shardTable(started);
        assertEquals(shard.toString(), 1, shard.size());
        assertTrue(shard.primaryShard().started());
        assertEquals(source.relocatingNodeId(), shard.primaryShard().currentNodeId());
        assertNull(shard.primaryShard().relocatingNodeId());
        assertEquals(target.allocationId().getId(), shard.primaryShard().allocationId().getId());
    }

    public void testLocalShardFailedAppliesTheFailureAndCompletesTheListener() {
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED);
        ShardRouting primary = shardTable(state).primaryShard();
        AtomicReference<ClusterState> applied = new AtomicReference<>();
        LocalShardStateAction action = action(state, applied, null);
        AtomicReference<Boolean> responded = new AtomicReference<>();

        action.localShardFailed(
            primary,
            "engine failure",
            null,
            ActionListener.wrap(r -> responded.set(true), e -> fail(e.toString())),
            state
        );

        assertNotNull("a failure must be applied to the local cluster state", applied.get());
        assertTrue(shardTable(applied.get()).primaryShard().unassigned());
        assertEquals("the listener must be completed", Boolean.TRUE, responded.get());
    }

    public void testLocalShardFailedPassesOnAFailureToApply() {
        ClusterState state = state(INDEX, true, ShardRoutingState.STARTED);
        ShardRouting primary = shardTable(state).primaryShard();
        LocalShardStateAction action = action(state, new AtomicReference<>(), new IllegalStateException("applier closed"));
        AtomicReference<Exception> failure = new AtomicReference<>();

        action.localShardFailed(primary, "engine failure", null, ActionListener.wrap(r -> fail("must not succeed"), failure::set), state);

        assertNotNull("the listener must be told the failure was not applied", failure.get());
        assertEquals("applier closed", failure.get().getMessage());
    }

    private static LocalShardStateAction action(ClusterState state, AtomicReference<ClusterState> applied, Exception applyFailure) {
        ClusterApplierService applier = mock(ClusterApplierService.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Function<ClusterState, ClusterState> update = invocation.getArgument(1);
            ClusterApplier.ClusterApplyListener listener = invocation.getArgument(2);
            if (applyFailure != null) {
                listener.onFailure(invocation.getArgument(0), applyFailure);
            } else {
                applied.set(update.apply(state));
                listener.onSuccess(invocation.getArgument(0));
            }
            return null;
        }).when(applier).updateClusterState(anyString(), any(), any());

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        when(clusterService.getClusterApplierService()).thenReturn(applier);
        return new LocalShardStateAction(clusterService, mock(TransportService.class), null, null, null);
    }

    /** Through the public API, so that these tests describe what the action does rather than how. */
    private static ClusterState started(ClusterState state, ShardRouting shard, long primaryTerm) {
        AtomicReference<ClusterState> applied = new AtomicReference<>();
        action(state, applied, null).shardStarted(shard, primaryTerm, "test", ActionListener.wrap(r -> {}, e -> fail(e.toString())), state);
        assertNotNull("an update must be submitted", applied.get());
        return applied.get();
    }

    private static IndexShardRoutingTable shardTable(ClusterState state) {
        return state.routingTable().index(INDEX).shard(0);
    }

    private static ClusterState withCopy(ClusterState state, ShardRouting existing, ShardRouting replacement) {
        IndexShardRoutingTable shard = new IndexShardRoutingTable.Builder(shardTable(state)).removeShard(existing)
            .addShard(replacement)
            .build();
        return ClusterState.builder(state)
            .routingTable(
                RoutingTable.builder(state.routingTable())
                    .add(org.opensearch.cluster.routing.IndexRoutingTable.builder(shard.shardId().getIndex()).addIndexShard(shard))
                    .build()
            )
            .build();
    }
}

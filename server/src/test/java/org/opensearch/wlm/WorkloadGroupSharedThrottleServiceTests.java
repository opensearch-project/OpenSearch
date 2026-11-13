/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.WorkloadGroupSharedThrottleService.TaskCheckRequest;
import org.opensearch.wlm.WorkloadGroupSharedThrottleService.TaskCheckResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import org.mockito.Mockito;

import static org.opensearch.wlm.SharedThrottleTestPermits.tryAcquire;
import static org.mockito.Mockito.when;

public class WorkloadGroupSharedThrottleServiceTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private TransportService transportService;
    private DiscoveryNode localNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        localNode = new DiscoveryNode(
            "local",
            "local",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        clusterService = Mockito.mock(ClusterService.class);
        threadPool = Mockito.mock(ThreadPool.class);
        transportService = Mockito.mock(TransportService.class);

        ClusterState state = Mockito.mock(ClusterState.class);
        DiscoveryNodes singleDataNode = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        when(state.nodes()).thenReturn(singleDataNode);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
    }

    private WorkloadGroupSharedThrottleService newService() {
        // Single data node => this node owns every bucket => acquire uses the local short-circuit (no real network).
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        // The ring is empty until a cluster-state change populates it (matches production: the constructor does not
        // read cluster state). Deliver a nodesChanged event with the current nodes.
        deliverNodesChanged(service, clusterService.state().nodes());
        return service;
    }

    private void deliverNodesChanged(WorkloadGroupSharedThrottleService service, DiscoveryNodes nodes) {
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        ClusterChangedEvent event = new ClusterChangedEvent("test", current, previous);
        service.clusterChanged(event);
    }

    private static Releasable awaitGrant(WorkloadGroupSharedThrottleService service, String bucket, int limit) {
        AtomicReference<Releasable> permit = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        service.acquireAsync(bucket, limit, SharedThrottleTestPermits.NO_SUCH_TASK, false, ActionListener.wrap(permit::set, failure::set));
        if (failure.get() != null) {
            throw failure.get() instanceof RuntimeException re ? re : new RuntimeException(failure.get());
        }
        return permit.get();
    }

    public void testRingPopulatesWhenNodeSetUnchangedVsPreviousState() {
        // Single-node regression: the first clusterChanged reports no node change, yet the ring must still populate.
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(nodes); // same node set as current -> nodesChanged() == false
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        ClusterChangedEvent event = new ClusterChangedEvent("test", current, previous);
        assertFalse("precondition: this event reports no node change", event.nodesChanged());

        service.clusterChanged(event);

        // shared_limit=1 must now actually enforce (grant then reject), not report the shared tier unavailable.
        assertNotNull(awaitGrant(service, "b", 1));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 1));
    }

    public void testSameIdRestartRebuildsRingWithFreshNode() {
        // A restart keeps the persistent id but gets a new ephemeral id: the ring must rebuild with the fresh node.
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);

        DiscoveryNode first = new DiscoveryNode(
            "n1",
            "n1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        deliverNodesChanged(service, DiscoveryNodes.builder().add(first).localNodeId("n1").build());
        assertSame("ring should hold the first node instance", first, service.ring().ownerFor("b").orElseThrow());

        // Same persistent id "n1", but a brand-new DiscoveryNode instance (fresh ephemeral id + transport address).
        DiscoveryNode restarted = new DiscoveryNode(
            "n1",
            "n1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        assertNotEquals("restarted node must not equal the old instance", first, restarted);
        deliverNodesChanged(service, DiscoveryNodes.builder().add(restarted).localNodeId("n1").build());

        assertSame("ring must have rebuilt with the restarted node instance", restarted, service.ring().ownerFor("b").orElseThrow());
    }

    public void testOnlyNodeChangesRebuildRingAfterFirstEvent() {
        WorkloadGroupSharedThrottleService service = newService();
        ThrottleOwnerSelector initial = service.ring();
        assertEquals(Set.of(localNode), initial.eligibleNodeSet());

        DiscoveryNode other = new DiscoveryNode(
            "other",
            "other",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        DiscoveryNodes withOther = DiscoveryNodes.builder().add(localNode).add(other).localNodeId("local").build();
        // Deliberately inconsistent with the ring (a real applier never produces this): an event reporting no node change
        // whose state nevertheless holds another eligible node, so a skipped scan is observable as an unchanged ring.
        ClusterState state = Mockito.mock(ClusterState.class);
        when(state.nodes()).thenReturn(withOther);
        ClusterChangedEvent unchanged = new ClusterChangedEvent("test", state, state);
        assertFalse("precondition: this event reports no node change", unchanged.nodesChanged());
        service.clusterChanged(unchanged);
        assertSame("an event without node changes must not rescan or rebuild the ring", initial, service.ring());

        deliverNodesChanged(service, withOther);
        assertEquals("a node change must rebuild the ring", Set.of(localNode, other), service.ring().eligibleNodeSet());
    }

    public void testLocalOwnerGrantsThenDeniesAtLimit() {
        WorkloadGroupSharedThrottleService service = newService();
        Releasable p1 = awaitGrant(service, "b", 1);
        assertNotNull(p1);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 1));
        // release frees the shared slot
        p1.close();
        assertNotNull(awaitGrant(service, "b", 1));
    }

    public void testDoubleCloseReleasesOnce() {
        WorkloadGroupSharedThrottleService service = newService();
        Releasable p = awaitGrant(service, "b", 2);
        assertNotNull(awaitGrant(service, "b", 2)); // second slot
        p.close();
        p.close(); // must not double-release
        // one slot still held, so only one more grant is available
        assertNotNull(awaitGrant(service, "b", 2));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 2));
    }

    public void testHasEligibleOwnerOnlyWhileTheRingHasADataNode() {
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        assertFalse("no cluster state applied yet", service.hasEligibleOwner());

        DiscoveryNodes withData = DiscoveryNodes.builder().add(dataNode("data")).localNodeId("data").build();
        deliverNodesChanged(service, DiscoveryNodes.EMPTY_NODES, withData);
        assertTrue(service.hasEligibleOwner());

        DiscoveryNodes withoutData = DiscoveryNodes.builder().add(coordinatingOnlyNode("coord")).localNodeId("coord").build();
        deliverNodesChanged(service, withData, withoutData);
        assertFalse("a cluster without data nodes has no owner", service.hasEligibleOwner());
    }

    public void testEmptyRingIsUnavailable() {
        // Cluster with no eligible data node (coordinating/manager-only) => empty ring => unavailable. WorkloadGroupService
        // checks hasEligibleOwner first and treats shared_limit as unset, so this covers a ring that empties after that check.
        DiscoveryNode managerOnly = new DiscoveryNode(
            "m",
            "m",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        ClusterState state = Mockito.mock(ClusterState.class);
        when(state.nodes()).thenReturn(DiscoveryNodes.builder().add(managerOnly).localNodeId("m").build());
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(managerOnly);

        WorkloadGroupSharedThrottleService service = newService();
        AtomicReference<Exception> failure = new AtomicReference<>();
        service.acquireAsync(
            "b",
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            ActionListener.wrap(p -> fail("must not admit: " + p), failure::set)
        );
        assertNotNull("listener must be invoked inline", failure.get());
        assertTrue("empty ring must report the shared tier unavailable", WorkloadGroupSharedThrottleService.isUnavailable(failure.get()));
    }

    public void testDisconnectedOwnerIsUnavailableWithoutSendingRequest() {
        // Ring owner is a remote data node (not the local node), so this is NOT the local short-circuit path.
        DiscoveryNode remote = new DiscoveryNode(
            "remote",
            "remote",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        ClusterState state = Mockito.mock(ClusterState.class);
        // Only the remote node is a data node, so every bucket hashes to it; local is a coordinating-only node.
        when(state.nodes()).thenReturn(DiscoveryNodes.builder().add(remote).localNodeId("local").build());
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(localNode);
        // The owner is known-disconnected.
        when(transportService.nodeConnected(remote)).thenReturn(false);

        WorkloadGroupSharedThrottleService service = newService();
        AtomicReference<Exception> failure = new AtomicReference<>();
        service.acquireAsync(
            "b",
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            ActionListener.wrap(p -> fail("must not admit: " + p), failure::set)
        );

        // An inline result proves the nodeConnected() pre-check fired: the mock cannot intercept the final sendRequest,
        // so sending would throw rather than complete the listener.
        assertNotNull("listener must be invoked inline (no RTT to a dead node)", failure.get());
        assertTrue("disconnected owner -> shared tier unavailable", WorkloadGroupSharedThrottleService.isUnavailable(failure.get()));
    }

    public void testAcquirePermitRequestSerializationRoundTrip() throws Exception {
        for (boolean wantsQueue : new boolean[] { false, true }) {
            WorkloadGroupSharedThrottleService.AcquirePermitRequest original = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                "grp1:username:alice",
                42,
                "permit-xyz",
                123_456_789L,
                "coordinator-ephemeral-id",
                randomLongBetween(0, Long.MAX_VALUE),
                wantsQueue
            );
            WorkloadGroupSharedThrottleService.AcquirePermitRequest copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.AcquirePermitRequest::new
            );
            assertEquals(original.bucketKey, copy.bucketKey);
            assertEquals(original.sharedLimit, copy.sharedLimit);
            assertEquals(original.permitId, copy.permitId);
            assertEquals(
                "the owner applies its own TTL, whatever the sender held",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                copy.ttlNanos
            );
            assertEquals(original.coordinatorId, copy.coordinatorId);
            assertEquals(original.coordinatorTaskId, copy.coordinatorTaskId);
            assertEquals(wantsQueue, copy.wantsQueue);
            // The ephemeral id is the coordinator's only identity on the wire: no persistent node id is sent.
            assertEquals(fullAcquireRequestBody().keySet(), writtenRequestKeys(original));
        }
    }

    public void testAcquirePermitResponseSerializationRoundTrip() throws Exception {
        for (WorkloadGroupSharedThrottleService.AcquirePermitResponse original : List.of(
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.GRANTED,
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED,
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED_AND_REGISTERED,
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.NOT_OWNER
        )) {
            WorkloadGroupSharedThrottleService.AcquirePermitResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.AcquirePermitResponse::new
            );
            assertEquals(original.granted, copy.granted);
            assertEquals(original.notOwner, copy.notOwner);
            assertEquals(original.registered, copy.registered);
        }
    }

    /** Only the explicit registered-denial response may claim a registration (never silently "registered"). */
    public void testAcquireResponseConstantsOnlyRegisterOnARegisteredDenial() {
        assertFalse(WorkloadGroupSharedThrottleService.AcquirePermitResponse.GRANTED.registered);
        assertFalse(WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED.registered);
        assertFalse(WorkloadGroupSharedThrottleService.AcquirePermitResponse.NOT_OWNER.registered);
        assertTrue(WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED_AND_REGISTERED.registered);
    }

    public void testFormerOwnerRefusesAcquireAndKeepsItsRecords() {
        DiscoveryNode newOwner = dataNode("other");
        DiscoveryNodes moved = DiscoveryNodes.builder().add(localNode).add(newOwner).localNodeId("local").build();
        String key = bucketOwnedBy(serviceWithRing(moved), newOwner);

        // Single data node: this node owns the bucket and grants a permit that is still in flight.
        WorkloadGroupSharedThrottleService service = newService();
        assertTrue(service.handleAcquire(acquireRequest(key, "held")).granted);

        // The bucket moves to another node; a coordinator still on the old ring keeps routing acquires here.
        deliverNodesChanged(service, moved);
        WorkloadGroupSharedThrottleService.AcquirePermitResponse response = service.handleAcquire(acquireRequest(key, "stale"));
        assertFalse("a former owner must not grant against its stale counter", response.granted);
        assertTrue("the refusal must say not_owner, not denied", response.notOwner);
        assertEquals("existing records drain by release or TTL, never cleared", 1, service.tracker().inFlight(key));
    }

    public void testJoiningNodeGrantsBeforeItsFirstClusterStateInsteadOfRefusing() {
        // Coordinators that applied this node's join route its buckets here before it applies its own first cluster state.
        WorkloadGroupSharedThrottleService joining = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        WorkloadGroupSharedThrottleService.AcquirePermitResponse early = joining.handleAcquire(acquireRequest("b", "early"));
        assertTrue("an empty, never-initialized ring must not refuse as not_owner", early.granted);
        assertEquals(1, joining.tracker().inFlight("b"));

        // Once its ring is built, ownership is enforced again, and the early record stays counted until it drains.
        DiscoveryNode other = dataNode("other");
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).add(other).localNodeId("local").build();
        String notOwned = bucketOwnedBy(serviceWithRing(nodes), other);
        deliverNodesChanged(joining, nodes);
        assertTrue(joining.handleAcquire(acquireRequest(notOwned, "after")).notOwner);
        assertEquals(0, joining.tracker().inFlight(notOwned));
        assertEquals(1, joining.tracker().inFlight("b"));
    }

    public void testOwnershipLostAfterTrackerAcquireReturnsThePermit() {
        DiscoveryNode newOwner = dataNode("other");
        DiscoveryNodes moved = DiscoveryNodes.builder().add(localNode).add(newOwner).localNodeId("local").build();
        String key = bucketOwnedBy(serviceWithRing(moved), newOwner); // owned locally now, by newOwner after the move

        // The tracker reads its clock once per acquire, after the ownership check and before it grants, so a clock that
        // moves the bucket on that read lands the move between the grant and the post-acquire re-check.
        // Owner side (handleAcquire).
        RingMovingClock ownerClock = new RingMovingClock();
        WorkloadGroupSharedThrottleService owner = newService(ownerClock);
        ownerClock.onNextRead(() -> deliverNodesChanged(owner, moved));
        Mockito.clearInvocations(clusterService);
        WorkloadGroupSharedThrottleService.AcquirePermitResponse response = owner.handleAcquire(acquireRequest(key, "racy"));
        assertTrue("the bucket must move during the tracker acquire", ownerClock.fired());
        // Once for the ownership check and once for the post-acquire re-check; a refusal before the grant reads it once.
        Mockito.verify(clusterService, Mockito.times(2)).localNode();
        assertTrue(response.notOwner);
        assertEquals("the permit acquired before the swap must be returned", 0, owner.tracker().inFlight(key));

        // Coordinator-side local short-circuit (acquireAsync).
        RingMovingClock localClock = new RingMovingClock();
        WorkloadGroupSharedThrottleService local = newService(localClock);
        localClock.onNextRead(() -> deliverNodesChanged(local, moved));
        Mockito.clearInvocations(clusterService);
        AtomicReference<Exception> failure = new AtomicReference<>();
        local.acquireAsync(
            key,
            5,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            ActionListener.wrap(p -> fail("must not admit: " + p), failure::set)
        );
        assertTrue("the bucket must move during the tracker acquire", localClock.fired());
        // Once to build the request and once for the post-acquire re-check.
        Mockito.verify(clusterService, Mockito.times(2)).localNode();
        assertTrue(
            "lost ownership mid-acquire must report the shared tier unavailable",
            WorkloadGroupSharedThrottleService.isUnavailable(failure.get())
        );
        assertEquals(0, local.tracker().inFlight(key));
    }

    private WorkloadGroupSharedThrottleService newService(LongSupplier clock) {
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
            clusterService,
            threadPool,
            transportService,
            WorkloadGroupSharedThrottleService.ACQUIRE_TIMEOUT,
            clock
        );
        deliverNodesChanged(service, clusterService.state().nodes());
        return service;
    }

    // A clock that runs a one-shot action on its next read; always reads zero.
    private static final class RingMovingClock implements LongSupplier {
        private final AtomicReference<Runnable> next = new AtomicReference<>();
        private final AtomicBoolean fired = new AtomicBoolean();

        void onNextRead(Runnable action) {
            next.set(action);
        }

        boolean fired() {
            return fired.get();
        }

        @Override
        public long getAsLong() {
            Runnable action = next.getAndSet(null);
            if (action != null) {
                action.run();
                fired.set(true);
            }
            return 0L;
        }
    }

    private static WorkloadGroupSharedThrottleService.AcquirePermitRequest acquireRequest(String key, String permitId) {
        return acquireRequest(key, permitId, "coord");
    }

    private static WorkloadGroupSharedThrottleService.AcquirePermitRequest acquireRequest(
        String key,
        String permitId,
        String coordinatorId
    ) {
        return new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            key,
            5,
            permitId,
            WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
            coordinatorId,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false
        );
    }

    // An acquire as the given coordinator sends it; wantsQueue asks the owner to register it as a waiter on a denial.
    private static WorkloadGroupSharedThrottleService.AcquirePermitRequest acquireRequest(
        String key,
        int sharedLimit,
        String permitId,
        DiscoveryNode coordinator,
        boolean wantsQueue
    ) {
        return new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            key,
            sharedLimit,
            permitId,
            WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
            coordinator.getEphemeralId(),
            SharedThrottleTestPermits.NO_SUCH_TASK,
            wantsQueue
        );
    }

    private static DiscoveryNode coordinatingOnlyNode(String id) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), Set.of(), Version.CURRENT);
    }

    private void deliverNodesChanged(WorkloadGroupSharedThrottleService service, DiscoveryNodes previousNodes, DiscoveryNodes nodes) {
        service.clusterChanged(nodesChangedEvent(previousNodes, nodes));
    }

    private static ClusterChangedEvent nodesChangedEvent(DiscoveryNodes previousNodes, DiscoveryNodes nodes) {
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(previousNodes);
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        return new ClusterChangedEvent("test", current, previous);
    }

    public void testCheckQueueOverflowExtendsPermitsInsteadOfStrandingThem() {
        AtomicLong now = new AtomicLong();
        // An executor that never runs the drain, so the queue fills up.
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(Mockito.mock(ExecutorService.class));
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
            clusterService,
            threadPool,
            transportService,
            WorkloadGroupSharedThrottleService.ACQUIRE_TIMEOUT,
            now::get
        );
        deliverNodesChanged(service, clusterService.state().nodes());
        int permits = WorkloadGroupSharedThrottleService.MAX_QUEUED_CHECKS + 1;
        for (int i = 0; i < permits; i++) {
            assertTrue(
                tryAcquire(
                    service.tracker(),
                    "b",
                    permits,
                    "p" + i,
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    localNode.getEphemeralId(),
                    i + 1
                )
            );
        }

        now.addAndGet(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1);
        service.tracker().sweepExpired();
        assertEquals(WorkloadGroupSharedThrottleService.MAX_QUEUED_CHECKS, service.queuedChecks().size());
        assertEquals("the overflowing permit stays counted", permits, service.tracker().inFlight("b"));

        // The overflowing permit was extended rather than left pending: once the extension passes, a scan collects it again.
        service.queuedChecks().clear();
        now.addAndGet(WorkloadGroupSharedThrottleService.FAILED_CHECK_TTL.nanos() + 1);
        service.tracker().sweepExpired();
        assertEquals(1, service.queuedChecks().size());
    }

    public void testRemovedCoordinatorPermitsArePurged() {
        // Coordinating-only peers: the ring stays on the single local data node, so this node owns every bucket.
        DiscoveryNode leaving = coordinatingOnlyNode("leaving");
        DiscoveryNode staying = coordinatingOnlyNode("staying");
        DiscoveryNodes before = DiscoveryNodes.builder().add(localNode).add(leaving).add(staying).localNodeId("local").build();
        WorkloadGroupSharedThrottleService service = newService();
        deliverNodesChanged(service, clusterService.state().nodes(), before);

        assertTrue(service.handleAcquire(acquireRequest("b", "from-leaving-1", leaving.getEphemeralId())).granted);
        assertTrue(service.handleAcquire(acquireRequest("c", "from-leaving-2", leaving.getEphemeralId())).granted);
        assertTrue(service.handleAcquire(acquireRequest("b", "from-staying", staying.getEphemeralId())).granted);
        assertTrue(service.handleAcquire(acquireRequest("b", "from-other", "other-coordinator")).granted);
        assertNotNull(awaitGrant(service, "b", 5)); // local coordinator

        deliverNodesChanged(service, before, DiscoveryNodes.builder().add(localNode).add(staying).localNodeId("local").build());
        assertEquals("only the departed coordinator's permits are purged", 3, service.tracker().inFlight("b"));
        assertEquals("its permits in every bucket are purged", 0, service.tracker().inFlight("c"));
    }

    public void testSameIdRestartPurgesTheOldIncarnationsPermits() {
        DiscoveryNode firstIncarnation = coordinatingOnlyNode("restarting");
        DiscoveryNode secondIncarnation = coordinatingOnlyNode("restarting"); // same persistent id, new ephemeral id
        assertNotEquals(firstIncarnation.getEphemeralId(), secondIncarnation.getEphemeralId());
        DiscoveryNodes before = DiscoveryNodes.builder().add(localNode).add(firstIncarnation).localNodeId("local").build();
        DiscoveryNodes after = DiscoveryNodes.builder().add(localNode).add(secondIncarnation).localNodeId("local").build();
        WorkloadGroupSharedThrottleService service = newService();
        deliverNodesChanged(service, clusterService.state().nodes(), before);

        assertTrue(service.handleAcquire(acquireRequest("b", "old", firstIncarnation.getEphemeralId())).granted);
        // The new incarnation can reach the owner before the owner applies the restart: its permit must survive the purge.
        assertTrue(
            tryAcquire(
                service.tracker(),
                "b",
                5,
                "new",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                secondIncarnation.getEphemeralId()
            )
        );

        ClusterChangedEvent restart = nodesChangedEvent(before, after);
        assertTrue("precondition: the restart must register as a node change", restart.nodesChanged());
        service.clusterChanged(restart);
        assertEquals("only the old incarnation's permit is purged", 1, service.tracker().inFlight("b"));
        assertEquals(
            "the survivor belongs to the new incarnation",
            1,
            service.tracker().releaseAllFrom(Set.of(secondIncarnation.getEphemeralId()))
        );
    }

    public void testTaskCheckUsesExistingTaskManagerAndCountsCancelledTasks() {
        TaskManager taskManager = Mockito.mock(TaskManager.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        SearchTask task = new SearchTask(42, "transport", "indices:data/read/search", () -> "search", TaskId.EMPTY_TASK_ID, Map.of());
        task.cancel("still stopping");
        when(taskManager.getTask(42)).thenReturn(task);
        WorkloadGroupSharedThrottleService service = newService();
        TaskCheckResponse response = service.checkTasks(new TaskCheckRequest(localNode.getEphemeralId(), new long[] { 41, 42, 43 }));
        assertArrayEquals(new boolean[] { false, true, false }, response.active);
        Mockito.verify(taskManager).getTask(41);
        Mockito.verify(taskManager).getTask(42);
        Mockito.verify(taskManager).getTask(43);
        Mockito.verifyNoMoreInteractions(taskManager);
    }

    public void testTaskCheckRejectsAnotherCoordinatorIncarnation() {
        TaskManager taskManager = Mockito.mock(TaskManager.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        WorkloadGroupSharedThrottleService service = newService();
        expectThrows(IllegalStateException.class, () -> service.checkTasks(new TaskCheckRequest("old-incarnation", new long[] { 42 })));
        Mockito.verifyNoInteractions(taskManager);
    }

    public void testTaskCheckRequestSerializationRoundTrip() throws Exception {
        long[] ids = new long[WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = randomLongBetween(0, Long.MAX_VALUE);
        }
        TaskCheckRequest original = new TaskCheckRequest("coordinator", ids);
        TaskCheckRequest copy = copyWriteable(original, writableRegistry(), TaskCheckRequest::new);
        assertEquals(original.coordinatorId, copy.coordinatorId);
        assertArrayEquals(original.taskIds, copy.taskIds);
    }

    public void testTaskCheckResponseSerializationRoundTrip() throws Exception {
        boolean[] active = new boolean[WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK];
        for (int i = 0; i < active.length; i++) {
            active[i] = randomBoolean();
        }
        TaskCheckResponse original = new TaskCheckResponse(active);
        TaskCheckResponse copy = copyWriteable(original, writableRegistry(), TaskCheckResponse::new);
        assertArrayEquals(original.active, copy.active);
    }

    public void testTaskCheckRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(TaskCheckRequest.KEY_COORDINATOR, "coordinator");
        body.put(TaskCheckRequest.KEY_TASK_IDS, List.of(7L, 8L));
        body.put("future_include_cancelled", true);
        body.put("future_list", List.of("a", "b"));
        StreamInput in = bodyBytes(body, true);
        TaskCheckRequest parsed = new TaskCheckRequest(in);
        assertEquals("coordinator", parsed.coordinatorId);
        assertArrayEquals(new long[] { 7L, 8L }, parsed.taskIds);
        assertEquals("unknown keys must be consumed, not left as trailing bytes", 0, in.available());
    }

    public void testTaskCheckResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(TaskCheckResponse.KEY_ACTIVE, List.of(false, true));
        body.put("future_cancelled", List.of(false, false));
        StreamInput in = bodyBytes(body, false);
        assertArrayEquals(new boolean[] { false, true }, new TaskCheckResponse(in).active);
        assertEquals(0, in.available());
    }

    public void testTaskCheckMessagesAreStrict() throws Exception {
        Map<String, Object> full = new HashMap<>();
        full.put(TaskCheckRequest.KEY_COORDINATOR, "coordinator");
        full.put(TaskCheckRequest.KEY_TASK_IDS, List.of(7L));
        for (String omitted : full.keySet()) {
            Map<String, Object> partial = new HashMap<>(full);
            partial.remove(omitted);
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> new TaskCheckRequest(bodyBytes(partial, true)));
            assertTrue(e.getMessage(), e.getMessage().contains(omitted));
        }
        expectThrows(IllegalStateException.class, () -> new TaskCheckResponse(bodyBytes(new HashMap<>(), false)));
    }

    public void testTaskCheckMessagesRejectElementsOfTheWrongType() throws Exception {
        Map<String, Object> request = new HashMap<>();
        request.put(TaskCheckRequest.KEY_COORDINATOR, "coordinator");
        request.put(TaskCheckRequest.KEY_TASK_IDS, List.of(7L, "eight"));
        expectThrows(IllegalStateException.class, () -> new TaskCheckRequest(bodyBytes(request, true)));
        Map<String, Object> response = new HashMap<>();
        response.put(TaskCheckResponse.KEY_ACTIVE, List.of(true, 1));
        expectThrows(IllegalStateException.class, () -> new TaskCheckResponse(bodyBytes(response, false)));
    }

    public void testTaskCheckRejectsOversizedPayloads() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            new TaskCheckRequest("coordinator", new long[WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK + 1]).writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                expectThrows(IOException.class, () -> new TaskCheckRequest(in));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            new TaskCheckResponse(new boolean[WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK + 1]).writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                expectThrows(IOException.class, () -> new TaskCheckResponse(in));
            }
        }
    }

    public void testReleasePermitResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WorkloadGroupSharedThrottleService.ReleasePermitResponse.INSTANCE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                new WorkloadGroupSharedThrottleService.ReleasePermitResponse(in);
                assertEquals(0, in.available());
            }
        }
        Map<String, Object> body = new HashMap<>();
        body.put("future_was_known", true);
        StreamInput in = bodyBytes(body, false);
        new WorkloadGroupSharedThrottleService.ReleasePermitResponse(in);
        assertEquals("unknown keys must be consumed, not left as trailing bytes", 0, in.available());
    }

    private static DiscoveryNode dataNode(String id) {
        return new DiscoveryNode(
            id,
            id,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }

    private WorkloadGroupSharedThrottleService serviceWithRing(DiscoveryNodes nodes) {
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        deliverNodesChanged(service, nodes);
        return service;
    }

    private static String bucketOwnedBy(WorkloadGroupSharedThrottleService service, DiscoveryNode owner) {
        for (int i = 0; i < 10000; i++) {
            String candidate = "bucket-" + i;
            if (service.ring().ownerFor(candidate).filter(owner::equals).isPresent()) {
                return candidate;
            }
        }
        throw new AssertionError("no bucket owned by " + owner);
    }

    public void testReleasePermitRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.ReleasePermitRequest original = new WorkloadGroupSharedThrottleService.ReleasePermitRequest(
            "grp1:group",
            "permit-abc"
        );
        WorkloadGroupSharedThrottleService.ReleasePermitRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.ReleasePermitRequest::new
        );
        assertEquals(original.bucketKey, copy.bucketKey);
        assertEquals(original.permitId, copy.permitId);

        // A release only names the permit: an unused grant is returned by the GRANT's reply, not by a flagged RELEASE.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in); // TransportRequest preamble
                assertEquals(
                    Set.of(
                        WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_BUCKET,
                        WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_PERMIT_ID
                    ),
                    in.readMap(StreamInput::readString, StreamInput::readGenericValue).keySet()
                );
            }
        }
    }

    public void testGrantPermitRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.GrantPermitRequest original = new WorkloadGroupSharedThrottleService.GrantPermitRequest(
            "grp1:group",
            "permit-g"
        );
        WorkloadGroupSharedThrottleService.GrantPermitRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.GrantPermitRequest::new
        );
        assertEquals("grp1:group", copy.bucketKey);
        assertEquals("permit-g", copy.permitId);

        // The coordinator needs no limit, so the grant carries none: a dead strict field would bind every future owner.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in); // TransportRequest preamble
                assertEquals(
                    Set.of(
                        WorkloadGroupSharedThrottleService.GrantPermitRequest.KEY_BUCKET,
                        WorkloadGroupSharedThrottleService.GrantPermitRequest.KEY_PERMIT_ID
                    ),
                    in.readMap(StreamInput::readString, StreamInput::readGenericValue).keySet()
                );
            }
        }
    }

    public void testGrantPermitResponseSerializationRoundTrip() throws Exception {
        final long taskId = randomLongBetween(1, Long.MAX_VALUE);
        for (OptionalLong admitted : List.of(OptionalLong.empty(), OptionalLong.of(taskId))) {
            WorkloadGroupSharedThrottleService.GrantPermitResponse original = new WorkloadGroupSharedThrottleService.GrantPermitResponse(
                admitted
            );
            WorkloadGroupSharedThrottleService.GrantPermitResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.GrantPermitResponse::new
            );
            assertEquals(admitted, copy.admittedTaskId);

            // Always written, as a list of zero or one ids: "nothing admitted" is an empty list, never a sentinel id.
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    assertEquals(
                        Map.of(
                            WorkloadGroupSharedThrottleService.GrantPermitResponse.KEY_ADMITTED_TASK_IDS,
                            admitted.isPresent() ? List.of(taskId) : List.of()
                        ),
                        in.readMap(StreamInput::readString, StreamInput::readGenericValue)
                    );
                }
            }
        }
        assertEquals(OptionalLong.empty(), WorkloadGroupSharedThrottleService.GrantPermitResponse.NOT_ADMITTED.admittedTaskId);
    }

    public void testRegisterWaiterRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.RegisterWaiterRequest original = new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(
            "grp1:group",
            "coordinator-ephemeral-id"
        );
        WorkloadGroupSharedThrottleService.RegisterWaiterRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.RegisterWaiterRequest::new
        );
        assertEquals(original.bucketKey, copy.bucketKey);
        assertEquals(original.coordinatorId, copy.coordinatorId);
        // The sender is named by its ephemeral id, as on an acquire: no persistent node id is sent.
        assertEquals(
            Set.of(
                WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_BUCKET,
                WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_COORDINATOR
            ),
            writtenRequestKeys(original)
        );
        assertEquals(
            "the same key as the acquire's",
            WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_COORDINATOR,
            WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_COORDINATOR
        );
    }

    public void testRegisterWaiterResponseSerializationRoundTrip() throws Exception {
        for (boolean registered : new boolean[] { true, false }) {
            WorkloadGroupSharedThrottleService.RegisterWaiterResponse original =
                new WorkloadGroupSharedThrottleService.RegisterWaiterResponse(registered);
            WorkloadGroupSharedThrottleService.RegisterWaiterResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.RegisterWaiterResponse::new
            );
            assertEquals(registered, copy.registered);
        }
    }

    // Serde tolerance: bodies are written by hand to stand in for another build's writer.

    private static StreamInput bodyBytes(Map<String, Object> body, boolean withTaskPreamble) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        if (withTaskPreamble) {
            TaskId.EMPTY_TASK_ID.writeTo(out); // TransportRequest preamble that super(in) consumes; responses have none
        }
        out.writeMap(body, StreamOutput::writeString, StreamOutput::writeGenericValue);
        return out.bytes().streamInput();
    }

    // The body keys a request writes.
    private static Set<String> writtenRequestKeys(TransportRequest request) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in); // TransportRequest preamble
                return in.readMap(StreamInput::readString, StreamInput::readGenericValue).keySet();
            }
        }
    }

    // Every key a current writer sends, so tolerance and strictness tests start from a complete body.
    private static Map<String, Object> fullAcquireRequestBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_COORDINATOR, "eph-coordinator");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_COORDINATOR_TASK_ID, 7L);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_WANTS_QUEUE, true);
        return body;
    }

    private static Map<String, Object> fullAcquireResponseBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_GRANTED, false);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_NOT_OWNER, false);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_REGISTERED, true);
        return body;
    }

    private static Map<String, Object> fullReleaseRequestBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_PERMIT_ID, "permit-abc");
        return body;
    }

    private static Map<String, Object> fullGrantRequestBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.GrantPermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.GrantPermitRequest.KEY_PERMIT_ID, "permit-g");
        return body;
    }

    private static Map<String, Object> fullGrantResponseBody(List<?> admittedTaskIds) {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.GrantPermitResponse.KEY_ADMITTED_TASK_IDS, admittedTaskIds);
        return body;
    }

    private static Map<String, Object> fullRegisterWaiterRequestBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_COORDINATOR, "eph-coordinator");
        return body;
    }

    private static Map<String, Object> fullRegisterWaiterResponseBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.RegisterWaiterResponse.KEY_REGISTERED, true);
        return body;
    }

    // Adds fields a newer peer might send, of every type readGenericValue knows.
    private static Map<String, Object> withFutureFields(Map<String, Object> body) {
        Map<String, Object> extended = new HashMap<>(body);
        extended.put("future_flag", true);
        extended.put("future_reason", "at_limit");
        extended.put("future_retry_after_millis", 250L);
        extended.put("future_list", List.of("a", "b"));
        extended.put("future_null", null);
        return extended;
    }

    // Every key of a complete body is required: dropping any one fails decoding, naming it, rather than being guessed.
    private static void assertEveryKeyRequired(
        Map<String, Object> full,
        boolean request,
        CheckedFunction<StreamInput, ?, IOException> reader
    ) throws IOException {
        for (String omitted : full.keySet()) {
            Map<String, Object> partial = new HashMap<>(full);
            partial.remove(omitted);
            StreamInput in = bodyBytes(partial, request);
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> reader.apply(in));
            assertTrue(e.getMessage(), e.getMessage().contains(omitted));
        }
    }

    public void testAcquirePermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = withFutureFields(fullAcquireRequestBody());
        body.put("ttl_nanos", 1_000L); // not part of the format: the owner applies its own TTL
        body.put("requesting_node_id", "node-coordinator"); // not part of the format either: an older dev build sent it
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.AcquirePermitRequest parsed = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals(5, parsed.sharedLimit);
        assertEquals("permit-abc", parsed.permitId);
        assertEquals(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, parsed.ttlNanos);
        assertEquals("eph-coordinator", parsed.coordinatorId);
        assertEquals(7L, parsed.coordinatorTaskId);
        assertTrue(parsed.wantsQueue);
        assertEquals("unknown keys must be consumed, not left as trailing bytes", 0, in.available());
    }

    public void testAcquirePermitRequestIsStrict() throws Exception {
        // Every field ships with throttling itself, so a missing one fails decoding (reported unavailable).
        assertEveryKeyRequired(fullAcquireRequestBody(), true, WorkloadGroupSharedThrottleService.AcquirePermitRequest::new);
    }

    public void testAcquirePermitRequestAcceptsAWidenedNumericType() throws Exception {
        // A peer that widens shared_limit to a long must still interoperate.
        Map<String, Object> body = fullAcquireRequestBody();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5L);
        WorkloadGroupSharedThrottleService.AcquirePermitRequest parsed = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            bodyBytes(body, true)
        );
        assertEquals(5, parsed.sharedLimit);
    }

    public void testAcquirePermitResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        StreamInput in = bodyBytes(withFutureFields(fullAcquireResponseBody()), false); // TransportResponse has no TaskId preamble
        WorkloadGroupSharedThrottleService.AcquirePermitResponse parsed = new WorkloadGroupSharedThrottleService.AcquirePermitResponse(in);
        assertFalse(parsed.granted);
        assertFalse(parsed.notOwner);
        assertTrue(parsed.registered);
        assertEquals(0, in.available());
    }

    public void testAcquirePermitResponseIsStrict() throws Exception {
        // In particular a missing registered key is rejected, never read as "not registered".
        assertEveryKeyRequired(fullAcquireResponseBody(), false, WorkloadGroupSharedThrottleService.AcquirePermitResponse::new);
    }

    public void testReleasePermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = withFutureFields(fullReleaseRequestBody());
        body.put("shared_limit", 5);
        // Not part of the format: a release carries no deregistration signal, whoever sends it.
        body.put("requesting_node_id", "node-1");
        body.put("unused_grant", true);
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.ReleasePermitRequest parsed = new WorkloadGroupSharedThrottleService.ReleasePermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals("permit-abc", parsed.permitId);
        assertEquals(0, in.available());
    }

    public void testReleasePermitRequestIsStrictLikeTheOthers() throws Exception {
        assertEveryKeyRequired(fullReleaseRequestBody(), true, WorkloadGroupSharedThrottleService.ReleasePermitRequest::new);
    }

    public void testGrantPermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = withFutureFields(fullGrantRequestBody());
        body.put("shared_limit", 4); // not part of the format: the coordinator needs no limit
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.GrantPermitRequest parsed = new WorkloadGroupSharedThrottleService.GrantPermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals("permit-g", parsed.permitId);
        assertEquals(0, in.available());
    }

    public void testGrantPermitRequestIsStrict() throws Exception {
        assertEveryKeyRequired(fullGrantRequestBody(), true, WorkloadGroupSharedThrottleService.GrantPermitRequest::new);
    }

    public void testGrantPermitResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        for (List<?> admittedTaskIds : List.of(List.of(), List.of(7L))) {
            StreamInput in = bodyBytes(withFutureFields(fullGrantResponseBody(admittedTaskIds)), false);
            WorkloadGroupSharedThrottleService.GrantPermitResponse parsed = new WorkloadGroupSharedThrottleService.GrantPermitResponse(in);
            assertEquals(admittedTaskIds.isEmpty() ? OptionalLong.empty() : OptionalLong.of(7L), parsed.admittedTaskId);
            assertEquals("unknown keys must be consumed, not left as trailing bytes", 0, in.available());
        }
    }

    public void testGrantPermitResponseIsStrict() throws Exception {
        // A missing outcome is rejected, never read as "nothing admitted" (which would reclaim a slot a search holds).
        assertEveryKeyRequired(fullGrantResponseBody(List.of(7L)), false, WorkloadGroupSharedThrottleService.GrantPermitResponse::new);
        assertEveryKeyRequired(fullGrantResponseBody(List.of()), false, WorkloadGroupSharedThrottleService.GrantPermitResponse::new);
    }

    public void testGrantPermitResponseRejectsMoreThanOneAdmittedTask() throws Exception {
        // One grant admits at most one request.
        IOException e = expectThrows(
            IOException.class,
            () -> new WorkloadGroupSharedThrottleService.GrantPermitResponse(bodyBytes(fullGrantResponseBody(List.of(7L, 8L)), false))
        );
        assertTrue(e.getMessage(), e.getMessage().contains(WorkloadGroupSharedThrottleService.GrantPermitResponse.KEY_ADMITTED_TASK_IDS));
    }

    public void testGrantPermitResponseRejectsAMalformedOutcome() throws Exception {
        for (Object malformed : List.<Object>of(7L, "7", List.of("seven"), List.of(true))) {
            Map<String, Object> body = new HashMap<>();
            body.put(WorkloadGroupSharedThrottleService.GrantPermitResponse.KEY_ADMITTED_TASK_IDS, malformed);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new WorkloadGroupSharedThrottleService.GrantPermitResponse(bodyBytes(body, false))
            );
            assertTrue(
                e.getMessage(),
                e.getMessage().contains(WorkloadGroupSharedThrottleService.GrantPermitResponse.KEY_ADMITTED_TASK_IDS)
            );
        }
    }

    public void testRegisterWaiterRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = withFutureFields(fullRegisterWaiterRequestBody());
        body.put("requesting_node_id", "node-1"); // not part of the format: an older dev build sent it
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.RegisterWaiterRequest parsed = new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals("eph-coordinator", parsed.coordinatorId);
        assertEquals(0, in.available());
    }

    public void testRegisterWaiterRequestIsStrict() throws Exception {
        assertEveryKeyRequired(fullRegisterWaiterRequestBody(), true, WorkloadGroupSharedThrottleService.RegisterWaiterRequest::new);
        // An older dev build named itself by its persistent id only: that id is ignored, and the missing key is reported.
        Map<String, Object> older = new HashMap<>();
        older.put(WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_BUCKET, "grp1:group");
        older.put("requesting_node_id", "node-1");
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(bodyBytes(older, true))
        );
        assertTrue(e.getMessage(), e.getMessage().contains(WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_COORDINATOR));
    }

    public void testRegisterWaiterResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        StreamInput in = bodyBytes(withFutureFields(fullRegisterWaiterResponseBody()), false);
        assertTrue(new WorkloadGroupSharedThrottleService.RegisterWaiterResponse(in).registered);
        assertEquals(0, in.available());
    }

    public void testRegisterWaiterResponseIsStrict() throws Exception {
        assertEveryKeyRequired(fullRegisterWaiterResponseBody(), false, WorkloadGroupSharedThrottleService.RegisterWaiterResponse::new);
    }

    // The action names and body keys every other version uses, spelled out rather than taken from the constants: renaming
    // one changes the writer and the reader alike, so every round trip still passes while a mixed cluster breaks.
    public void testActionNamesAndBodyKeysAreFixed() throws IOException {
        assertEquals("internal:wlm/throttle/shared/acquire", WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME);
        assertEquals("internal:wlm/throttle/shared/release", WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME);
        assertEquals("internal:wlm/throttle/shared/check_tasks", WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME);
        assertEquals("internal:wlm/throttle/shared/grant", WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME);
        assertEquals("internal:wlm/throttle/shared/register_waiter", WorkloadGroupSharedThrottleService.REGISTER_WAITER_ACTION_NAME);

        assertEquals(
            Set.of("bucket_key", "shared_limit", "permit_id", "coordinator_node_id", "coordinator_task_id", "wants_queue"),
            writtenRequestKeys(acquireRequest("grp1:group", "permit-1"))
        );
        assertEquals(
            Set.of("granted", "not_owner", "registered"),
            writtenResponseKeys(WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED)
        );
        assertEquals(
            Set.of("bucket_key", "permit_id"),
            writtenRequestKeys(new WorkloadGroupSharedThrottleService.ReleasePermitRequest("grp1:group", "permit-1"))
        );
        assertEquals(Set.of(), writtenResponseKeys(WorkloadGroupSharedThrottleService.ReleasePermitResponse.INSTANCE));
        assertEquals(
            Set.of("coordinator_node_id", "task_ids"),
            writtenRequestKeys(new TaskCheckRequest("eph-coordinator", new long[] { 7 }))
        );
        assertEquals(Set.of("active"), writtenResponseKeys(new TaskCheckResponse(new boolean[] { true })));
        assertEquals(
            Set.of("bucket_key", "coordinator_node_id"),
            writtenRequestKeys(new WorkloadGroupSharedThrottleService.RegisterWaiterRequest("grp1:group", "eph-coordinator"))
        );
        assertEquals(Set.of("registered"), writtenResponseKeys(WorkloadGroupSharedThrottleService.RegisterWaiterResponse.NOT_REGISTERED));
        assertEquals(
            Set.of("bucket_key", "permit_id"),
            writtenRequestKeys(new WorkloadGroupSharedThrottleService.GrantPermitRequest("grp1:group", "permit-1"))
        );
        assertEquals(Set.of("admitted_task_ids"), writtenResponseKeys(WorkloadGroupSharedThrottleService.GrantPermitResponse.NOT_ADMITTED));
    }

    // The body keys a response writes.
    private static Set<String> writtenResponseKeys(TransportResponse response) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return in.readMap(StreamInput::readString, StreamInput::readGenericValue).keySet();
            }
        }
    }

    // Request queueing is reserved in this version: the GRANT and REGISTER_WAITER handlers are registered so a later version
    // gets an answer rather than a missing-action error, and that answer is always "nothing happened".

    public void testGrantIsAnsweredWithNothingAdmitted() {
        WorkloadGroupSharedThrottleService service = newService();
        WorkloadGroupSharedThrottleService.GrantPermitResponse response = service.handleGrant(
            new WorkloadGroupSharedThrottleService.GrantPermitRequest("grp1:group:group", "permit-g")
        );
        assertEquals(OptionalLong.empty(), response.admittedTaskId);
    }

    public void testRegisterWaiterIsRefused() {
        WorkloadGroupSharedThrottleService service = newService();
        // Refused for a remote coordinator and for this node itself, on a bucket this node owns.
        for (String coordinatorId : List.of("eph-remote-coordinator", localNode.getEphemeralId())) {
            WorkloadGroupSharedThrottleService.RegisterWaiterResponse response = service.handleRegisterWaiter(
                new WorkloadGroupSharedThrottleService.RegisterWaiterRequest("grp1:group:group", coordinatorId)
            );
            assertFalse(response.registered);
        }
    }

    public void testAcquireIgnoresWantsQueueAndNeverRegisters() {
        WorkloadGroupSharedThrottleService service = newService();
        DiscoveryNode coordinator = dataNode("later-version-coordinator");
        String key = "grp1:group:group";

        WorkloadGroupSharedThrottleService.AcquirePermitResponse granted = service.handleAcquire(
            acquireRequest(key, 1, "p1", coordinator, true)
        );
        assertTrue(granted.granted);
        assertFalse(granted.registered);

        // At the limit, a denial of an acquire that asked to queue registers nothing: no grant follows from it.
        WorkloadGroupSharedThrottleService.AcquirePermitResponse denied = service.handleAcquire(
            acquireRequest(key, 1, "p2", coordinator, true)
        );
        assertFalse(denied.granted);
        assertFalse(denied.notOwner);
        assertFalse(denied.registered);
        assertEquals(1, service.tracker().inFlight(key));
    }
}

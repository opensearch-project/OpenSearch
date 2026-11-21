/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RequestHandlerRegistry;
import org.opensearch.transport.TestTransportChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.wlm.WorkloadGroupSharedThrottleService.TaskCheckRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.wlm.SharedThrottleTestPermits.tryAcquire;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises the cross-node (remote owner) acquire/release path of {@link WorkloadGroupSharedThrottleService} over two
 * real {@link MockTransportService}s, since {@code TransportService#sendRequest} is final and cannot be mocked.
 */
public class WorkloadGroupSharedThrottleServiceTransportTests extends OpenSearchTestCase {

    // Far above any CI stall, so the grant/deny assertions never race the production 200ms acquire timeout.
    private static final TimeValue GENEROUS_ACQUIRE_TIMEOUT = TimeValue.timeValueSeconds(30);

    private ThreadPool threadPool;
    private MockTransportService coordinatorTransport;
    private MockTransportService ownerTransport;

    private WorkloadGroupSharedThrottleService coordinatorService;
    private WorkloadGroupSharedThrottleService ownerService;

    private DiscoveryNode ownerNode;
    private DiscoveryNode coordinatorNode;
    private AtomicLong ownerClock;

    // A bucket key whose ring owner is the remote (owner) node, forcing the coordinator down the transport path.
    private String remoteKey;

    // The last ACQUIRE response handler a coordinator sent, captured by its transport interceptor.
    private final AtomicReference<WorkloadGroupSharedThrottleService.RemoteAcquire> sentAcquire = new AtomicReference<>();
    // Counted down once the owner has applied a CHECK_TASKS reply or failure; see expectTaskChecks.
    private final AtomicReference<CountDownLatch> checkRepliesHandled = new AtomicReference<>(new CountDownLatch(0));

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());

        // Two real transports. Default node roles include DATA at Version.CURRENT (>= MIN_OWNER_VERSION), so both
        // nodes are eligible ring owners.
        coordinatorTransport = newTransport(captureAcquires());
        ownerTransport = newTransport(countCheckReplies());
        coordinatorTransport.start();
        coordinatorTransport.acceptIncomingRequests();
        ownerTransport.start();
        ownerTransport.acceptIncomingRequests();
        // The coordinator must be able to reach the owner for the acquire/release RPCs.
        coordinatorTransport.connectToNode(ownerTransport.getLocalDiscoNode());
        ownerTransport.connectToNode(coordinatorTransport.getLocalDiscoNode());

        coordinatorNode = coordinatorTransport.getLocalDiscoNode();
        ownerNode = ownerTransport.getLocalDiscoNode();

        // Each service sees itself as the local node.
        ClusterService coordinatorClusterService = mock(ClusterService.class);
        when(coordinatorClusterService.localNode()).thenReturn(coordinatorNode);
        ClusterService ownerClusterService = mock(ClusterService.class);
        when(ownerClusterService.localNode()).thenReturn(ownerNode);

        // Build one service per transport so BOTH register their acquire/release handlers on their own transport.
        coordinatorService = newService(coordinatorClusterService, coordinatorTransport, GENEROUS_ACQUIRE_TIMEOUT);
        ownerClock = new AtomicLong();
        ownerService = new WorkloadGroupSharedThrottleService(
            ownerClusterService,
            threadPool,
            ownerTransport,
            GENEROUS_ACQUIRE_TIMEOUT,
            ownerClock::get
        );

        // Drive an identical membership (both nodes as data nodes) into both services so they build the same ring and
        // agree on a single deterministic owner per bucket.
        DiscoveryNodes bothNodes = DiscoveryNodes.builder()
            .add(coordinatorNode)
            .add(ownerNode)
            .localNodeId(coordinatorNode.getId())
            .build();
        deliverNodes(coordinatorService, bothNodes);
        deliverNodes(ownerService, bothNodes);

        remoteKey = findKeyOwnedBy(ownerNode, coordinatorService, ownerService);
    }

    private WorkloadGroupSharedThrottleService newService(
        ClusterService clusterService,
        MockTransportService transport,
        TimeValue timeout
    ) {
        return new WorkloadGroupSharedThrottleService(clusterService, threadPool, transport, timeout);
    }

    private MockTransportService newTransport(TransportInterceptor interceptor) {
        return MockTransportService.createNewService(
            Settings.EMPTY,
            MockTransportService.newMockTransport(Settings.EMPTY, Version.CURRENT, threadPool),
            Version.CURRENT,
            threadPool,
            null,
            Collections.emptySet(),
            interceptor,
            NoopTracer.INSTANCE
        );
    }

    // Records each ACQUIRE's response handler (the service's RemoteAcquire, passed through unwrapped) as it is sent.
    private TransportInterceptor captureAcquires() {
        return new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return new AsyncSender() {
                    @Override
                    public <T extends TransportResponse> void sendRequest(
                        Transport.Connection connection,
                        String action,
                        TransportRequest request,
                        TransportRequestOptions options,
                        TransportResponseHandler<T> handler
                    ) {
                        if (WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME.equals(action)
                            && handler instanceof WorkloadGroupSharedThrottleService.RemoteAcquire acquire) {
                            sentAcquire.set(acquire);
                        }
                        sender.sendRequest(connection, action, request, options, handler);
                    }
                };
            }
        };
    }

    // Counts down checkRepliesHandled after the owner's CHECK_TASKS handler has applied a reply or a failure.
    private TransportInterceptor countCheckReplies() {
        return new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return new AsyncSender() {
                    @Override
                    public <T extends TransportResponse> void sendRequest(
                        Transport.Connection connection,
                        String action,
                        TransportRequest request,
                        TransportRequestOptions options,
                        TransportResponseHandler<T> handler
                    ) {
                        if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action) == false) {
                            sender.sendRequest(connection, action, request, options, handler);
                            return;
                        }
                        sender.sendRequest(connection, action, request, options, new TransportResponseHandler<T>() {
                            @Override
                            public T read(StreamInput in) throws IOException {
                                return handler.read(in);
                            }

                            @Override
                            public void handleResponse(T response) {
                                try {
                                    handler.handleResponse(response);
                                } finally {
                                    checkRepliesHandled.get().countDown();
                                }
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                try {
                                    handler.handleException(exp);
                                } finally {
                                    checkRepliesHandled.get().countDown();
                                }
                            }

                            @Override
                            public void handleRejection(Exception exp) {
                                handler.handleRejection(exp);
                            }

                            @Override
                            public String executor() {
                                return handler.executor();
                            }
                        });
                    }
                };
            }
        };
    }

    // Finds a bucket key that every given service's ring maps to the remote owner, so acquireAsync takes the transport
    // path (and the owner does not refuse it as not_owner).
    private static String findKeyOwnedBy(DiscoveryNode owner, WorkloadGroupSharedThrottleService... services) {
        for (int i = 0; i < 10000; i++) {
            String candidate = "bucket-" + i;
            boolean ownedByAll = true;
            for (WorkloadGroupSharedThrottleService service : services) {
                DiscoveryNode candidateOwner = service.ring().ownerFor(candidate).orElse(null);
                ownedByAll &= candidateOwner != null && candidateOwner.getId().equals(owner.getId());
            }
            if (ownedByAll) {
                return candidate;
            }
        }
        throw new AssertionError("could not find a bucket owned by the remote owner node");
    }

    @Override
    public void tearDown() throws Exception {
        coordinatorTransport.close();
        ownerTransport.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private void deliverNodes(WorkloadGroupSharedThrottleService service, DiscoveryNodes nodes) {
        deliverNodes(service, DiscoveryNodes.EMPTY_NODES, nodes);
    }

    private void deliverNodes(WorkloadGroupSharedThrottleService service, DiscoveryNodes previousNodes, DiscoveryNodes nodes) {
        ClusterState previous = mock(ClusterState.class);
        when(previous.nodes()).thenReturn(previousNodes);
        ClusterState current = mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        service.clusterChanged(new ClusterChangedEvent("test", current, previous));
    }

    /**
     * Neither handler may trip the in-flight breaker: one owner's heap pressure would otherwise fail every bucket it owns
     * closed, and a breaker-rejected RELEASE would hold its permit until the TTL.
     */
    public void testAcquireAndReleaseHandlersCannotTripCircuitBreaker() {
        for (String action : List.of(
            WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
            WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME,
            WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME,
            WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME,
            WorkloadGroupSharedThrottleService.REGISTER_WAITER_ACTION_NAME
        )) {
            RequestHandlerRegistry<? extends TransportRequest> handler = ownerTransport.getRequestHandler(action);
            assertNotNull("handler must be registered for " + action, handler);
            assertFalse(action + " must not trip the in-flight circuit breaker", handler.canTripCircuitBreaker());
        }
    }

    /**
     * Happy path across nodes: the coordinator asks the remote owner for a permit, the owner grants it over the wire,
     * and closing the returned {@link Releasable} sends the fire-and-forget RELEASE RPC that drains the owner's tracker.
     */
    public void testRemoteAcquireGrantedThenReleaseRoundTrips() throws Exception {
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
        listener.await();

        assertNull("granted acquire must not fail", listener.failure.get());
        assertTrue("listener must have been notified via onResponse", listener.responded.get());
        Releasable permit = listener.response.get();
        assertNotNull("remote owner granted a permit, so a non-null Releasable is expected", permit);
        assertOnSearchThread(listener);
        // The grant was recorded on the OWNER node's tracker (the acquire handler ran there before responding).
        assertEquals("owner tracker must hold exactly one in-flight permit", 1, ownerService.tracker().inFlight(remoteKey));

        // Closing the permit fires the RELEASE RPC (fire-and-forget), which the owner applies asynchronously.
        permit.close();
        assertBusy(() -> assertEquals("release RPC must drain the owner's in-flight count", 0, ownerService.tracker().inFlight(remoteKey)));
    }

    public void testShortRequestsNeverSendTaskChecks() throws Exception {
        AtomicInteger checks = countTaskChecks();
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            permit = acquireTrackedPermit(task);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS - 1);
            ownerService.tracker().sweepExpired();
            CapturingListener overflow = new CapturingListener();
            coordinatorService.acquireAsync(remoteKey, 1, task.getId(), false, overflow);
            overflow.await();
            assertTrue(WorkloadGroupSharedThrottleService.isDenial(overflow.failure.get()));
            assertEquals("fresh permits require no liveness RPC", 0, checks.get());
            permit.close();
            assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(remoteKey)));
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            assertEquals("completed short searches also require no liveness RPC", 0, checks.get());
        } finally {
            if (permit != null) {
                permit.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testExpiredRemotePermitRenewsOnlyWhileItsTaskIsRegistered() throws Exception {
        AtomicInteger checks = countTaskChecks();
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            permit = acquireTrackedPermit(task);
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(1, checks.get());
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
            CapturingListener overflow = new CapturingListener();
            coordinatorService.acquireAsync(remoteKey, 1, task.getId(), false, overflow);
            overflow.await();
            assertTrue(
                "the active expired search still occupies its slot",
                WorkloadGroupSharedThrottleService.isDenial(overflow.failure.get())
            );

            ownerClock.set(2 * WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS - 1);
            ownerService.tracker().sweepExpired();
            assertEquals("renewal uses the original five-minute TTL", 1, checks.get());
            coordinatorTransport.getTaskManager().unregister(task);
            applied = expectTaskChecks(1);
            ownerClock.set(2 * WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(2, checks.get());
            assertEquals("a lost release is reclaimed once the task is absent", 0, ownerService.tracker().inFlight(remoteKey));
        } finally {
            if (permit != null) {
                permit.close();
            }
            if (coordinatorTransport.getTaskManager().getTask(task.getId()) != null) {
                coordinatorTransport.getTaskManager().unregister(task);
            }
        }
    }

    public void testExpiredPermitsAreBatchedAcrossBuckets() throws Exception {
        List<Integer> sizes = new CopyOnWriteArrayList<>();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action)) {
                sizes.add(((TaskCheckRequest) request).taskIds.length);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        Task task = registerSearchTask();
        try {
            for (int i = 0; i < 3; i++) {
                assertTrue(
                    tryAcquire(ownerService.tracker(), "batch-" + i, 1, "permit-" + i, 100, coordinatorNode.getEphemeralId(), task.getId())
                );
            }
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.set(100);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(List.of(3), sizes);
            for (int i = 0; i < 3; i++) {
                assertEquals(1, ownerService.tracker().inFlight("batch-" + i));
                ownerService.tracker().release("batch-" + i, "permit-" + i);
            }
        } finally {
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testExpiredChecksSplitAtTheBatchLimit() throws Exception {
        List<Integer> sizes = new CopyOnWriteArrayList<>();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action)) {
                sizes.add(((TaskCheckRequest) request).taskIds.length);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        Task task = registerSearchTask();
        int permits = WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK + 1;
        try {
            for (int i = 0; i < permits; i++) {
                assertTrue(
                    tryAcquire(
                        ownerService.tracker(),
                        remoteKey,
                        permits,
                        "permit-" + i,
                        100,
                        coordinatorNode.getEphemeralId(),
                        task.getId()
                    )
                );
            }
            CountDownLatch applied = expectTaskChecks(2);
            ownerClock.set(100);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(List.of(1, WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK), sizes.stream().sorted().toList());
            assertEquals(permits, ownerService.tracker().inFlight(remoteKey));
        } finally {
            ownerService.tracker().releaseAllFrom(Set.of(coordinatorNode.getEphemeralId()));
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testFailedCheckUsesShortTtlAndRecoveryRestoresFullTtl() throws Exception {
        AtomicBoolean failChecks = new AtomicBoolean(true);
        AtomicInteger checks = new AtomicInteger();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action)) {
                checks.incrementAndGet();
                if (failChecks.get()) {
                    throw new ConnectTransportException(connection.getNode(), "simulated task check failure");
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            permit = acquireTrackedPermit(task);
            CountDownLatch applied = expectTaskChecks(1);
            long expiry = WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS;
            ownerClock.set(expiry);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
            ownerClock.set(expiry + WorkloadGroupSharedThrottleService.FAILED_CHECK_TTL.nanos() - 1);
            ownerService.tracker().sweepExpired();
            assertEquals("failed checks do not trigger continuous retries", 1, checks.get());
            failChecks.set(false);
            applied = expectTaskChecks(1);
            ownerClock.set(expiry + WorkloadGroupSharedThrottleService.FAILED_CHECK_TTL.nanos());
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals("retry occurs after thirty seconds, not another five minutes", 2, checks.get());
            ownerClock.addAndGet(WorkloadGroupSharedThrottleService.FAILED_CHECK_TTL.nanos());
            ownerService.tracker().sweepExpired();
            assertEquals("an active reply restores the full five-minute TTL", 2, checks.get());
        } finally {
            if (permit != null) {
                permit.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testUnreachableCoordinatorGetsShortExtension() throws Exception {
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            permit = acquireTrackedPermit(task);
            ownerTransport.disconnectFromNode(coordinatorNode);
            long scansBefore = ownerService.tracker().pruneScanCount();
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            assertTrue("the sweep must collect the expired permit", ownerService.tracker().pruneScanCount() > scansBefore);
            // A disconnected coordinator gets no RPC; the drain extends the permit directly, clearing its pending mark.
            assertBusy(() -> assertEquals("task checks were not applied", 0, ownerService.tracker().pendingChecks()));
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
            ownerTransport.connectToNode(coordinatorNode);
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.addAndGet(WorkloadGroupSharedThrottleService.FAILED_CHECK_TTL.nanos());
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
        } finally {
            if (permit != null) {
                permit.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testLateTaskCheckCannotRestoreReleasedPermit() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<CheckedRunnable<Exception>> heldCheck = new AtomicReference<>();
        coordinatorTransport.addRequestHandlingBehavior(
            WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME,
            (handler, request, channel, transportTask) -> {
                heldCheck.set(() -> handler.messageReceived(request, channel, transportTask));
                received.countDown();
            }
        );
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            permit = acquireTrackedPermit(task);
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            assertTrue(received.await(10, TimeUnit.SECONDS));
            permit.close();
            assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(remoteKey)));
            heldCheck.getAndSet(null).run();
            awaitChecks(applied);
            assertEquals("a delayed active reply must never recreate a released permit", 0, ownerService.tracker().inFlight(remoteKey));
        } finally {
            CheckedRunnable<Exception> pending = heldCheck.getAndSet(null);
            if (pending != null) {
                pending.run();
            }
            coordinatorTransport.clearAllRules();
            if (permit != null) {
                permit.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testFormerOwnerCanVerifyItsRetainedPermits() throws Exception {
        DiscoveryNode third = new DiscoveryNode(
            "third",
            "third",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        DiscoveryNodes before = DiscoveryNodes.builder().add(ownerNode).add(coordinatorNode).localNodeId(ownerNode.getId()).build();
        DiscoveryNodes after = DiscoveryNodes.builder()
            .add(ownerNode)
            .add(coordinatorNode)
            .add(third)
            .localNodeId(ownerNode.getId())
            .build();
        ThrottleOwnerSelector nextRing = ThrottleOwnerSelector.fromDiscoveryNodes(after);
        String moved = null;
        for (int i = 0; i < 10000; i++) {
            String candidate = "moving-" + i;
            if (ownerService.ring().ownerFor(candidate).orElseThrow().equals(ownerNode)
                && nextRing.ownerFor(candidate).orElseThrow().equals(ownerNode) == false) {
                moved = candidate;
                break;
            }
        }
        assertNotNull(moved);
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            CapturingListener listener = new CapturingListener();
            coordinatorService.acquireAsync(moved, 1, task.getId(), false, listener);
            listener.await();
            permit = listener.response.get();
            assertNotNull(permit);
            deliverNodes(ownerService, before, after);
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(1, ownerService.tracker().inFlight(moved));
        } finally {
            if (permit != null) {
                permit.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    private Task registerSearchTask() {
        return coordinatorTransport.getTaskManager().register("transport", "indices:data/read/search", new SearchRequest("test"));
    }

    private Releasable acquireTrackedPermit(Task task) throws Exception {
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, task.getId(), false, listener);
        listener.await();
        assertNull(listener.failure.get());
        assertNotNull(listener.response.get());
        return listener.response.get();
    }

    private AtomicInteger countTaskChecks() {
        AtomicInteger checks = new AtomicInteger();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action)) {
                checks.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });
        return checks;
    }

    // Expects count CHECK_TASKS round trips (reply or failure) to be applied by the owner.
    private CountDownLatch expectTaskChecks(int count) {
        CountDownLatch applied = new CountDownLatch(count);
        checkRepliesHandled.set(applied);
        return applied;
    }

    private static void awaitChecks(CountDownLatch applied) throws InterruptedException {
        assertTrue("task checks were not applied", applied.await(10, TimeUnit.SECONDS));
    }

    /**
     * A coordinator that leaves the cluster never releases what it holds; the owner purges its permits on the node
     * removal instead of pinning the slots until the TTL.
     */
    public void testOwnerPurgesPermitsOfDepartedCoordinator() throws Exception {
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
        listener.await();
        assertNotNull("the remote grant must be delivered, failure: " + listener.failure.get(), listener.response.get());
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));

        DiscoveryNodes both = DiscoveryNodes.builder()
            .add(coordinatorTransport.getLocalDiscoNode())
            .add(ownerNode)
            .localNodeId(ownerNode.getId())
            .build();
        deliverNodes(ownerService, both, DiscoveryNodes.builder().add(ownerNode).localNodeId(ownerNode.getId()).build());
        assertEquals("the departed coordinator's permit must be purged", 0, ownerService.tracker().inFlight(remoteKey));
    }

    /**
     * An owner at its shared limit denies over the wire, and a caller that rejects on denial gets the denial inline on the
     * response thread (see {@code acquireAsync}).
     */
    public void testRemoteAcquireDeniedReturns429() throws Exception {
        // Pre-fill the owner's tracker to the limit directly so the next acquire is denied at the source.
        assertTrue(
            tryAcquire(
                ownerService.tracker(),
                remoteKey,
                1,
                "pre",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                "filler-coordinator"
            )
        );
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
        listener.await();

        assertFalse("denied acquire must not invoke onResponse", listener.responded.get());
        assertTrue("must be the shared-limit denial marker", WorkloadGroupSharedThrottleService.isDenial(listener.failure.get()));
        assertNotOnSearchThread(listener);
        assertTrue(
            "denial must be an OpenSearchRejectedExecutionException (429), was: " + listener.failure.get(),
            listener.failure.get() instanceof OpenSearchRejectedExecutionException
        );
    }

    /** A caller that proceeds on denial (MONITOR) continues the search from the callback, so it gets it on the search pool. */
    public void testRemoteDenialForCallerThatProceedsIsDeliveredOnSearch() throws Exception {
        assertTrue(
            tryAcquire(
                ownerService.tracker(),
                remoteKey,
                1,
                "pre",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                "filler-coordinator"
            )
        );

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, true, listener);
        listener.await();

        assertTrue("must be the shared-limit denial marker", WorkloadGroupSharedThrottleService.isDenial(listener.failure.get()));
        assertOnSearchThread(listener);
    }

    /**
     * Mid-rebalance the coordinator's ring can still name a node that no longer owns the bucket. That node refuses with
     * not_owner, and the coordinator must treat it as the shared tier being unavailable (fail closed), not as a denial.
     */
    public void testNotOwnerReplyIsUnavailable() throws Exception {
        // The owner node's own ring now says the coordinator owns every bucket, so it is a former owner for remoteKey.
        deliverNodes(
            ownerService,
            DiscoveryNodes.builder().add(coordinatorTransport.getLocalDiscoNode()).localNodeId(ownerNode.getId()).build()
        );

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
        listener.await();

        assertFalse("a not_owner reply must not admit", listener.responded.get());
        assertTrue(
            "a not_owner reply must report the shared tier unavailable, was: " + listener.failure.get(),
            WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
        );
        assertEquals("the former owner must not have recorded a permit", 0, ownerService.tracker().inFlight(remoteKey));
        assertNotOnSearchThread(listener);
    }

    /**
     * An acquire that fails at send time fails closed through {@code handleException} (the owner stays connected, so the
     * {@code nodeConnected} pre-check passes) and sends no release.
     */
    public void testUnavailableWhenOwnerTransportFails() throws Exception {
        assertTrue(
            "precondition: owner must be connected so we exercise the send path, not the pre-check",
            coordinatorTransport.nodeConnected(ownerNode)
        );
        AtomicInteger releasesSent = new AtomicInteger();
        coordinatorTransport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME.equals(action)) {
                // Simulate the send blowing up; TransportService routes this to the handler's handleException.
                throw new ConnectTransportException(connection.getNode(), "simulated acquire send failure");
            }
            if (WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME.equals(action)) {
                releasesSent.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
        listener.await();

        assertFalse("a transport error must not admit the request", listener.responded.get());
        assertTrue(
            "a transport error must report the shared tier unavailable, was: " + listener.failure.get(),
            WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
        );
        assertEquals("listener must be notified exactly once", 1, listener.invocations.get());
        // handleException sends any release before it notifies the listener, so this is already final.
        assertEquals("a failure that never reached the owner must not send a release", 0, releasesSent.get());
        assertEquals(0, ownerService.tracker().inFlight(remoteKey));
    }

    /**
     * The owner records the grant but answers with an error, so the coordinator gets a {@code RemoteTransportException}.
     * The owner provably processed the acquire, so the coordinator reports the shared tier unavailable once and releases
     * the permit (ordered after the grant) instead of leaving it to the TTL.
     */
    public void testRemoteErrorAfterOwnerGrantedReleasesThePermit() throws Exception {
        AtomicReference<TransportResponse> recordedReply = new AtomicReference<>();
        ownerTransport.addRequestHandlingBehavior(
            WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
            (handler, request, channel, task) -> {
                handler.messageReceived(
                    request,
                    new TestTransportChannel(ActionListener.wrap(recordedReply::set, e -> fail("the owner handler must not fail: " + e))),
                    task
                );
                channel.sendResponse(new IllegalStateException("simulated failure after the grant was recorded"));
            }
        );

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
        listener.await();

        assertTrue(
            "the owner must have granted (and recorded) the acquire",
            ((WorkloadGroupSharedThrottleService.AcquirePermitResponse) recordedReply.get()).granted
        );
        assertFalse("a remote error must not admit the request", listener.responded.get());
        assertTrue(
            "a remote error must report the shared tier unavailable, was: " + listener.failure.get(),
            WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
        );
        assertBusy(() -> assertEquals("the recorded grant must be released", 0, ownerService.tracker().inFlight(remoteKey)));
        assertEquals("listener must be notified exactly once", 1, listener.invocations.get());
    }

    /**
     * An acquire the owner never answers must fail closed once the acquire timeout elapses: an unavailable
     * {@code onFailure}, exactly once. The coordinator's timer decides the outcome, so no RELEASE is sent at the timeout:
     * the acquire may still be in flight, and a release could overtake it.
     */
    public void testUnavailableWhenAcquireTimesOut() throws Exception {
        withShortTimeoutCoordinator((transport, service, key) -> {
            AtomicBoolean acquireSwallowed = new AtomicBoolean();
            AtomicInteger releasesSent = new AtomicInteger();
            transport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
                if (WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME.equals(action)) {
                    acquireSwallowed.set(true); // never delivered, never answered
                    return;
                }
                if (WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME.equals(action)) {
                    releasesSent.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });

            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
            listener.await();

            assertTrue("the ACQUIRE must have been sent (and swallowed)", acquireSwallowed.get());
            assertFalse("a timeout must not admit the request", listener.responded.get());
            assertTrue(
                "a timeout must report the shared tier unavailable, was: " + listener.failure.get(),
                WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
            );
            assertFalse(
                "listener must be notified exactly once",
                waitUntil(() -> listener.invocations.get() > 1, 500, TimeUnit.MILLISECONDS)
            );
            assertEquals("a timeout must not send a release", 0, releasesSent.get());
            assertEquals("the owner never saw the acquire, so it holds nothing", 0, ownerService.tracker().inFlight(key));
        });
    }

    /**
     * The acquire reaches the owner only after the coordinator's timer reported it unavailable (a stalled owner). No
     * release may be sent at the timeout, since it would arrive first and strand the later grant; the late grant reply
     * triggers the release instead.
     */
    public void testLateGrantAfterTimeoutIsReleasedAfterTheGrant() throws Exception {
        withShortTimeoutCoordinator((transport, service, key) -> {
            // Hold the whole owner-side handling (acquire + reply) until the coordinator has given up on it.
            CountDownLatch acquireArrived = new CountDownLatch(1);
            AtomicReference<CheckedRunnable<Exception>> heldAcquire = new AtomicReference<>();
            AtomicBoolean acquireProcessed = new AtomicBoolean();
            AtomicInteger releasesBeforeAcquire = new AtomicInteger();
            ownerTransport.addRequestHandlingBehavior(
                WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    heldAcquire.set(() -> {
                        handler.messageReceived(request, channel, task);
                        acquireProcessed.set(true);
                    });
                    acquireArrived.countDown();
                }
            );
            ownerTransport.addRequestHandlingBehavior(
                WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    if (acquireProcessed.get() == false) {
                        releasesBeforeAcquire.incrementAndGet();
                    }
                    handler.messageReceived(request, channel, task);
                }
            );

            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
            listener.await();
            assertTrue(
                "the timer must report the shared tier unavailable, was: " + listener.failure.get(),
                WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
            );
            assertTrue("the acquire must have reached the owner", acquireArrived.await(10, TimeUnit.SECONDS));
            // The handler stays registered for the late reply, but must no longer pin the answered request's listener.
            WorkloadGroupSharedThrottleService.RemoteAcquire acquire = sentAcquire.get();
            assertNotNull("the ACQUIRE handler must reach the interceptor unwrapped", acquire);
            assertFalse("a decided acquire must drop the caller's listener", acquire.holdsListener());
            assertFalse(
                "no release may reach the owner ahead of the acquire it would reclaim",
                waitUntil(() -> releasesBeforeAcquire.get() > 0, 500, TimeUnit.MILLISECONDS)
            );

            // The owner now grants, and its reply reaches the coordinator after the timeout.
            heldAcquire.get().run();
            assertTrue(acquireProcessed.get());
            assertBusy(() -> assertEquals("the late grant must be released", 0, ownerService.tracker().inFlight(key)));
            assertEquals("listener must be notified exactly once", 1, listener.invocations.get());
            assertFalse(listener.responded.get());
        });
    }

    /**
     * A reply that beats the timer decides the outcome: a normal grant, the timer is cancelled, and the timer firing
     * anyway afterwards must neither notify again nor release the permit the caller now holds.
     */
    public void testReplyBeforeTimeoutIsDeliveredOnce() throws Exception {
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
        listener.await();
        assertNotNull("a prompt grant must be delivered, failure: " + listener.failure.get(), listener.response.get());

        WorkloadGroupSharedThrottleService.RemoteAcquire acquire = sentAcquire.get();
        assertNotNull("the ACQUIRE handler must reach the interceptor unwrapped", acquire);
        assertTrue("the reply must cancel the acquire timer", acquire.timer().isCancelled());
        assertFalse("a decided acquire must drop the caller's listener", acquire.holdsListener());
        // Simulate the timer having already fired when the reply decided: it must be a no-op.
        acquire.onTimeout();
        assertEquals("a grant must not be followed by a second notification", 1, listener.invocations.get());
        assertEquals("the delivered permit still holds its slot", 1, ownerService.tracker().inFlight(remoteKey));
        listener.response.get().close();
        assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(remoteKey)));
    }

    private interface ShortTimeoutBody {
        void run(MockTransportService transport, WorkloadGroupSharedThrottleService service, String key) throws Exception;
    }

    // Runs body against a dedicated coordinator with a short acquire timeout; key is owned by the remote owner node.
    private void withShortTimeoutCoordinator(ShortTimeoutBody body) throws Exception {
        final TimeValue acquireTimeout = TimeValue.timeValueMillis(50);
        try (MockTransportService transport = newTransport(captureAcquires())) {
            transport.start();
            transport.acceptIncomingRequests();
            transport.connectToNode(ownerNode);
            DiscoveryNode node = transport.getLocalDiscoNode();
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.localNode()).thenReturn(node);
            WorkloadGroupSharedThrottleService service = newService(clusterService, transport, acquireTimeout);
            DiscoveryNodes nodes = DiscoveryNodes.builder().add(node).add(ownerNode).localNodeId(node.getId()).build();
            deliverNodes(service, nodes);
            // The owner must know this coordinator too, both to agree on the ring and so its removal can be observed.
            deliverNodes(ownerService, nodes);
            String key = findKeyOwnedBy(ownerNode, service, ownerService);
            body.run(transport, service, key);
        }
    }

    /**
     * If the search pool rejects the hand-off of a granted acquire, the coordinator must release the shared permit it was
     * just granted and surface the pool's own rejection (a 429), never the shared-limit denial marker.
     */
    public void testSearchPoolRejectionReleasesGrantedPermit() throws Exception {
        withSaturatedSearchPool((service, key) -> {
            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
            listener.await();

            assertFalse("a rejected hand-off must not admit the request", listener.responded.get());
            assertTrue(
                "the pool's rejection must surface, was: " + listener.failure.get(),
                listener.failure.get() instanceof OpenSearchRejectedExecutionException
            );
            assertFalse(
                "a pool rejection is not a shared-limit denial",
                WorkloadGroupSharedThrottleService.isDenial(listener.failure.get())
            );
            assertBusy(() -> assertEquals("the granted permit must be released", 0, ownerService.tracker().inFlight(key)));
        });
    }

    /**
     * A caller that proceeds on every outcome (MONITOR) must never be rejected by the shared tier: when the pool rejects
     * the hand-off of a grant, the grant is delivered inline instead.
     */
    public void testSearchPoolRejectionDeliversGrantInlineForCallerThatProceeds() throws Exception {
        withSaturatedSearchPool((service, key) -> {
            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, true, listener);
            listener.await();

            assertNull("the grant must not be turned into a failure, was: " + listener.failure.get(), listener.failure.get());
            assertNotNull("the granted permit must be delivered", listener.response.get());
            assertEquals("the delivered permit still holds its slot", 1, ownerService.tracker().inFlight(key));
            listener.response.get().close();
            assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(key)));
        });
    }

    /** With the search pool saturated, a denial for a caller that proceeds (MONITOR) is delivered inline, not lost to the pool. */
    public void testSearchPoolRejectionDeliversDenialInlineForCallerThatProceeds() throws Exception {
        withSaturatedSearchPool((service, key) -> {
            assertTrue(
                tryAcquire(ownerService.tracker(), key, 1, "pre", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, "filler-coordinator")
            );
            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, true, listener);
            listener.await();

            assertTrue(
                "the denial must survive the pool rejection, was: " + listener.failure.get(),
                WorkloadGroupSharedThrottleService.isDenial(listener.failure.get())
            );
            assertNotOnSearchThread(listener);
        });
    }

    /** With the search pool saturated, a denial for a caller that rejects on denial is still the denial marker. */
    public void testSaturatedSearchPoolDoesNotMaskDenial() throws Exception {
        withSaturatedSearchPool((service, key) -> {
            assertTrue(
                tryAcquire(ownerService.tracker(), key, 1, "pre", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, "filler-coordinator")
            );
            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
            listener.await();

            assertTrue(
                "a denial must not become a pool rejection, was: " + listener.failure.get(),
                WorkloadGroupSharedThrottleService.isDenial(listener.failure.get())
            );
        });
    }

    private interface CoordinatorBody {
        void run(WorkloadGroupSharedThrottleService service, String key) throws Exception;
    }

    // Runs body against a fresh coordinator whose search pool has its only thread and only queue slot occupied, so any
    // hand-off to it is rejected. key is owned by the remote owner node.
    private void withSaturatedSearchPool(CoordinatorBody body) throws Exception {
        Settings oneSlotSearchPool = Settings.builder().put("thread_pool.search.size", 1).put("thread_pool.search.queue_size", 1).build();
        ThreadPool saturatedPool = new TestThreadPool("saturated-search", oneSlotSearchPool);
        CountDownLatch unblock = new CountDownLatch(1);
        try (
            MockTransportService transport = MockTransportService.createNewService(
                Settings.EMPTY,
                Version.CURRENT,
                threadPool,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            transport.acceptIncomingRequests();
            transport.connectToNode(ownerNode);
            DiscoveryNode node = transport.getLocalDiscoNode();
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.localNode()).thenReturn(node);
            WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
                clusterService,
                saturatedPool,
                transport,
                GENEROUS_ACQUIRE_TIMEOUT
            );
            deliverNodes(service, DiscoveryNodes.builder().add(node).add(ownerNode).localNodeId(node.getId()).build());
            String key = findKeyOwnedBy(ownerNode, service, ownerService);

            // Occupy the only search thread, then the only queue slot, so the next hand-off is rejected.
            CountDownLatch running = new CountDownLatch(1);
            saturatedPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                running.countDown();
                try {
                    unblock.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(running.await(10, TimeUnit.SECONDS));
            saturatedPool.executor(ThreadPool.Names.SEARCH).execute(() -> {});

            body.run(service, key);
        } finally {
            unblock.countDown();
            ThreadPool.terminate(saturatedPool, 10, TimeUnit.SECONDS);
        }
    }

    private static final String TEST_HEADER = "x-wlm-test-header";
    private static final String TEST_TRANSIENT = "x-wlm-test-transient";

    /**
     * A remote grant resumes the search on the coordinator's SEARCH pool, not on the thread that called acquireAsync. The
     * caller's ThreadContext (request headers and transients, e.g. the security user) must be restored for that
     * continuation, not the network thread's.
     */
    public void testRemoteGrantResumesOnSearchWithCallersThreadContext() throws Exception {
        ContextCapturingListener listener = acquireWithCallerContext(coordinatorService, remoteKey, false);
        assertNotNull("remote owner granted a permit", listener.response.get());
        assertOnSearchThread(listener);
        listener.assertSawCallerContext();
        listener.response.get().close();
    }

    /** A MONITOR denial also continues the search, on the SEARCH pool, with the caller's context. */
    public void testRemoteDenialForCallerThatProceedsKeepsCallersThreadContext() throws Exception {
        fillOwner(remoteKey);
        ContextCapturingListener listener = acquireWithCallerContext(coordinatorService, remoteKey, true);
        assertTrue(WorkloadGroupSharedThrottleService.isDenial(listener.failure.get()));
        assertOnSearchThread(listener);
        listener.assertSawCallerContext();
    }

    /** An ENFORCED denial is delivered inline on the response (network) thread, still with the caller's context. */
    public void testInlineDenialKeepsCallersThreadContext() throws Exception {
        fillOwner(remoteKey);
        ContextCapturingListener listener = acquireWithCallerContext(coordinatorService, remoteKey, false);
        assertTrue(WorkloadGroupSharedThrottleService.isDenial(listener.failure.get()));
        assertNotOnSearchThread(listener);
        listener.assertSawCallerContext();
    }

    /** A timed-out acquire is reported from the GENERIC timer thread, still with the caller's context. */
    public void testTimeoutKeepsCallersThreadContext() throws Exception {
        withShortTimeoutCoordinator((transport, service, key) -> {
            transport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
                if (WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME.equals(action) == false) {
                    connection.sendRequest(requestId, action, request, options);
                } // the ACQUIRE is swallowed so the timer decides
            });
            ContextCapturingListener listener = acquireWithCallerContext(service, key, false);
            assertTrue(WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get()));
            listener.assertSawCallerContext();
        });
    }

    private void fillOwner(String key) {
        assertTrue(
            tryAcquire(ownerService.tracker(), key, 1, "pre", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, "filler-coordinator")
        );
    }

    // Calls acquireAsync under a caller context carrying a header and a transient, then restores the test thread's own
    // context before waiting, so anything the listener sees must have come from the captured caller context.
    private ContextCapturingListener acquireWithCallerContext(WorkloadGroupSharedThrottleService service, String key, boolean proceeds)
        throws InterruptedException {
        ContextCapturingListener listener = new ContextCapturingListener(threadPool.getThreadContext());
        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(TEST_HEADER, "caller");
            threadPool.getThreadContext().putTransient(TEST_TRANSIENT, "caller-transient");
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, proceeds, listener);
        }
        assertNull("the caller context must not leak into the test thread", threadPool.getThreadContext().getHeader(TEST_HEADER));
        listener.await();
        return listener;
    }

    /** Records the header and transient visible in the ThreadContext when the outcome is delivered. */
    private static final class ContextCapturingListener extends CapturingListener {
        private final ThreadContext threadContext;
        final AtomicReference<String> header = new AtomicReference<>();
        final AtomicReference<Object> transientValue = new AtomicReference<>();

        ContextCapturingListener(ThreadContext threadContext) {
            this.threadContext = threadContext;
        }

        @Override
        public void onResponse(Releasable releasable) {
            capture();
            super.onResponse(releasable);
        }

        @Override
        public void onFailure(Exception e) {
            capture();
            super.onFailure(e);
        }

        private void capture() {
            header.set(threadContext.getHeader(TEST_HEADER));
            transientValue.set(threadContext.getTransient(TEST_TRANSIENT));
        }

        void assertSawCallerContext() {
            assertEquals("caller header must survive the hop to [" + thread.get() + "]", "caller", header.get());
            assertEquals("caller transient must survive the hop to [" + thread.get() + "]", "caller-transient", transientValue.get());
        }
    }

    private static void assertOnSearchThread(CapturingListener listener) {
        assertTrue(
            "remote outcome must be delivered on the search pool, was [" + listener.thread.get() + "]",
            listener.thread.get().contains("[" + ThreadPool.Names.SEARCH + "]")
        );
    }

    private static void assertNotOnSearchThread(CapturingListener listener) {
        assertFalse(
            "outcome must be delivered inline, not on the search pool, was [" + listener.thread.get() + "]",
            listener.thread.get().contains("[" + ThreadPool.Names.SEARCH + "]")
        );
    }

    /**
     * Request queueing is reserved in this version, but a later-version node may already use its RPCs. Over the wire, this
     * version answers each of them with "nothing happened" rather than an action-not-found error: a GRANT admits nothing, a
     * REGISTER_WAITER is refused, and an ACQUIRE that asks to queue is answered as any other, without a registration. Its
     * own acquires never ask to queue.
     */
    public void testReservedQueueingMessagesAreAnsweredWithNothingHappened() throws Exception {
        WorkloadGroupSharedThrottleService.GrantPermitResponse grant = sendToOwner(
            WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME,
            new WorkloadGroupSharedThrottleService.GrantPermitRequest(remoteKey, "permit-g"),
            WorkloadGroupSharedThrottleService.GrantPermitResponse::new
        );
        assertEquals(OptionalLong.empty(), grant.admittedTaskId);

        WorkloadGroupSharedThrottleService.RegisterWaiterResponse registration = sendToOwner(
            WorkloadGroupSharedThrottleService.REGISTER_WAITER_ACTION_NAME,
            new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(remoteKey, coordinatorNode.getEphemeralId()),
            WorkloadGroupSharedThrottleService.RegisterWaiterResponse::new
        );
        assertFalse(registration.registered);

        // An acquire from a coordinator that asks to queue: granted below the limit, denied at it, and never registered.
        WorkloadGroupSharedThrottleService.AcquirePermitResponse granted = sendToOwner(
            WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
            queueingAcquire("permit-1"),
            WorkloadGroupSharedThrottleService.AcquirePermitResponse::new
        );
        assertTrue(granted.granted);
        assertFalse(granted.registered);
        WorkloadGroupSharedThrottleService.AcquirePermitResponse denied = sendToOwner(
            WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
            queueingAcquire("permit-2"),
            WorkloadGroupSharedThrottleService.AcquirePermitResponse::new
        );
        assertFalse(denied.granted);
        assertFalse(denied.notOwner);
        assertFalse(denied.registered);
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));

        AtomicReference<Boolean> sentWantsQueue = new AtomicReference<>();
        coordinatorTransport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
            if (request instanceof WorkloadGroupSharedThrottleService.AcquirePermitRequest acquire) {
                sentWantsQueue.set(acquire.wantsQueue);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
        listener.await();
        assertTrue("the bucket is at its limit", WorkloadGroupSharedThrottleService.isDenial(listener.failure.get()));
        assertEquals(Boolean.FALSE, sentWantsQueue.get());
    }

    // An acquire for remoteKey with limit 1, as a coordinator that retained the request would send it.
    private WorkloadGroupSharedThrottleService.AcquirePermitRequest queueingAcquire(String permitId) {
        return new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            remoteKey,
            1,
            permitId,
            WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
            coordinatorNode.getEphemeralId(),
            SharedThrottleTestPermits.NO_SUCH_TASK,
            true
        );
    }

    // Sends request from the coordinator's transport to the owner's registered handler and waits for the reply.
    private <T extends TransportResponse> T sendToOwner(String action, TransportRequest request, Writeable.Reader<T> reader) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        coordinatorTransport.sendRequest(ownerNode, action, request, new ActionListenerResponseHandler<>(future, reader));
        return future.actionGet(TimeValue.timeValueSeconds(10));
    }

    /** Captures the outcome of an async acquire, distinguishing onResponse(null) from an unfired listener via a latch. */
    private static class CapturingListener implements ActionListener<Releasable> {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Releasable> response = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final AtomicBoolean responded = new AtomicBoolean(false);
        final AtomicReference<String> thread = new AtomicReference<>();
        final AtomicInteger invocations = new AtomicInteger();

        @Override
        public void onResponse(Releasable releasable) {
            invocations.incrementAndGet();
            thread.set(Thread.currentThread().getName());
            response.set(releasable);
            responded.set(true);
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            invocations.incrementAndGet();
            thread.set(Thread.currentThread().getName());
            failure.set(e);
            latch.countDown();
        }

        void await() throws InterruptedException {
            assertTrue("listener was not invoked within the timeout", latch.await(10, TimeUnit.SECONDS));
        }
    }
}

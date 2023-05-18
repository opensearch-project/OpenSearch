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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.action.shard;

import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.action.shard.ShardStateAction.FailedShardEntry;
import org.opensearch.cluster.action.shard.ShardStateAction.StartedShardEntry;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NodeDisconnectedException;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.opensearch.test.VersionUtils.randomCompatibleVersion;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ShardStateActionTests extends OpenSearchTestCase {
    private static ThreadPool THREAD_POOL;

    private TestShardStateAction shardStateAction;
    private CapturingTransport transport;
    private TransportService transportService;
    private ClusterService clusterService;

    private static class TestShardStateAction extends ShardStateAction {
        TestShardStateAction(
            ClusterService clusterService,
            TransportService transportService,
            AllocationService allocationService,
            RerouteService rerouteService
        ) {
            super(clusterService, transportService, allocationService, rerouteService, THREAD_POOL);
        }

        private Runnable onBeforeWaitForNewClusterManagerAndRetry;

        public void setOnBeforeWaitForNewClusterManagerAndRetry(Runnable onBeforeWaitForNewClusterManagerAndRetry) {
            this.onBeforeWaitForNewClusterManagerAndRetry = onBeforeWaitForNewClusterManagerAndRetry;
        }

        private Runnable onAfterWaitForNewClusterManagerAndRetry;

        public void setOnAfterWaitFornewClusterManagerAndRetry(Runnable onAfterWaitFornewClusterManagerAndRetry) {
            this.onAfterWaitForNewClusterManagerAndRetry = onAfterWaitFornewClusterManagerAndRetry;
        }

        @Override
        protected void waitForNewClusterManagerAndRetry(
            String actionName,
            ClusterStateObserver observer,
            TransportRequest request,
            ActionListener<Void> listener,
            Predicate<ClusterState> changePredicate
        ) {
            onBeforeWaitForNewClusterManagerAndRetry.run();
            super.waitForNewClusterManagerAndRetry(actionName, observer, request, listener, changePredicate);
            onAfterWaitForNewClusterManagerAndRetry.run();
        }
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool("ShardStateActionTest");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            THREAD_POOL,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        shardStateAction = new TestShardStateAction(clusterService, transportService, null, null);
        shardStateAction.setOnBeforeWaitForNewClusterManagerAndRetry(() -> {});
        shardStateAction.setOnAfterWaitFornewClusterManagerAndRetry(() -> {});
    }

    @Override
    @After
    public void tearDown() throws Exception {
        clusterService.close();
        transportService.close();
        super.tearDown();
        assertThat(shardStateAction.remoteShardFailedCacheSize(), equalTo(0));
    }

    @AfterClass
    public static void stopThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        THREAD_POOL = null;
    }

    public void testSuccess() throws InterruptedException {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        final TestListener listener = new TestListener();
        ShardRouting shardRouting = getRandomShardRouting(index);
        shardStateAction.localShardFailed(shardRouting, "test", getSimulatedFailure(), listener);

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);
        // the request is a shard failed request
        assertThat(capturedRequests[0].request, is(instanceOf(ShardStateAction.FailedShardEntry.class)));
        ShardStateAction.FailedShardEntry shardEntry = (ShardStateAction.FailedShardEntry) capturedRequests[0].request;
        // for the right shard
        assertEquals(shardEntry.shardId, shardRouting.shardId());
        assertEquals(shardEntry.allocationId, shardRouting.allocationId().getId());
        // sent to the cluster-manager
        assertEquals(clusterService.state().nodes().getClusterManagerNode().getId(), capturedRequests[0].node.getId());

        transport.handleResponse(capturedRequests[0].requestId, TransportResponse.Empty.INSTANCE);

        listener.await();
        assertNull(listener.failure.get());
    }

    public void testNoClusterManager() throws InterruptedException {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        DiscoveryNodes.Builder noClusterManagerBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
        noClusterManagerBuilder.clusterManagerNodeId(null);
        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(noClusterManagerBuilder));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger retries = new AtomicInteger();
        AtomicBoolean success = new AtomicBoolean();

        setUpClusterManagerRetryVerification(1, retries, latch, requestId -> {});

        ShardRouting failedShard = getRandomShardRouting(index);
        shardStateAction.localShardFailed(failedShard, "test", getSimulatedFailure(), new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                success.set(false);
                latch.countDown();
                assert false;
            }
        });

        latch.await();

        assertThat(retries.get(), equalTo(1));
        assertTrue(success.get());
    }

    public void testClusterManagerChannelException() throws InterruptedException {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger retries = new AtomicInteger();
        AtomicBoolean success = new AtomicBoolean();
        AtomicReference<Throwable> throwable = new AtomicReference<>();

        LongConsumer retryLoop = requestId -> {
            if (randomBoolean()) {
                transport.handleRemoteError(
                    requestId,
                    randomFrom(new NotClusterManagerException("simulated"), new FailedToCommitClusterStateException("simulated"))
                );
            } else {
                if (randomBoolean()) {
                    transport.handleLocalError(requestId, new NodeNotConnectedException(null, "simulated"));
                } else {
                    transport.handleError(requestId, new NodeDisconnectedException(null, ShardStateAction.SHARD_FAILED_ACTION_NAME));
                }
            }
        };

        final int numberOfRetries = randomIntBetween(1, 256);
        setUpClusterManagerRetryVerification(numberOfRetries, retries, latch, retryLoop);

        ShardRouting failedShard = getRandomShardRouting(index);
        shardStateAction.localShardFailed(failedShard, "test", getSimulatedFailure(), new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                success.set(false);
                throwable.set(e);
                latch.countDown();
                assert false;
            }
        });

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        assertFalse(success.get());
        assertThat(retries.get(), equalTo(0));
        retryLoop.accept(capturedRequests[0].requestId);

        latch.await();
        assertNull(throwable.get());
        assertThat(retries.get(), equalTo(numberOfRetries));
        assertTrue(success.get());
    }

    public void testUnhandledFailure() {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        final TestListener listener = new TestListener();
        ShardRouting failedShard = getRandomShardRouting(index);
        shardStateAction.localShardFailed(failedShard, "test", getSimulatedFailure(), listener);

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        transport.handleRemoteError(capturedRequests[0].requestId, new TransportException("simulated"));
        assertNotNull(listener.failure.get());
    }

    public void testShardNotFound() throws InterruptedException {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        final TestListener listener = new TestListener();
        ShardRouting failedShard = getRandomShardRouting(index);
        RoutingTable routingTable = RoutingTable.builder(clusterService.state().getRoutingTable()).remove(index).build();
        setState(clusterService, ClusterState.builder(clusterService.state()).routingTable(routingTable));
        shardStateAction.localShardFailed(failedShard, "test", getSimulatedFailure(), listener);

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        transport.handleResponse(capturedRequests[0].requestId, TransportResponse.Empty.INSTANCE);

        listener.await();
        assertNull(listener.failure.get());
    }

    public void testNoLongerPrimaryShardException() throws InterruptedException {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        ShardRouting failedShard = getRandomShardRouting(index);

        final TestListener listener = new TestListener();
        long primaryTerm = clusterService.state().metadata().index(index).primaryTerm(failedShard.id());
        assertThat(primaryTerm, greaterThanOrEqualTo(1L));
        shardStateAction.remoteShardFailed(
            failedShard.shardId(),
            failedShard.allocationId().getId(),
            primaryTerm + 1,
            randomBoolean(),
            "test",
            getSimulatedFailure(),
            listener
        );

        ShardStateAction.NoLongerPrimaryShardException catastrophicError = new ShardStateAction.NoLongerPrimaryShardException(
            failedShard.shardId(),
            "dummy failure"
        );
        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        transport.handleRemoteError(capturedRequests[0].requestId, catastrophicError);

        listener.await();

        final Exception failure = listener.failure.get();
        assertNotNull(failure);
        assertThat(failure, instanceOf(ShardStateAction.NoLongerPrimaryShardException.class));
        assertThat(failure.getMessage(), equalTo(catastrophicError.getMessage()));
    }

    public void testCacheRemoteShardFailed() throws Exception {
        final String index = "test";
        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));
        ShardRouting failedShard = getRandomShardRouting(index);
        boolean markAsStale = randomBoolean();
        int numListeners = between(1, 100);
        CountDownLatch latch = new CountDownLatch(numListeners);
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        for (int i = 0; i < numListeners; i++) {
            shardStateAction.remoteShardFailed(
                failedShard.shardId(),
                failedShard.allocationId().getId(),
                primaryTerm,
                markAsStale,
                "test",
                getSimulatedFailure(),
                new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void aVoid) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        latch.countDown();
                    }
                }
            );
        }
        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests, arrayWithSize(1));
        transport.handleResponse(capturedRequests[0].requestId, TransportResponse.Empty.INSTANCE);
        latch.await();
        assertThat(transport.capturedRequests(), arrayWithSize(0));
    }

    public void testRemoteShardFailedConcurrently() throws Exception {
        final String index = "test";
        final AtomicBoolean shutdown = new AtomicBoolean(false);
        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));
        ShardRouting[] failedShards = new ShardRouting[between(1, 5)];
        for (int i = 0; i < failedShards.length; i++) {
            failedShards[i] = getRandomShardRouting(index);
        }
        Thread[] clientThreads = new Thread[between(1, 6)];
        int iterationsPerThread = scaledRandomIntBetween(50, 500);
        Phaser barrier = new Phaser(clientThreads.length + 2); // one for cluster-manager thread, one for the main thread
        Thread clusterManagerThread = new Thread(() -> {
            barrier.arriveAndAwaitAdvance();
            while (shutdown.get() == false) {
                for (CapturingTransport.CapturedRequest request : transport.getCapturedRequestsAndClear()) {
                    if (randomBoolean()) {
                        transport.handleResponse(request.requestId, TransportResponse.Empty.INSTANCE);
                    } else {
                        transport.handleRemoteError(request.requestId, randomFrom(getSimulatedFailure()));
                    }
                }
            }
        });
        clusterManagerThread.start();

        AtomicInteger notifiedResponses = new AtomicInteger();
        for (int t = 0; t < clientThreads.length; t++) {
            clientThreads[t] = new Thread(() -> {
                barrier.arriveAndAwaitAdvance();
                for (int i = 0; i < iterationsPerThread; i++) {
                    ShardRouting failedShard = randomFrom(failedShards);
                    shardStateAction.remoteShardFailed(
                        failedShard.shardId(),
                        failedShard.allocationId().getId(),
                        randomLongBetween(1, Long.MAX_VALUE),
                        randomBoolean(),
                        "test",
                        getSimulatedFailure(),
                        new ActionListener<Void>() {
                            @Override
                            public void onResponse(Void aVoid) {
                                notifiedResponses.incrementAndGet();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                notifiedResponses.incrementAndGet();
                            }
                        }
                    );
                }
            });
            clientThreads[t].start();
        }
        barrier.arriveAndAwaitAdvance();
        for (Thread t : clientThreads) {
            t.join();
        }
        assertBusy(() -> assertThat(notifiedResponses.get(), equalTo(clientThreads.length * iterationsPerThread)));
        shutdown.set(true);
        clusterManagerThread.join();
    }

    public void testShardStarted() throws InterruptedException {
        final String index = "test";
        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        final ShardRouting shardRouting = getRandomShardRouting(index);
        final long primaryTerm = clusterService.state().metadata().index(shardRouting.index()).primaryTerm(shardRouting.id());
        final TestListener listener = new TestListener();
        shardStateAction.shardStarted(shardRouting, primaryTerm, "testShardStarted", listener);

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests[0].request, instanceOf(ShardStateAction.StartedShardEntry.class));

        ShardStateAction.StartedShardEntry entry = (ShardStateAction.StartedShardEntry) capturedRequests[0].request;
        assertThat(entry.shardId, equalTo(shardRouting.shardId()));
        assertThat(entry.allocationId, equalTo(shardRouting.allocationId().getId()));
        assertThat(entry.primaryTerm, equalTo(primaryTerm));

        transport.handleResponse(capturedRequests[0].requestId, TransportResponse.Empty.INSTANCE);
        listener.await();
        assertNull(listener.failure.get());
    }

    private ShardRouting getRandomShardRouting(String index) {
        IndexRoutingTable indexRoutingTable = clusterService.state().routingTable().index(index);
        ShardsIterator shardsIterator = indexRoutingTable.randomAllActiveShardsIt();
        ShardRouting shardRouting = shardsIterator.nextOrNull();
        assert shardRouting != null;
        return shardRouting;
    }

    private void setUpClusterManagerRetryVerification(
        int numberOfRetries,
        AtomicInteger retries,
        CountDownLatch latch,
        LongConsumer retryLoop
    ) {
        shardStateAction.setOnBeforeWaitForNewClusterManagerAndRetry(() -> {
            DiscoveryNodes.Builder clusterManagerBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
            clusterManagerBuilder.clusterManagerNodeId(
                clusterService.state().nodes().getClusterManagerNodes().values().iterator().next().getId()
            );
            setState(clusterService, ClusterState.builder(clusterService.state()).nodes(clusterManagerBuilder));
        });

        shardStateAction.setOnAfterWaitFornewClusterManagerAndRetry(() -> verifyRetry(numberOfRetries, retries, latch, retryLoop));
    }

    private void verifyRetry(int numberOfRetries, AtomicInteger retries, CountDownLatch latch, LongConsumer retryLoop) {
        // assert a retry request was sent
        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        if (capturedRequests.length == 1) {
            retries.incrementAndGet();
            if (retries.get() == numberOfRetries) {
                // finish the request
                transport.handleResponse(capturedRequests[0].requestId, TransportResponse.Empty.INSTANCE);
            } else {
                retryLoop.accept(capturedRequests[0].requestId);
            }
        } else {
            // there failed to be a retry request
            // release the driver thread to fail the test
            latch.countDown();
        }
    }

    private Exception getSimulatedFailure() {
        return new CorruptIndexException("simulated", (String) null);
    }

    public void testFailedShardEntrySerialization() throws Exception {
        final ShardId shardId = new ShardId(randomRealisticUnicodeOfLengthBetween(10, 100), UUID.randomUUID().toString(), between(0, 1000));
        final String allocationId = randomRealisticUnicodeOfCodepointLengthBetween(10, 100);
        final long primaryTerm = randomIntBetween(0, 100);
        final String message = randomRealisticUnicodeOfCodepointLengthBetween(10, 100);
        final Exception failure = randomBoolean() ? null : getSimulatedFailure();
        final boolean markAsStale = randomBoolean();

        final Version version = randomFrom(randomCompatibleVersion(random(), Version.CURRENT));
        final FailedShardEntry failedShardEntry = new FailedShardEntry(shardId, allocationId, primaryTerm, message, failure, markAsStale);
        try (StreamInput in = serialize(failedShardEntry, version).streamInput()) {
            in.setVersion(version);
            final FailedShardEntry deserialized = new FailedShardEntry(in);
            assertThat(deserialized.shardId, equalTo(shardId));
            assertThat(deserialized.allocationId, equalTo(allocationId));
            assertThat(deserialized.primaryTerm, equalTo(primaryTerm));
            assertThat(deserialized.message, equalTo(message));
            if (failure != null) {
                assertThat(deserialized.failure, notNullValue());
                assertThat(deserialized.failure.getClass(), equalTo(failure.getClass()));
                assertThat(deserialized.failure.getMessage(), equalTo(failure.getMessage()));
            } else {
                assertThat(deserialized.failure, nullValue());
            }
            assertThat(deserialized.markAsStale, equalTo(markAsStale));
            assertEquals(failedShardEntry, deserialized);
        }
    }

    public void testStartedShardEntrySerialization() throws Exception {
        final ShardId shardId = new ShardId(randomRealisticUnicodeOfLengthBetween(10, 100), UUID.randomUUID().toString(), between(0, 1000));
        final String allocationId = randomRealisticUnicodeOfCodepointLengthBetween(10, 100);
        final long primaryTerm = randomIntBetween(0, 100);
        final String message = randomRealisticUnicodeOfCodepointLengthBetween(10, 100);

        final Version version = randomFrom(randomCompatibleVersion(random(), Version.CURRENT));
        try (StreamInput in = serialize(new StartedShardEntry(shardId, allocationId, primaryTerm, message), version).streamInput()) {
            in.setVersion(version);
            final StartedShardEntry deserialized = new StartedShardEntry(in);
            assertThat(deserialized.shardId, equalTo(shardId));
            assertThat(deserialized.allocationId, equalTo(allocationId));
            assertThat(deserialized.primaryTerm, equalTo(primaryTerm));
            assertThat(deserialized.message, equalTo(message));
        }
    }

    BytesReference serialize(Writeable writeable, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            writeable.writeTo(out);
            return out.bytes();
        }
    }

    private static class TestListener implements ActionListener<Void> {

        private final SetOnce<Exception> failure = new SetOnce<>();
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onResponse(Void aVoid) {
            try {
                failure.set(null);
            } finally {
                latch.countDown();
            }
        }

        @Override
        public void onFailure(final Exception e) {
            try {
                failure.set(e);
            } finally {
                latch.countDown();
            }
        }

        void await() throws InterruptedException {
            latch.await();
        }
    }
}

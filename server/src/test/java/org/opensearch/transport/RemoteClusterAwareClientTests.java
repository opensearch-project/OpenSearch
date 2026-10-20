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

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteClusterAwareClientTests extends OpenSearchTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes) {
        return RemoteClusterConnectionTests.startTransport(id, knownNodes, Version.CURRENT, threadPool);
    }

    public void testSearchShards() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes);
            MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes)
        ) {
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalDiscoNode().getAddress().toString());
            try (
                MockTransportService service = MockTransportService.createNewService(
                    builder.build(),
                    Version.CURRENT,
                    threadPool,
                    NoopTracer.INSTANCE
                )
            ) {
                service.start();
                service.acceptIncomingRequests();

                try (RemoteClusterAwareClient client = new RemoteClusterAwareClient(Settings.EMPTY, threadPool, service, "cluster1")) {
                    SearchRequest request = new SearchRequest("test-index");
                    CountDownLatch responseLatch = new CountDownLatch(1);
                    AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                    ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest("test-index").indicesOptions(
                        request.indicesOptions()
                    ).local(true).preference(request.preference()).routing(request.routing());
                    client.admin()
                        .cluster()
                        .searchShards(
                            searchShardsRequest,
                            new LatchedActionListener<>(
                                ActionListener.wrap(reference::set, e -> fail("no failures expected")),
                                responseLatch
                            )
                        );
                    responseLatch.await();
                    assertNotNull(reference.get());
                    ClusterSearchShardsResponse clusterSearchShardsResponse = reference.get();
                    assertEquals(knownNodes, Arrays.asList(clusterSearchShardsResponse.getNodes()));
                }
            }
        }
    }

    public void testSendStreamRequestThrowsWhenStreamTransportDisabled() throws Exception {
        // The streaming send path must NOT fall back to the regular transport when the stream transport
        // is unavailable — enabling streaming is a deliberate opt-in, so a missing stream transport is a
        // misconfiguration that surfaces as an error (routed to the handler), not a silent pull.
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes)) {
            knownNodes.add(seedTransport.getLocalDiscoNode());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalDiscoNode().getAddress().toString());
            try (
                MockTransportService service = MockTransportService.createNewService(
                    builder.build(),
                    Version.CURRENT,
                    threadPool,
                    NoopTracer.INSTANCE
                )
            ) {
                service.start();
                service.acceptIncomingRequests();

                // 4-arg constructor => streamTransportService is null (stream transport not enabled).
                try (RemoteClusterAwareClient client = new RemoteClusterAwareClient(Settings.EMPTY, threadPool, service, "cluster1")) {
                    IllegalStateException e = expectThrows(
                        IllegalStateException.class,
                        () -> client.sendStreamRequest(
                            "internal:test/stream",
                            new SearchRequest("test-index"),
                            seedTransport.getLocalDiscoNode(),
                            noopStreamHandler()
                        )
                    );
                    assertTrue(
                        "message should explain the stream transport is not enabled, was: " + e.getMessage(),
                        e.getMessage().contains("stream transport is not enabled")
                    );
                }
            }
        }
    }

    public void testSendStreamRequestConnectsToTargetNodeOnStreamTransport() throws Exception {
        // sendStreamRequest routes through the stream transport: after ensureConnected on the remote
        // cluster it connects the resolved target node on the StreamTransportService before sending. Here
        // that connect fails, so we can assert (a) it targeted the requested node and (b) the failure is
        // wrapped as a ConnectTransportException delivered to the handler (wrapAsTransportException).
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes)) {
            DiscoveryNode seedNode = seedTransport.getLocalDiscoNode();
            knownNodes.add(seedNode);
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedNode.getAddress().toString());
            try (
                MockTransportService service = MockTransportService.createNewService(
                    builder.build(),
                    Version.CURRENT,
                    threadPool,
                    NoopTracer.INSTANCE
                )
            ) {
                service.start();
                service.acceptIncomingRequests();

                StreamTransportService streamTransportService = mock(StreamTransportService.class);
                when(streamTransportService.getStreamTransportReqTimeout()).thenReturn(TimeValue.timeValueSeconds(30));
                // connectToNode (non-final, stream-transport-specific) fails synchronously.
                doAnswer(inv -> {
                    ActionListener<Void> l = inv.getArgument(2);
                    l.onFailure(new RuntimeException("no stream route"));
                    return null;
                }).when(streamTransportService).connectToNode(any(DiscoveryNode.class), any(), any());

                CountDownLatch failed = new CountDownLatch(1);
                AtomicReference<TransportException> handlerError = new AtomicReference<>();
                try (
                    RemoteClusterAwareClient client = new RemoteClusterAwareClient(
                        Settings.EMPTY,
                        threadPool,
                        service,
                        "cluster1",
                        streamTransportService
                    )
                ) {
                    client.sendStreamRequest(
                        "internal:test/stream",
                        new SearchRequest("test-index"),
                        seedNode,
                        new StreamTransportResponseHandler<>() {
                            @Override
                            public void handleStreamResponse(StreamTransportResponse<TransportResponse> s) {}

                            @Override
                            public void handleException(TransportException exp) {
                                handlerError.set(exp);
                                failed.countDown();
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }

                            @Override
                            public TransportResponse read(StreamInput in) {
                                return null;
                            }
                        }
                    );

                    assertTrue("handler must be notified of the connect failure", failed.await(5, TimeUnit.SECONDS));
                    verify(streamTransportService).connectToNode(eq(seedNode), any(), any());
                    assertTrue(
                        "connect failure must be wrapped as ConnectTransportException, was: " + handlerError.get(),
                        handlerError.get() instanceof ConnectTransportException
                    );
                }
            }
        }
    }

    private static StreamTransportResponseHandler<TransportResponse> noopStreamHandler() {
        return new StreamTransportResponseHandler<>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TransportResponse> s) {}

            @Override
            public void handleException(TransportException exp) {}

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TransportResponse read(StreamInput in) {
                return null;
            }
        };
    }

    public void testSearchShardsThreadContextHeader() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes);
            MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes)
        ) {
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalDiscoNode().getAddress().toString());
            try (
                MockTransportService service = MockTransportService.createNewService(
                    builder.build(),
                    Version.CURRENT,
                    threadPool,
                    NoopTracer.INSTANCE
                )
            ) {
                service.start();
                service.acceptIncomingRequests();

                try (RemoteClusterAwareClient client = new RemoteClusterAwareClient(Settings.EMPTY, threadPool, service, "cluster1")) {
                    SearchRequest request = new SearchRequest("test-index");
                    int numThreads = 10;
                    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
                    for (int i = 0; i < numThreads; i++) {
                        final String threadId = Integer.toString(i);
                        executorService.submit(() -> {
                            ThreadContext threadContext = seedTransport.threadPool.getThreadContext();
                            threadContext.putHeader("threadId", threadId);
                            AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                            final ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest("test-index")
                                .indicesOptions(request.indicesOptions())
                                .local(true)
                                .preference(request.preference())
                                .routing(request.routing());
                            CountDownLatch responseLatch = new CountDownLatch(1);
                            client.admin()
                                .cluster()
                                .searchShards(searchShardsRequest, new LatchedActionListener<>(ActionListener.wrap(resp -> {
                                    reference.set(resp);
                                    assertEquals(threadId, seedTransport.threadPool.getThreadContext().getHeader("threadId"));
                                }, e -> fail("no failures expected")), responseLatch));
                            try {
                                responseLatch.await();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            assertNotNull(reference.get());
                            ClusterSearchShardsResponse clusterSearchShardsResponse = reference.get();
                            assertEquals(knownNodes, Arrays.asList(clusterSearchShardsResponse.getNodes()));
                        });
                    }
                    ThreadPool.terminate(executorService, 5, TimeUnit.SECONDS);
                }
            }
        }
    }
}

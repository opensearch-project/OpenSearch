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

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.Streams;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.nativeprotocol.NativeOutboundHandler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.instanceOf;

public class NativeOutboundHandlerTests extends OpenSearchTestCase {

    private final String feature1 = "feature1";
    private final String feature2 = "feature2";
    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final TransportRequestOptions options = TransportRequestOptions.EMPTY;
    private final AtomicReference<Tuple<Header, BytesReference>> message = new AtomicReference<>();
    private InboundPipeline pipeline;
    private OutboundHandler handler;
    private NativeOutboundHandler nativeOutboundHandler;
    private FakeTcpChannel channel;
    private DiscoveryNode node;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        channel = new FakeTcpChannel(randomBoolean(), buildNewFakeTransportAddress().address(), buildNewFakeTransportAddress().address());
        TransportAddress transportAddress = buildNewFakeTransportAddress();
        node = new DiscoveryNode("", transportAddress, Version.CURRENT);
        String[] features = { feature1, feature2 };
        StatsTracker statsTracker = new StatsTracker();
        handler = new OutboundHandler(statsTracker, threadPool);
        nativeOutboundHandler = new NativeOutboundHandler(
            "node",
            Version.CURRENT,
            features,
            statsTracker,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            handler
        );

        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final Supplier<CircuitBreaker> breaker = () -> new NoopCircuitBreaker("test");
        final InboundAggregator aggregator = new InboundAggregator(breaker, (Predicate<String>) action -> true);
        pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, (c, m) -> {
            try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
                Streams.copy(m.openOrGetStreamInput(), streamOutput);
                message.set(new Tuple<>(m.getHeader(), streamOutput.bytes()));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testSendRequest() throws IOException {
        ThreadContext threadContext = threadPool.getThreadContext();
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        String value = "message";
        threadContext.putHeader("header", "header_value");
        TestRequest request = new TestRequest(value);

        AtomicReference<DiscoveryNode> nodeRef = new AtomicReference<>();
        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<TransportRequest> requestRef = new AtomicReference<>();
        nativeOutboundHandler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions options
            ) {
                nodeRef.set(node);
                requestIdRef.set(requestId);
                actionRef.set(action);
                requestRef.set(request);
            }
        });
        nativeOutboundHandler.sendRequest(node, channel, requestId, action, request, options, version, compress, isHandshake);

        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertEquals(node, nodeRef.get());
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertEquals(request, requestRef.get());

        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {}));
        final Tuple<Header, BytesReference> tuple = message.get();
        final Header header = tuple.v1();
        final TestRequest message = new TestRequest(tuple.v2().streamInput());
        assertEquals(version, header.getVersion());
        assertEquals(requestId, header.getRequestId());
        assertTrue(header.isRequest());
        assertFalse(header.isResponse());
        if (isHandshake) {
            assertTrue(header.isHandshake());
        } else {
            assertFalse(header.isHandshake());
        }
        if (compress) {
            assertTrue(header.isCompressed());
        } else {
            assertFalse(header.isCompressed());
        }

        assertEquals(value, message.getValue());
        assertEquals("header_value", header.getHeaders().v1().get("header"));
    }

    public void testSendResponse() throws IOException {
        ThreadContext threadContext = threadPool.getThreadContext();
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        String value = "message";
        threadContext.putHeader("header", "header_value");
        TestResponse response = new TestResponse(value);

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<TransportResponse> responseRef = new AtomicReference<>();
        nativeOutboundHandler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, TransportResponse response) {
                requestIdRef.set(requestId);
                actionRef.set(action);
                responseRef.set(response);
            }
        });
        nativeOutboundHandler.sendResponse(version, Collections.emptySet(), channel, requestId, action, response, compress, isHandshake);

        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertEquals(response, responseRef.get());

        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {}));
        final Tuple<Header, BytesReference> tuple = message.get();
        final Header header = tuple.v1();
        final TestResponse message = new TestResponse(tuple.v2().streamInput());
        assertEquals(version, header.getVersion());
        assertEquals(requestId, header.getRequestId());
        assertFalse(header.isRequest());
        assertTrue(header.isResponse());
        if (isHandshake) {
            assertTrue(header.isHandshake());
        } else {
            assertFalse(header.isHandshake());
        }
        if (compress) {
            assertTrue(header.isCompressed());
        } else {
            assertFalse(header.isCompressed());
        }

        assertFalse(header.isError());

        assertEquals(value, message.getValue());
        assertEquals("header_value", header.getHeaders().v1().get("header"));
    }

    public void testErrorResponse() throws IOException {
        ThreadContext threadContext = threadPool.getThreadContext();
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        threadContext.putHeader("header", "header_value");
        OpenSearchException error = new OpenSearchException("boom");

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<Exception> responseRef = new AtomicReference<>();
        nativeOutboundHandler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, Exception error) {
                requestIdRef.set(requestId);
                actionRef.set(action);
                responseRef.set(error);
            }
        });
        nativeOutboundHandler.sendErrorResponse(version, Collections.emptySet(), channel, requestId, action, error);

        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertEquals(error, responseRef.get());

        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {}));
        final Tuple<Header, BytesReference> tuple = message.get();
        final Header header = tuple.v1();
        assertEquals(version, header.getVersion());
        assertEquals(requestId, header.getRequestId());
        assertFalse(header.isRequest());
        assertTrue(header.isResponse());
        assertFalse(header.isCompressed());
        assertFalse(header.isHandshake());
        assertTrue(header.isError());

        RemoteTransportException remoteException = tuple.v2().streamInput().readException();
        assertThat(remoteException.getCause(), instanceOf(OpenSearchException.class));
        assertEquals(remoteException.getCause().getMessage(), "boom");
        assertEquals(action, remoteException.action());
        assertEquals(channel.getLocalAddress(), remoteException.address().address());

        assertEquals("header_value", header.getHeaders().v1().get("header"));
    }
}

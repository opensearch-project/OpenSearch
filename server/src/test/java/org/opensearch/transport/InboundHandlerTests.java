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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.instanceOf;

public class InboundHandlerTests extends OpenSearchTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final Version version = Version.CURRENT;

    private TaskManager taskManager;
    private Transport.ResponseHandlers responseHandlers;
    private Transport.RequestHandlers requestHandlers;
    private InboundHandler handler;
    private OutboundHandler outboundHandler;
    private FakeTcpChannel channel;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        channel = new FakeTcpChannel(randomBoolean(), buildNewFakeTransportAddress().address(), buildNewFakeTransportAddress().address()) {
            public void sendMessage(BytesReference reference, org.opensearch.action.ActionListener<Void> listener) {
                super.sendMessage(reference, listener);
                if (listener != null) {
                    listener.onResponse(null);
                }
            }
        };
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        TransportHandshaker handshaker = new TransportHandshaker(version, threadPool, (n, c, r, v) -> {});
        outboundHandler = new OutboundHandler(
            "node",
            version,
            new String[0],
            new StatsTracker(),
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE
        );
        TransportKeepAlive keepAlive = new TransportKeepAlive(threadPool, outboundHandler::sendBytes);
        requestHandlers = new Transport.RequestHandlers();
        responseHandlers = new Transport.ResponseHandlers();
        handler = new InboundHandler(
            threadPool,
            outboundHandler,
            namedWriteableRegistry,
            handshaker,
            keepAlive,
            requestHandlers,
            responseHandlers
        );
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testPing() throws Exception {
        AtomicReference<TransportChannel> channelCaptor = new AtomicReference<>();
        RequestHandlerRegistry<TestRequest> registry = new RequestHandlerRegistry<>(
            "test-request",
            TestRequest::new,
            taskManager,
            (request, channel, task) -> channelCaptor.set(channel),
            ThreadPool.Names.SAME,
            false,
            true
        );
        requestHandlers.registerHandler(registry);

        handler.inboundMessage(channel, new InboundMessage(null, true));
        if (channel.isServerChannel()) {
            BytesReference ping = channel.getMessageCaptor().get();
            assertEquals('E', ping.get(0));
            assertEquals(6, ping.length());
        }
    }

    public void testRequestAndResponse() throws Exception {
        String action = "test-request";
        int headerSize = TcpHeader.headerSize(version);
        boolean isError = randomBoolean();
        AtomicReference<TestRequest> requestCaptor = new AtomicReference<>();
        AtomicReference<TestResponse> responseCaptor = new AtomicReference<>();
        AtomicReference<Exception> exceptionCaptor = new AtomicReference<>();
        AtomicReference<TransportChannel> channelCaptor = new AtomicReference<>();

        long requestId = responseHandlers.add(new Transport.ResponseContext<>(new TransportResponseHandler<TestResponse>() {
            @Override
            public void handleResponse(TestResponse response) {
                responseCaptor.set(response);
            }

            @Override
            public void handleException(TransportException exp) {
                exceptionCaptor.set(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        }, null, action));
        RequestHandlerRegistry<TestRequest> registry = new RequestHandlerRegistry<>(
            action,
            TestRequest::new,
            taskManager,
            (request, channel, task) -> {
                channelCaptor.set(channel);
                requestCaptor.set(request);
            },
            ThreadPool.Names.SAME,
            false,
            true
        );
        requestHandlers.registerHandler(registry);
        String requestValue = randomAlphaOfLength(10);
        OutboundMessage.Request request = new OutboundMessage.Request(
            threadPool.getThreadContext(),
            new String[0],
            new TestRequest(requestValue),
            version,
            action,
            requestId,
            false,
            false
        );

        BytesReference fullRequestBytes = request.serialize(new BytesStreamOutput());
        BytesReference requestContent = fullRequestBytes.slice(headerSize, fullRequestBytes.length() - headerSize);
        Header requestHeader = new Header(fullRequestBytes.length() - 6, requestId, TransportStatus.setRequest((byte) 0), version);
        InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(requestContent), () -> {});
        requestHeader.finishParsingHeader(requestMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, requestMessage);

        TransportChannel transportChannel = channelCaptor.get();
        assertEquals(Version.CURRENT, transportChannel.getVersion());
        assertEquals("transport", transportChannel.getChannelType());
        assertEquals(requestValue, requestCaptor.get().value);

        String responseValue = randomAlphaOfLength(10);
        byte responseStatus = TransportStatus.setResponse((byte) 0);
        if (isError) {
            responseStatus = TransportStatus.setError(responseStatus);
            transportChannel.sendResponse(new OpenSearchException("boom"));
        } else {
            transportChannel.sendResponse(new TestResponse(responseValue));
        }

        BytesReference fullResponseBytes = channel.getMessageCaptor().get();
        BytesReference responseContent = fullResponseBytes.slice(headerSize, fullResponseBytes.length() - headerSize);
        Header responseHeader = new Header(fullResponseBytes.length() - 6, requestId, responseStatus, version);
        InboundMessage responseMessage = new InboundMessage(responseHeader, ReleasableBytesReference.wrap(responseContent), () -> {});
        responseHeader.finishParsingHeader(responseMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, responseMessage);

        if (isError) {
            assertThat(exceptionCaptor.get(), instanceOf(RemoteTransportException.class));
            assertThat(exceptionCaptor.get().getCause(), instanceOf(OpenSearchException.class));
            assertEquals("boom", exceptionCaptor.get().getCause().getMessage());
        } else {
            assertEquals(responseValue, responseCaptor.get().value);
        }
    }

    public void testSendsErrorResponseToHandshakeFromCompatibleVersion() throws Exception {
        // Nodes use their minimum compatibility version for the TCP handshake, so a node from v(major-1).x will report its version as
        // v(major-2).last in the TCP handshake, with which we are not really compatible. We put extra effort into making sure that if
        // successful we can respond correctly in a format this old, but we do not guarantee that we can respond correctly with an error
        // response. However if the two nodes are from the same major version then we do guarantee compatibility of error responses.

        final Version remoteVersion = VersionUtils.randomCompatibleVersion(random(), version);
        final long requestId = randomNonNegativeLong();
        final Header requestHeader = new Header(
            between(0, 100),
            requestId,
            TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)),
            remoteVersion
        );
        final InboundMessage requestMessage = unreadableInboundHandshake(remoteVersion, requestHeader);
        requestHeader.actionName = TransportHandshaker.HANDSHAKE_ACTION_NAME;
        requestHeader.headers = Tuple.tuple(Map.of(), Map.of());
        requestHeader.features = Set.of();
        handler.inboundMessage(channel, requestMessage);

        final BytesReference responseBytesReference = channel.getMessageCaptor().get();
        final Header responseHeader = InboundDecoder.readHeader(remoteVersion, responseBytesReference.length(), responseBytesReference);
        assertTrue(responseHeader.isResponse());
        assertTrue(responseHeader.isError());
    }

    public void testClosesChannelOnErrorInHandshakeWithIncompatibleVersion() throws Exception {
        // Nodes use their minimum compatibility version for the TCP handshake, so a node from v(major-1).x will report its version as
        // v(major-2).last in the TCP handshake, with which we are not really compatible. We put extra effort into making sure that if
        // successful we can respond correctly in a format this old, but we do not guarantee that we can respond correctly with an error
        // response so we must just close the connection on an error. To avoid the failure disappearing into a black hole we at least log
        // it.

        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(InboundHandler.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "expected message",
                    InboundHandler.class.getCanonicalName(),
                    Level.WARN,
                    "could not send error response to handshake"
                )
            );

            final AtomicBoolean isClosed = new AtomicBoolean();
            channel.addCloseListener(ActionListener.wrap(() -> assertTrue(isClosed.compareAndSet(false, true))));

            final Version remoteVersion = Version.fromId(randomIntBetween(0, version.minimumCompatibilityVersion().id - 1));
            final long requestId = randomNonNegativeLong();
            final Header requestHeader = new Header(
                between(0, 100),
                requestId,
                TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)),
                remoteVersion
            );
            final InboundMessage requestMessage = unreadableInboundHandshake(remoteVersion, requestHeader);
            requestHeader.actionName = TransportHandshaker.HANDSHAKE_ACTION_NAME;
            requestHeader.headers = Tuple.tuple(Map.of(), Map.of());
            requestHeader.features = Set.of();
            handler.inboundMessage(channel, requestMessage);
            assertTrue(isClosed.get());
            assertNull(channel.getMessageCaptor().get());
            mockAppender.assertAllExpectationsMatched();
        }
    }

    public void testLogsSlowInboundProcessing() throws Exception {
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(InboundHandler.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "expected message",
                    InboundHandler.class.getCanonicalName(),
                    Level.WARN,
                    "handling inbound transport message "
                )
            );

            handler.setSlowLogThreshold(TimeValue.timeValueMillis(5L));
            final Version remoteVersion = Version.CURRENT;
            final long requestId = randomNonNegativeLong();
            final Header requestHeader = new Header(
                between(0, 100),
                requestId,
                TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)),
                remoteVersion
            );
            final InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(BytesArray.EMPTY), () -> {
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
            requestHeader.actionName = TransportHandshaker.HANDSHAKE_ACTION_NAME;
            requestHeader.headers = Tuple.tuple(Collections.emptyMap(), Collections.emptyMap());
            requestHeader.features = Set.of();
            handler.inboundMessage(channel, requestMessage);
            assertNotNull(channel.getMessageCaptor().get());
            mockAppender.assertAllExpectationsMatched();
        }
    }

    public void testRequestNotFullyRead() throws Exception {
        String action = "test-request";
        int headerSize = TcpHeader.headerSize(version);
        AtomicReference<Exception> exceptionCaptor = new AtomicReference<>();

        long requestId = responseHandlers.add(new Transport.ResponseContext<>(new TransportResponseHandler<TestResponse>() {
            @Override
            public void handleResponse(TestResponse response) {}

            @Override
            public void handleException(TransportException exp) {
                exceptionCaptor.set(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        }, null, action));

        RequestHandlerRegistry<TestRequest> registry = new RequestHandlerRegistry<>(
            action,
            TestRequest::new,
            taskManager,
            (request, channel, task) -> {},
            ThreadPool.Names.SAME,
            false,
            true
        );

        requestHandlers.registerHandler(registry);
        String requestValue = randomAlphaOfLength(10);
        OutboundMessage.Request request = new OutboundMessage.Request(
            threadPool.getThreadContext(),
            new String[0],
            new TestRequest(requestValue),
            version,
            action,
            requestId,
            false,
            false
        );

        outboundHandler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, Exception error) {
                exceptionCaptor.set(error);
            }
        });

        // Create the request payload with 1 byte overflow
        final BytesRef bytes = request.serialize(new BytesStreamOutput()).toBytesRef();
        final ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 1);
        buffer.put(bytes.bytes, 0, bytes.length);
        buffer.put((byte) 1);

        BytesReference fullRequestBytes = BytesReference.fromByteBuffer((ByteBuffer) buffer.flip());
        BytesReference requestContent = fullRequestBytes.slice(headerSize, fullRequestBytes.length() - headerSize);
        Header requestHeader = new Header(fullRequestBytes.length() - 6, requestId, TransportStatus.setRequest((byte) 0), version);
        InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(requestContent), () -> {});
        requestHeader.finishParsingHeader(requestMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, requestMessage);

        assertThat(exceptionCaptor.get(), instanceOf(IllegalStateException.class));
        assertThat(exceptionCaptor.get().getMessage(), startsWith("Message not fully read (request) for requestId"));
    }

    public void testRequestFullyReadButMoreDataIsAvailable() throws Exception {
        String action = "test-request";
        int headerSize = TcpHeader.headerSize(version);
        AtomicReference<Exception> exceptionCaptor = new AtomicReference<>();

        long requestId = responseHandlers.add(new Transport.ResponseContext<>(new TransportResponseHandler<TestResponse>() {
            @Override
            public void handleResponse(TestResponse response) {}

            @Override
            public void handleException(TransportException exp) {
                exceptionCaptor.set(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        }, null, action));

        RequestHandlerRegistry<TestRequest> registry = new RequestHandlerRegistry<>(
            action,
            TestRequest::new,
            taskManager,
            (request, channel, task) -> {},
            ThreadPool.Names.SAME,
            false,
            true
        );

        requestHandlers.registerHandler(registry);
        String requestValue = randomAlphaOfLength(10);
        OutboundMessage.Request request = new OutboundMessage.Request(
            threadPool.getThreadContext(),
            new String[0],
            new TestRequest(requestValue),
            version,
            action,
            requestId,
            false,
            false
        );

        outboundHandler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, Exception error) {
                exceptionCaptor.set(error);
            }
        });

        final BytesReference fullRequestBytes = request.serialize(new BytesStreamOutput());
        // Create the request payload by intentionally stripping 1 byte away
        BytesReference requestContent = fullRequestBytes.slice(headerSize, fullRequestBytes.length() - headerSize - 1);
        Header requestHeader = new Header(fullRequestBytes.length() - 6, requestId, TransportStatus.setRequest((byte) 0), version);
        InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(requestContent), () -> {});
        requestHeader.finishParsingHeader(requestMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, requestMessage);

        assertThat(exceptionCaptor.get(), instanceOf(IllegalStateException.class));
        assertThat(exceptionCaptor.get().getCause(), instanceOf(EOFException.class));
        assertThat(exceptionCaptor.get().getMessage(), startsWith("Message fully read (request) but more data is expected for requestId"));
    }

    public void testResponseNotFullyRead() throws Exception {
        String action = "test-request";
        int headerSize = TcpHeader.headerSize(version);
        AtomicReference<TestRequest> requestCaptor = new AtomicReference<>();
        AtomicReference<Exception> exceptionCaptor = new AtomicReference<>();
        AtomicReference<TestResponse> responseCaptor = new AtomicReference<>();
        AtomicReference<TransportChannel> channelCaptor = new AtomicReference<>();

        long requestId = responseHandlers.add(new Transport.ResponseContext<>(new TransportResponseHandler<TestResponse>() {
            @Override
            public void handleResponse(TestResponse response) {
                responseCaptor.set(response);
            }

            @Override
            public void handleException(TransportException exp) {
                exceptionCaptor.set(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        }, null, action));
        RequestHandlerRegistry<TestRequest> registry = new RequestHandlerRegistry<>(
            action,
            TestRequest::new,
            taskManager,
            (request, channel, task) -> {
                channelCaptor.set(channel);
                requestCaptor.set(request);
            },
            ThreadPool.Names.SAME,
            false,
            true
        );
        requestHandlers.registerHandler(registry);
        String requestValue = randomAlphaOfLength(10);
        OutboundMessage.Request request = new OutboundMessage.Request(
            threadPool.getThreadContext(),
            new String[0],
            new TestRequest(requestValue),
            version,
            action,
            requestId,
            false,
            false
        );

        BytesReference fullRequestBytes = request.serialize(new BytesStreamOutput());
        BytesReference requestContent = fullRequestBytes.slice(headerSize, fullRequestBytes.length() - headerSize);
        Header requestHeader = new Header(fullRequestBytes.length() - 6, requestId, TransportStatus.setRequest((byte) 0), version);
        InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(requestContent), () -> {});
        requestHeader.finishParsingHeader(requestMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, requestMessage);

        TransportChannel transportChannel = channelCaptor.get();
        assertEquals(Version.CURRENT, transportChannel.getVersion());
        assertEquals("transport", transportChannel.getChannelType());
        assertEquals(requestValue, requestCaptor.get().value);

        String responseValue = randomAlphaOfLength(10);
        byte responseStatus = TransportStatus.setResponse((byte) 0);
        transportChannel.sendResponse(new TestResponse(responseValue));

        // Create the response payload with 1 byte overflow
        final BytesRef bytes = channel.getMessageCaptor().get().toBytesRef();
        final ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 1);
        buffer.put(bytes.bytes, 0, bytes.length);
        buffer.put((byte) 1);

        BytesReference fullResponseBytes = BytesReference.fromByteBuffer((ByteBuffer) buffer.flip());
        BytesReference responseContent = fullResponseBytes.slice(headerSize, fullResponseBytes.length() - headerSize);
        Header responseHeader = new Header(fullResponseBytes.length() - 6, requestId, responseStatus, version);
        InboundMessage responseMessage = new InboundMessage(responseHeader, ReleasableBytesReference.wrap(responseContent), () -> {});
        responseHeader.finishParsingHeader(responseMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, responseMessage);

        assertThat(exceptionCaptor.get(), instanceOf(RemoteTransportException.class));
        assertThat(exceptionCaptor.get().getCause(), instanceOf(TransportSerializationException.class));
        assertThat(exceptionCaptor.get().getMessage(), containsString("Failed to deserialize response from handler"));
    }

    public void testResponseFullyReadButMoreDataIsAvailable() throws Exception {
        String action = "test-request";
        int headerSize = TcpHeader.headerSize(version);
        AtomicReference<TestRequest> requestCaptor = new AtomicReference<>();
        AtomicReference<Exception> exceptionCaptor = new AtomicReference<>();
        AtomicReference<TestResponse> responseCaptor = new AtomicReference<>();
        AtomicReference<TransportChannel> channelCaptor = new AtomicReference<>();

        long requestId = responseHandlers.add(new Transport.ResponseContext<>(new TransportResponseHandler<TestResponse>() {
            @Override
            public void handleResponse(TestResponse response) {
                responseCaptor.set(response);
            }

            @Override
            public void handleException(TransportException exp) {
                exceptionCaptor.set(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        }, null, action));
        RequestHandlerRegistry<TestRequest> registry = new RequestHandlerRegistry<>(
            action,
            TestRequest::new,
            taskManager,
            (request, channel, task) -> {
                channelCaptor.set(channel);
                requestCaptor.set(request);
            },
            ThreadPool.Names.SAME,
            false,
            true
        );
        requestHandlers.registerHandler(registry);
        String requestValue = randomAlphaOfLength(10);
        OutboundMessage.Request request = new OutboundMessage.Request(
            threadPool.getThreadContext(),
            new String[0],
            new TestRequest(requestValue),
            version,
            action,
            requestId,
            false,
            false
        );

        BytesReference fullRequestBytes = request.serialize(new BytesStreamOutput());
        BytesReference requestContent = fullRequestBytes.slice(headerSize, fullRequestBytes.length() - headerSize);
        Header requestHeader = new Header(fullRequestBytes.length() - 6, requestId, TransportStatus.setRequest((byte) 0), version);
        InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(requestContent), () -> {});
        requestHeader.finishParsingHeader(requestMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, requestMessage);

        TransportChannel transportChannel = channelCaptor.get();
        assertEquals(Version.CURRENT, transportChannel.getVersion());
        assertEquals("transport", transportChannel.getChannelType());
        assertEquals(requestValue, requestCaptor.get().value);

        String responseValue = randomAlphaOfLength(10);
        byte responseStatus = TransportStatus.setResponse((byte) 0);
        transportChannel.sendResponse(new TestResponse(responseValue));

        BytesReference fullResponseBytes = channel.getMessageCaptor().get();
        // Create the response payload by intentionally stripping 1 byte away
        BytesReference responseContent = fullResponseBytes.slice(headerSize, fullResponseBytes.length() - headerSize - 1);
        Header responseHeader = new Header(fullResponseBytes.length() - 6, requestId, responseStatus, version);
        InboundMessage responseMessage = new InboundMessage(responseHeader, ReleasableBytesReference.wrap(responseContent), () -> {});
        responseHeader.finishParsingHeader(responseMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, responseMessage);

        assertThat(exceptionCaptor.get(), instanceOf(RemoteTransportException.class));
        assertThat(exceptionCaptor.get().getCause(), instanceOf(TransportSerializationException.class));
        assertThat(exceptionCaptor.get().getMessage(), containsString("Failed to deserialize response from handler"));
    }

    private static InboundMessage unreadableInboundHandshake(Version remoteVersion, Header requestHeader) {
        return new InboundMessage(requestHeader, ReleasableBytesReference.wrap(BytesArray.EMPTY), () -> {}) {
            @Override
            public StreamInput openOrGetStreamInput() {
                final StreamInput streamInput = new InputStreamStreamInput(new InputStream() {
                    @Override
                    public int read() {
                        throw new OpenSearchException("unreadable handshake");
                    }
                });
                streamInput.setVersion(remoteVersion);
                return streamInput;
            }
        };
    }

}

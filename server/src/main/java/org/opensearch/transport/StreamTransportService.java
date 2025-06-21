/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanBuilder;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.handler.TraceableTransportResponseHandler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

import static org.opensearch.discovery.HandshakingTransportAddressConnector.PROBE_CONNECT_TIMEOUT_SETTING;
import static org.opensearch.discovery.HandshakingTransportAddressConnector.PROBE_HANDSHAKE_TIMEOUT_SETTING;

/**
 * Transport service for streaming requests, handling StreamTransportResponse.
 *
 * @opensearch.internal
 */
public class StreamTransportService extends TransportService {
    private static final Logger logger = LogManager.getLogger(StreamTransportService.class);

    public StreamTransportService(
        Settings settings,
        Transport streamTransport,
        ThreadPool threadPool,
        TransportInterceptor transportInterceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders,
        Tracer tracer
    ) {
        super(
            settings,
            streamTransport,
            threadPool,
            transportInterceptor,
            localNodeFactory,
            clusterSettings,
            taskHeaders,
            new ClusterConnectionManager(
                ConnectionProfile.buildSingleChannelProfile(
                    TransportRequestOptions.Type.STREAM,
                    PROBE_CONNECT_TIMEOUT_SETTING.get(settings),
                    PROBE_HANDSHAKE_TIMEOUT_SETTING.get(settings),
                    TimeValue.MINUS_ONE,
                    false
                ),
                streamTransport
            ),
            tracer
        );
    }

    public <T extends TransportResponse> void handleStreamRequest(
        final DiscoveryNode node,
        final String action,
        final TransportRequest request,
        final TransportRequestOptions options,
        final TransportResponseHandler<T> handler
    ) {
        final Transport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (final NodeNotConnectedException ex) {
            handler.handleException(ex);
            return;
        }
        handleStreamRequest(connection, action, request, options, handler);
    }

    @Override
    public <T extends TransportResponse> void sendChildRequest(
        final Transport.Connection connection,
        final String action,
        final TransportRequest request,
        final Task parentTask,
        final TransportResponseHandler<T> handler
    ) {
        sendChildRequest(
            connection,
            action,
            request,
            parentTask,
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            handler
        );
    }

    public <T extends TransportResponse> void handleStreamRequest(
        final Transport.Connection connection,
        final String action,
        final TransportRequest request,
        final TransportRequestOptions options,
        final TransportResponseHandler<T> handler
    ) {
        final Span span = tracer.startSpan(SpanBuilder.from(action, connection));
        try (SpanScope spanScope = tracer.withSpanInScope(span)) {
            TransportResponseHandler<T> traceableTransportResponseHandler = TraceableTransportResponseHandler.create(handler, span, tracer);
            sendRequestAsync(connection, action, request, options, traceableTransportResponseHandler);
        }
    }

    @Override
    public void connectToNode(final DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Void> listener) {
        if (isLocalNode(node)) {
            listener.onResponse(null);
            return;
        }
        // TODO: add logic for validation
        connectionManager.connectToNode(node, connectionProfile, new ConnectionManager.ConnectionValidator() {
            @Override
            public void validate(Transport.Connection connection, ConnectionProfile profile, ActionListener<Void> listener) {
                listener.onResponse(null);
            }
        }, listener);
    }

    @Override
    protected void sendLocalRequest(long requestId, final String action, final TransportRequest request, TransportRequestOptions options) {
        final StreamDirectResponseChannel channel = new StreamDirectResponseChannel(localNode, action, requestId, this, threadPool);
        try {
            onRequestSent(localNode, requestId, action, request, options);
            onRequestReceived(requestId, action);
            final RequestHandlerRegistry reg = getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final String executor = reg.getExecutor();
            if (ThreadPool.Names.SAME.equals(executor)) {
                reg.processMessageReceived(request, channel);
            } else {
                threadPool.executor(executor).execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        reg.processMessageReceived(request, channel);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return reg.isForceExecution();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            logger.warn(
                                () -> new ParameterizedMessage("failed to notify channel of error message for action [{}]", action),
                                inner
                            );
                        }
                    }

                    @Override
                    public String toString() {
                        return "processing of [" + requestId + "][" + action + "]: " + request;
                    }
                });
            }
        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.warn(() -> new ParameterizedMessage("failed to notify channel of error message for action [{}]", action), inner);
            }
        }
    }

    /**
     * A channel for handling local streaming responses in StreamTransportService.
     *
     * @opensearch.internal
     */
    class StreamDirectResponseChannel implements TransportChannel {
        private static final Logger logger = LogManager.getLogger(StreamDirectResponseChannel.class);
        private static final String DIRECT_RESPONSE_PROFILE = ".direct";

        private final DiscoveryNode localNode;
        private final String action;
        private final long requestId;
        private final StreamTransportService service;
        private final ThreadPool threadPool;

        public StreamDirectResponseChannel(
            DiscoveryNode localNode,
            String action,
            long requestId,
            StreamTransportService service,
            ThreadPool threadPool
        ) {
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.service = service;
            this.threadPool = threadPool;
        }

        @Override
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public String getChannelType() {
            return "direct";
        }

        @Override
        public Version getVersion() {
            return localNode.getVersion();
        }

        @Override
        public void sendResponseBatch(TransportResponse response) {}

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            throw new UnsupportedOperationException("StreamTransportService cannot send non-stream responses");
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
            service.onResponseSent(requestId, action, exception);
            final TransportResponseHandler<?> handler = service.responseHandlers.onResponseReceived(requestId, service);
            if (handler != null) {
                final RemoteTransportException rtx = wrapInRemote(exception);
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processException(handler, rtx);
                } else {
                    threadPool.executor(executor).execute(() -> processException(handler, rtx));
                }
            }
        }

        private RemoteTransportException wrapInRemote(Exception e) {
            if (e instanceof RemoteTransportException) {
                return (RemoteTransportException) e;
            }
            return new RemoteTransportException(localNode.getName(), localNode.getAddress(), action, e);
        }

        private void processException(final TransportResponseHandler<?> handler, final RemoteTransportException rtx) {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error(
                    () -> new ParameterizedMessage("failed to handle exception for action [{}], handler [{}]", action, handler),
                    e
                );
            }
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Header;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * TcpChannel implementation for Apache Arrow Flight client with async response handling.
 *
 * @opensearch.internal
 */
public class FlightClientChannel implements TcpChannel {
    private static final Logger logger = LogManager.getLogger(FlightClientChannel.class);
    private static final long SLOW_LOG_THRESHOLD_MS = 5000; // Configurable threshold for slow operations

    private final FlightClient client;
    private final DiscoveryNode node;
    private final Location location;
    private final boolean isServer;
    private final String profile;
    private final CompletableFuture<Void> connectFuture;
    private final CompletableFuture<Void> closeFuture;
    private final List<ActionListener<Void>> connectListeners;
    private final List<ActionListener<Void>> closeListeners;
    private final ChannelStats stats;
    private final Transport.ResponseHandlers responseHandlers;
    private final ThreadPool threadPool;
    private final TransportMessageListener messageListener;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final HeaderContext headerContext;
    private volatile boolean isClosed;

    /**
     * Constructs a new FlightClientChannel for handling Arrow Flight streams.
     *
     * @param client                 the Arrow Flight client
     * @param node                  the discovery node for this channel
     * @param location              the flight server location
     * @param headerContext         the context for header management
     * @param isServer              whether this is a server channel
     * @param profile               the channel profile
     * @param responseHandlers       the transport response handlers
     * @param threadPool            the thread pool for async operations
     * @param messageListener       the transport message listener
     * @param namedWriteableRegistry the registry for deserialization
     */
    public FlightClientChannel(
        FlightClient client,
        DiscoveryNode node,
        Location location,
        HeaderContext headerContext,
        boolean isServer,
        String profile,
        Transport.ResponseHandlers responseHandlers,
        ThreadPool threadPool,
        TransportMessageListener messageListener,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.client = client;
        this.node = node;
        this.location = location;
        this.headerContext = headerContext;
        this.isServer = isServer;
        this.profile = profile;
        this.responseHandlers = responseHandlers;
        this.threadPool = threadPool;
        this.messageListener = messageListener;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.connectFuture = new CompletableFuture<>();
        this.closeFuture = new CompletableFuture<>();
        this.connectListeners = new CopyOnWriteArrayList<>();
        this.closeListeners = new CopyOnWriteArrayList<>();
        this.stats = new ChannelStats();
        this.isClosed = false;

        initializeConnection();
    }

    /**
     * Initializes the connection and notifies listeners of the result.
     */
    private void initializeConnection() {
        try {
            connectFuture.complete(null);
            notifyListeners(connectListeners, connectFuture);
        } catch (Exception e) {
            connectFuture.completeExceptionally(e);
            notifyListeners(connectListeners, connectFuture);
        }
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        try {
            client.close();
            closeFuture.complete(null);
            notifyListeners(closeListeners, closeFuture);
        } catch (Exception e) {
            closeFuture.completeExceptionally(e);
            notifyListeners(closeListeners, closeFuture);
        }
    }

    @Override
    public boolean isServerChannel() {
        return isServer;
    }

    @Override
    public String getProfile() {
        return profile;
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeListeners.add(listener);
        if (closeFuture.isDone()) {
            notifyListener(listener, closeFuture);
        }
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {
        connectListeners.add(listener);
        if (connectFuture.isDone()) {
            notifyListener(listener, connectFuture);
        }
    }

    @Override
    public ChannelStats getChannelStats() {
        return stats;
    }

    @Override
    public boolean isOpen() {
        return !isClosed;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return null; // TODO: Derive from client if possible
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return new InetSocketAddress(location.getUri().getHost(), location.getUri().getPort());
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        if (!isOpen()) {
            listener.onFailure(new TransportException("FlightClientChannel is closed"));
            return;
        }
        try {
            Ticket ticket = serializeToTicket(reference);
            FlightTransportResponse<?> streamResponse = createStreamResponse(ticket);
            processStreamResponseAsync(streamResponse);
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(new TransportException("Failed to send message", e));
        }
    }

    /**
     * Creates a new FlightTransportResponse for the given ticket.
     *
     * @param ticket the ticket for the stream
     * @return a new FlightTransportResponse
     * @throws RuntimeException if stream creation fails
     */
    private FlightTransportResponse<?> createStreamResponse(Ticket ticket) {
        try {
            return new FlightTransportResponse<>(
                client,
                headerContext,
                ticket,
                namedWriteableRegistry
            );
        } catch (Exception e) {
            logger.error("Failed to create stream for ticket at [{}]", location, e);
            throw new RuntimeException("Failed to create stream", e);
        }
    }

    /**
     * Processes the stream response asynchronously using the thread pool.
     *
     * @param streamResponse the stream response to process
     */
    private void processStreamResponseAsync(FlightTransportResponse<?> streamResponse) {
        long startTime = threadPool.relativeTimeInMillis();
        threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME).execute(() -> {
            try {
                handleStreamResponse(streamResponse, startTime);
            } catch (Exception e) {
                handleStreamException(streamResponse, e, startTime);
            }
        });
    }

    /**
     * Handles the stream response by fetching the header and dispatching to the handler.
     *
     * @param streamResponse the stream response
     * @param startTime     the start time for logging slow operations
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void handleStreamResponse(FlightTransportResponse<?> streamResponse, long startTime) {
        Header header = streamResponse.currentHeader();
        if (header == null) {
            throw new IllegalStateException("Missing header for stream");
        }
        long requestId = header.getRequestId();
        TransportResponseHandler handler = responseHandlers.onResponseReceived(requestId, messageListener);
        if (handler == null) {
            streamResponse.close();
            throw new IllegalStateException("Missing handler for stream request [" + requestId + "].");
        }
        streamResponse.setHandler(handler);
        executeWithThreadContext(header, handler, streamResponse);
        logSlowOperation(startTime);
    }

    /**
     * Executes the handler with the appropriate thread context and executor.
     *
     * @param header        the header for the response
     * @param handler       the response handler
     * @param streamResponse the stream response
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void executeWithThreadContext(Header header, TransportResponseHandler handler, StreamTransportResponse<?> streamResponse) {
        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
            threadContext.setHeaders(header.getHeaders());
            String executor = handler.executor();
            if (ThreadPool.Names.SAME.equals(executor)) {
                try {
                    handler.handleStreamResponse(streamResponse);
                } finally {
                    try {
                        streamResponse.close();
                    } catch (IOException e) {
                        // Log the exception instead of throwing it
                        logger.error("Failed to close streamResponse", e);
                    }
                }
            } else {
                threadPool.executor(executor).execute(() -> {
                    try {
                        handler.handleStreamResponse(streamResponse);
                    } finally {
                        try {
                            streamResponse.close();
                        } catch (IOException e) {
                            // Log the exception instead of throwing it
                            logger.error("Failed to close streamResponse", e);
                        }
                    }
                });
            }
        }
    }

    /**
     * Handles exceptions during stream processing, notifying the appropriate handler.
     *
     * @param streamResponse the stream response
     * @param e             the exception
     * @param startTime     the start time for logging slow operations
     */
    private void handleStreamException(FlightTransportResponse<?> streamResponse, Exception e, long startTime) {
        try {
            Header header = streamResponse.currentHeader();
            if (header != null) {
                long requestId = header.getRequestId();
                logger.error("Failed to handle stream for requestId [{}]", requestId, e);
                TransportResponseHandler<?> handler = responseHandlers.onResponseReceived(requestId, messageListener);
                if (handler != null) {
                    handler.handleException(new TransportException(e));
                } else {
                    logger.error("No handler found for requestId [{}]", requestId);
                }
            } else {
                logger.error("Failed to handle stream, no header available", e);
            }
        } finally {
            streamResponse.close();
            logSlowOperation(startTime);
        }
    }

    private void logSlowOperation(long startTime) {
        long took = threadPool.relativeTimeInMillis() - startTime;
        if (took > SLOW_LOG_THRESHOLD_MS) {
            logger.warn("Stream handling took [{}ms], exceeding threshold [{}ms]", took, SLOW_LOG_THRESHOLD_MS);
        }
    }

    private void notifyListeners(List<ActionListener<Void>> listeners, CompletableFuture<Void> future) {
        for (ActionListener<Void> listener : listeners) {
            notifyListener(listener, future);
        }
    }

    private void notifyListener(ActionListener<Void> listener, CompletableFuture<Void> future) {
        if (future.isCompletedExceptionally()) {
            future.handle((result, ex) -> {
                listener.onFailure(ex instanceof Exception ? (Exception) ex : new Exception(ex));
                return null;
            });
        } else {
            listener.onResponse(null);
        }
    }

    private Ticket serializeToTicket(BytesReference reference) {
        byte[] data = Arrays.copyOfRange(((BytesArray) reference).array(), 0, reference.length());
        return new Ticket(data);
    }

    @Override
    public String toString() {
        return "FlightClientChannel{node=" + node.getId() +
            ", remoteAddress=" + getRemoteAddress() +
            ", profile=" + profile +
            ", isServer=" + isServer + '}';
    }
}

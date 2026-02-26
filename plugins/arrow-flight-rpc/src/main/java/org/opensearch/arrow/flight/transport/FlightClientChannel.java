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
import org.opensearch.arrow.flight.stats.FlightCallTracker;
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Header;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TcpChannel implementation for Flight client with async response handling.
 *
 */
class FlightClientChannel implements TcpChannel {
    private static final Logger logger = LogManager.getLogger(FlightClientChannel.class);
    private static final AtomicLong GLOBAL_CHANNEL_COUNTER = new AtomicLong();
    private final AtomicLong correlationIdGenerator = new AtomicLong();
    private final FlightClient client;
    private final DiscoveryNode node;
    private final BoundTransportAddress boundAddress;
    private final Location location;
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
    private final FlightStatsCollector statsCollector;
    private final FlightTransportConfig config;

    /**
     * Constructs a new FlightClientChannel for handling Arrow Flight streams.
     *
     * @param client                 the Arrow Flight client
     * @param node                   the discovery node for this channel
     * @param location               the flight server location
     * @param headerContext          the context for header management
     * @param profile                the channel profile
     * @param responseHandlers       the transport response handlers
     * @param threadPool             the thread pool for async operations
     * @param messageListener        the transport message listener
     * @param namedWriteableRegistry the registry for deserialization
     * @param statsCollector         the collector for flight statistics
     * @param config                 the shared transport configuration
     */
    public FlightClientChannel(
        BoundTransportAddress boundTransportAddress,
        FlightClient client,
        DiscoveryNode node,
        Location location,
        HeaderContext headerContext,
        String profile,
        Transport.ResponseHandlers responseHandlers,
        ThreadPool threadPool,
        TransportMessageListener messageListener,
        NamedWriteableRegistry namedWriteableRegistry,
        FlightStatsCollector statsCollector,
        FlightTransportConfig config
    ) {
        this.boundAddress = boundTransportAddress;
        this.client = client;
        this.node = node;
        this.location = location;
        this.headerContext = headerContext;
        this.profile = profile;
        this.responseHandlers = responseHandlers;
        this.threadPool = threadPool;
        this.messageListener = messageListener;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.statsCollector = statsCollector;
        this.config = config;
        this.connectFuture = new CompletableFuture<>();
        this.closeFuture = new CompletableFuture<>();
        this.connectListeners = new CopyOnWriteArrayList<>();
        this.closeListeners = new CopyOnWriteArrayList<>();
        this.stats = new ChannelStats();
        this.isClosed = false;
        // Initialize with timestamp + global counter to ensure uniqueness with multiple channels
        // Upper bits: timestamp, lower 20 bits: channel ID
        long channelId = GLOBAL_CHANNEL_COUNTER.incrementAndGet() & 0xFFFFF; // 20 bits for channel ID
        long initialValue = (System.currentTimeMillis() << 20) | channelId;
        this.correlationIdGenerator.set(initialValue);
        if (statsCollector != null) {
            statsCollector.incrementClientChannelsActive();
        }
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

        if (statsCollector != null) {
            statsCollector.decrementClientChannelsActive();
        }

        isClosed = true;
        closeFuture.complete(null);
        notifyListeners(closeListeners, closeFuture);
    }

    @Override
    public boolean isServerChannel() {
        return false;
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
        return boundAddress.publishAddress().address();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        try {
            return new InetSocketAddress(InetAddress.getByName(location.getUri().getHost()), location.getUri().getPort());
        } catch (Exception e) {
            throw new StreamException(StreamErrorCode.INTERNAL, "Failed to resolve remote address", e);
        }
    }

    @Override
    public void sendMessage(long requestId, BytesReference reference, ActionListener<Void> listener) {
        if (!isOpen()) {
            listener.onFailure(new StreamException(StreamErrorCode.UNAVAILABLE, "FlightClientChannel is closed"));
            return;
        }

        FlightCallTracker callTracker = null;
        if (statsCollector != null) {
            callTracker = statsCollector.createClientCallTracker();
            callTracker.recordRequestBytes(reference.length());
        }

        try {
            // ticket will contain the serialized headers
            Ticket ticket = serializeToTicket(reference);
            TransportResponseHandler<?> handler = responseHandlers.onResponseReceived(requestId, messageListener);
            long correlationId = correlationIdGenerator.incrementAndGet();

            if (callTracker != null) {
                handler = new MetricsTrackingResponseHandler<>(handler, callTracker);
            }

            FlightTransportResponse<?> streamResponse = new FlightTransportResponse<>(
                handler,
                correlationId,
                client,
                headerContext,
                ticket,
                namedWriteableRegistry,
                config
            );

            // Open stream and prefetch first batch, invoke handler when ready
            openStreamAndInvokeHandler(streamResponse);
            listener.onResponse(null);
        } catch (Exception e) {
            if (callTracker != null) {
                callTracker.recordCallEnd(StreamErrorCode.INTERNAL.name());
            }
            listener.onFailure(new StreamException(StreamErrorCode.INTERNAL, "Failed to send message", e));
        }
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        throw new IllegalStateException("sendMessage must be accompanied with requestId for FlightClientChannel, use the right variant.");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void openStreamAndInvokeHandler(FlightTransportResponse<?> streamResponse) {
        TransportResponseHandler handler = streamResponse.getHandler();
        String executor = handler.executor();

        if (ThreadPool.Names.SAME.equals(executor)) {
            logger.debug("Stream transport handler using SAME executor, which may cause blocking behavior");
        }

        var threadContext = threadPool.getThreadContext();
        CompletableFuture<Header> future = new CompletableFuture<>();
        streamResponse.openAndPrefetchAsync(future);

        future.whenComplete((header, error) -> {
            if (error != null) {
                handleStreamException(streamResponse, error instanceof Exception ? (Exception) error : new Exception(error));
                return;
            }

            Runnable task = () -> {
                try (var ignored = threadContext.stashContext()) {
                    if (header == null) {
                        handleStreamException(streamResponse, new StreamException(StreamErrorCode.INTERNAL, "Header is null"));
                    }
                    threadContext.setHeaders(header.getHeaders());
                    handler.handleStreamResponse(streamResponse);
                } catch (Exception e) {
                    cleanupStreamResponse(streamResponse);
                    throw e;
                }
            };

            if (ThreadPool.Names.SAME.equals(executor)) {
                task.run();
            } else {
                threadPool.executor(executor).execute(task);
            }
        });
    }

    private void cleanupStreamResponse(StreamTransportResponse<?> streamResponse) {
        try {
            streamResponse.close();
        } catch (IOException e) {
            logger.error("Failed to close stream response", e);
        }
    }

    private void handleStreamException(FlightTransportResponse<?> streamResponse, Exception exception) {
        logger.error("Exception while handling stream response", exception);
        try {
            cancelStream(streamResponse, exception);
            TransportResponseHandler<?> handler = streamResponse.getHandler();
            notifyHandlerOfException(handler, exception);
        } finally {
            cleanupStreamResponse(streamResponse);
        }
    }

    private void cancelStream(FlightTransportResponse<?> streamResponse, Exception cause) {
        try {
            streamResponse.cancel("Client-side exception: " + cause.getMessage(), cause);
        } catch (Exception cancelEx) {
            logger.warn("Failed to cancel stream after exception", cancelEx);
        }
    }

    private void notifyHandlerOfException(TransportResponseHandler<?> handler, Exception exception) {
        StreamException streamException;
        if (exception instanceof StreamException se) {
            streamException = se;
        } else {
            streamException = new StreamException(StreamErrorCode.INTERNAL, "Stream processing failed", exception);
        }

        String executor = handler.executor();

        if (ThreadPool.Names.SAME.equals(executor)) {
            safeHandleException(handler, streamException);
        } else {
            threadPool.executor(executor).execute(() -> safeHandleException(handler, streamException));
        }
    }

    private void safeHandleException(TransportResponseHandler<?> handler, StreamException exception) {
        try {
            handler.handleException(exception);
        } catch (Exception handlerEx) {
            logger.error("Handler failed to process exception", handlerEx);
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
                listener.onFailure(ex instanceof Exception exception ? exception : new Exception(ex));
                return null;
            });
        } else {
            listener.onResponse(null);
        }
    }

    private Ticket serializeToTicket(BytesReference reference) {
        return new Ticket(BytesReference.toBytes(reference));
    }

    @Override
    public String toString() {
        return "FlightClientChannel{node=" + node.getId() + ", remoteAddress=" + getRemoteAddress() + ", profile=" + profile + '}';
    }
}

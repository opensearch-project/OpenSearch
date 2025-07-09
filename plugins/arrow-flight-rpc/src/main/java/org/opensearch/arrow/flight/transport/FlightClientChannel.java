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
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Header;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
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
    private static final long SLOW_LOG_THRESHOLD_MS = 5000; // Configurable threshold for slow operations
    private final AtomicLong requestIdGenerator = new AtomicLong();
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
        FlightStatsCollector statsCollector
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
            throw new RuntimeException("Failed to resolve remote address", e);
        }
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        if (!isOpen()) {
            listener.onFailure(new TransportException("FlightClientChannel is closed"));
            return;
        }
        try {
            // ticket will contain the serialized headers
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
                requestIdGenerator.incrementAndGet(), // we can't use reqId directly since its already serialized; so generating a new on
                // for header correlation
                client,
                headerContext,
                ticket,
                namedWriteableRegistry,
                statsCollector
            );
        } catch (Exception e) {
            logger.error("Failed to create stream for ticket at [{}]: {}", location, e.getMessage());
            throw new TransportException("Failed to create stream", e);
        }
    }

    /**
     * Processes the stream response asynchronously using the thread pool.
     * This is necessary because Flight client callbacks may be on gRPC threads
     * which should not be blocked with OpenSearch processing.
     *
     * @param streamResponse the stream response to process
     */
    private void processStreamResponseAsync(FlightTransportResponse<?> streamResponse) {
        long startTime = threadPool.relativeTimeInMillis();
        threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME).execute(() -> {
            TransportResponseHandler<?> handler = null;
            try {
                handler = getAndValidateHandler(streamResponse);
                executeWithThreadContext(streamResponse.currentHeader(), handler, streamResponse, startTime);
            } catch (Exception e) {
                handleStreamException(streamResponse, handler, e, startTime);
            }
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private TransportResponseHandler<?> getAndValidateHandler(FlightTransportResponse<?> streamResponse) {
        Header header = streamResponse.currentHeader();
        if (header == null) {
            throw new TransportException("Missing header for stream");
        }

        long requestId = header.getRequestId();
        TransportResponseHandler handler = responseHandlers.onResponseReceived(requestId, messageListener);
        if (handler == null) {
            throw new TransportException("Missing handler for stream request [" + requestId + "].");
        }
        streamResponse.setHandler(handler);
        return handler;
    }

    /**
     * Executes the handler with the appropriate thread context and executor.
     * Ensures proper resource cleanup even on exceptions.
     *
     * @param header        the header for the response
     * @param handler       the response handler
     * @param streamResponse the stream response
     * @param startTime     the start time for performance tracking
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void executeWithThreadContext(
        Header header,
        TransportResponseHandler handler,
        StreamTransportResponse<?> streamResponse,
        long startTime
    ) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        final String executor = handler.executor();

        if (ThreadPool.Names.SAME.equals(executor)) {
            executeHandler(threadContext, header, handler, streamResponse, startTime);
        } else {
            threadPool.executor(executor).execute(() -> executeHandler(threadContext, header, handler, streamResponse, startTime));
        }
    }

    /**
     * Executes the handler with proper thread context management.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void executeHandler(
        ThreadContext threadContext,
        Header header,
        TransportResponseHandler handler,
        StreamTransportResponse<?> streamResponse,
        long startTime
    ) {
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.setHeaders(header.getHeaders());
            handler.handleStreamResponse(streamResponse);
        } catch (Exception e) {
            cleanupStreamResponse(streamResponse);
            logSlowOperation(startTime);
            throw e;
        }
    }

    /**
     * Cleanup stream response resources and update stats.
     * This method ensures resources are always cleaned up, even if close() fails.
     */
    private void cleanupStreamResponse(StreamTransportResponse<?> streamResponse) {
        try {
            streamResponse.close();
        } catch (IOException e) {
            logger.error("Failed to close stream response", e);
        }
    }

    /**
     * Handles exceptions during stream processing, notifying the appropriate handler.
     * Ensures proper resource cleanup and error propagation.
     *
     * @param streamResponse the stream response
     * @param handler       the handler (may be null if exception occurred before handler retrieval)
     * @param exception     the exception that occurred
     * @param startTime     the start time for logging slow operations
     */
    private void handleStreamException(
        FlightTransportResponse<?> streamResponse,
        TransportResponseHandler<?> handler,
        Exception exception,
        long startTime
    ) {
        logger.error("Exception while handling stream response", exception);

        try {
            cancelStream(streamResponse, exception);
            if (handler != null) {
                notifyHandlerOfException(handler, exception);
            } else {
                logger.warn("Cannot notify handler of exception - handler not available");
            }
        } finally {
            cleanupStreamResponse(streamResponse);
            logSlowOperation(startTime);
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
        TransportException transportException = new TransportException("Stream processing failed", exception);
        String executor = handler.executor();

        if (ThreadPool.Names.SAME.equals(executor)) {
            safeHandleException(handler, transportException);
        } else {
            threadPool.executor(executor).execute(() -> safeHandleException(handler, transportException));
        }
    }

    private void safeHandleException(TransportResponseHandler<?> handler, TransportException exception) {
        try {
            handler.handleException(exception);
        } catch (Exception handlerEx) {
            logger.error("Handler failed to process exception", handlerEx);
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
        return "FlightClientChannel{node=" + node.getId() + ", remoteAddress=" + getRemoteAddress() + ", profile=" + profile + '}';
    }
}

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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private volatile boolean isClosed;
    private final FlightStatsCollector statsCollector;
    private final FlightTransportConfig config;
    /**
     * Streams created by this channel whose buffers may still be accounted against the client's
     * allocator. Entries remove themselves via {@link FlightTransportResponse#setOnClosed} once
     * their underlying flight stream is closed. {@link #close()} cancels every remaining stream
     * and waits for the set to drain before closing the {@link FlightClient}, whose allocator
     * close treats any outstanding buffer as a leak.
     */
    private final Set<FlightTransportResponse<?>> activeStreams = ConcurrentHashMap.newKeySet();
    /** Signalled whenever a stream leaves {@link #activeStreams}, to wake {@link #close()}. */
    private final Object activeStreamsMonitor = new Object();

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
        if (closeStarted.compareAndSet(false, true) == false) {
            return;
        }

        if (statsCollector != null) {
            statsCollector.decrementClientChannelsActive();
        }

        isClosed = true;

        // The FlightClient's allocator close treats any outstanding buffer as a leak, and live
        // streams legitimately hold buffers (current batch root, retained heartbeat metadata)
        // until their consumer closes them. So: cancel every active stream — FlightStream.cancel
        // is safe from this thread and unblocks consumers parked in nextResponse()
        for (FlightTransportResponse<?> streamResponse : activeStreams) {
            try {
                streamResponse.cancelStreamOnly("channel to node [" + node.getId() + "] closed");
            } catch (Exception e) {
                logger.warn(
                    () -> new ParameterizedMessage("Error cancelling active stream while closing channel to node [{}]", node.getId()),
                    e
                );
            }
        }
        awaitActiveStreamsClosed();

        closeFuture.complete(null);
        notifyListeners(closeListeners, closeFuture);
        try {
            client.close();
        } catch (Exception e) {
            logger.warn("Failed to close FlightClient for node [" + node.getId() + "]", e);
        }
    }

    /**
     * Waits up to {@link FlightTransportConfig#getStreamCloseTimeout()} for active streams to be
     * released by their consumers (each removes itself from {@link #activeStreams} once its buffers
     * are released). On timeout, proceeds anyway — the subsequent {@link FlightClient#close()} may
     * then report the stragglers' buffers as leaked, which is logged, not thrown.
     *
     * <p>Streams whose consumer callback is running on this very thread are excluded from the wait:
     * their release cannot happen until the callback returns, which cannot happen until this close
     * returns, so waiting for them would burn the whole timeout to no effect.
     */
    private void awaitActiveStreamsClosed() {
        final long timeoutMillis = config.getStreamCloseTimeout().millis();
        final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (activeStreamsMonitor) {
            long remainingNanos;
            while (hasStreamsToAwait() && (remainingNanos = deadlineNanos - System.nanoTime()) > 0) {
                try {
                    // Wake on the next release, or at the deadline; never wait(0), which waits forever.
                    activeStreamsMonitor.wait(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        Thread current = Thread.currentThread();
        List<FlightTransportResponse<?>> remaining = new ArrayList<>(activeStreams);
        long selfOwned = remaining.stream().filter(s -> s.getDispatchThread() == current).count();
        long straggling = remaining.size() - selfOwned;
        if (straggling > 0) {
            logger.warn(
                "Gave up after [{}ms] waiting for [{}] active stream(s) to be released while closing channel to node [{}]; "
                    + "their buffers may be reported as leaked",
                timeoutMillis,
                straggling,
                node.getId()
            );
        }
        if (selfOwned > 0) {
            logger.warn(
                "Channel to node [{}] is being closed from a stream consumer callback; [{}] stream(s) owned by this thread "
                    + "cannot be released before close returns and their buffers may be reported as leaked",
                node.getId(),
                selfOwned
            );
        }
    }

    /**
     * Whether any active stream can still be released while this close is waiting — that is, any
     * stream not owned by the calling thread. See {@link #awaitActiveStreamsClosed()}.
     */
    private boolean hasStreamsToAwait() {
        Thread current = Thread.currentThread();
        for (FlightTransportResponse<?> streamResponse : activeStreams) {
            if (streamResponse.getDispatchThread() != current) {
                return true;
            }
        }
        return false;
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

            // Track the stream until its buffers are released, so close() can wait for it before
            // closing the FlightClient. Set the callback before any consumer can close the stream.
            streamResponse.setOnClosed(() -> {
                activeStreams.remove(streamResponse);
                synchronized (activeStreamsMonitor) {
                    activeStreamsMonitor.notifyAll();
                }
            });
            activeStreams.add(streamResponse);
            if (isOpen() == false) {
                // close() may have iterated activeStreams before this add; make sure this stream
                // is cancelled rather than left running against a closing client. The response
                // handler was already removed from responseHandlers above, so notify it directly
                // rather than leaving the request without a terminal callback.
                StreamException exception = new StreamException(StreamErrorCode.UNAVAILABLE, "FlightClientChannel is closed");
                streamResponse.cancelStreamOnly("channel to node [" + node.getId() + "] closed");
                notifyHandlerOfException(handler, exception);
                listener.onFailure(exception);
                return;
            }

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
                // While the consumer callback runs, this thread owns the stream: it is the only one
                // that can release it, so a close() reaching this thread must not wait for it.
                try (var dispatchMark = streamResponse.markDispatchThread()) {
                    try (var ignored = threadContext.stashContext()) {
                        if (header == null) {
                            // Must return: handleStreamException does not throw, so falling through here
                            // would NPE on getHeaders() below and mask the real failure. A null header is
                            // reachable whenever the middleware never stored one (HeaderContext.getHeader
                            // is a plain map remove), e.g. a call closed before its headers arrived.
                            handleStreamException(streamResponse, new StreamException(StreamErrorCode.INTERNAL, "Header is null"));
                            return;
                        }
                        threadContext.setHeaders(header.getHeaders());
                        handler.handleStreamResponse(streamResponse);
                    } catch (Exception e) {
                        cleanupStreamResponse(streamResponse);
                        throw e;
                    }
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
            logFailure("Failed to close stream response", e);
        }
    }

    private void handleStreamException(FlightTransportResponse<?> streamResponse, Exception exception) {
        logFailure("Exception while handling stream response for correlationId [" + streamResponse.getCorrelationId() + "]", exception);
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
            logFailure("Failed to cancel stream after exception", cancelEx);
        }
    }

    /**
     * Logs a per-stream failure as a one-line summary at ERROR, with the full stack trace available
     * only at TRACE.
     *
     * <p><b>Never hand a throwable to log4j from these paths.</b> They run on the per-stream prefetch
     * virtual thread started by {@link FlightTransportResponse#openAndPrefetchAsync}, and letting log4j
     * render a stack trace there can wedge the whole node:
     *
     * <ol>
     *   <li>OpenSearch's JSON layout always appends {@code %exceptionAsJson}, so a logged throwable reaches
     *       log4j's <em>extended</em> stack-trace renderer, which annotates every frame with its source JAR.</li>
     *   <li>To do that it resolves each frame's declaring class via {@code Class.forName}. The resulting
     *       {@code forName0} <em>native</em> frame sits on the stack while the classloader monitor is
     *       contended, and a continuation carrying a native frame cannot be unmounted. So the virtual thread
     *       pins its carrier instead of yielding it, even on JDK 25 where JEP 491 lets {@code synchronized}
     *       blocking unmount.</li>
     *   <li>The scheduler's carrier count defaults to {@code availableProcessors}. A mass stream failure
     *       (for example every stream reconnecting at once after a network partition heals) can therefore pin
     *       every carrier, while the thread holding the classloader lock is itself unmounted and can never be
     *       rescheduled to release it. That circular wait does not resolve: the node keeps reporting healthy
     *       while making no progress.</li>
     * </ol>
     *
     * <p>The TRACE line is safe for the same reason: it passes a pre-rendered string, so log4j never sees a
     * throwable and the extended renderer never runs. {@link ExceptionsHelper#stackTrace} only formats frames
     * that were captured when the throwable was constructed, resolving no classes and taking no lock.
     */
    private void logFailure(String message, Throwable cause) {
        logger.error("{}: {}", message, FlightUtils.causeSummary(cause));
        if (logger.isTraceEnabled()) {
            logger.trace("{}: {}", message, ExceptionsHelper.stackTrace(cause));
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
            // Runs on the prefetch virtual thread when the handler declares the SAME executor.
            logFailure("Handler failed to process exception", handlerEx);
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

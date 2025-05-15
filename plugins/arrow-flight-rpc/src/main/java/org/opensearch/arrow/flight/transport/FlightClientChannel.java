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
import org.opensearch.Version;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Header;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * TcpChannel implementation for Arrow Flight client with inbound response handling.
 *
 * @opensearch.internal
 */
public class FlightClientChannel implements TcpChannel {
    private static final Logger logger = LogManager.getLogger(FlightClientChannel.class);

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
    private volatile boolean isClosed;
    private final Transport.ResponseHandlers responseHandlers;
    private final ThreadPool threadPool;
    private final TransportMessageListener messageListener;
    private final Version version;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public FlightClientChannel(
        FlightClient client,
        DiscoveryNode node,
        Location location,
        boolean isServer,
        String profile,
        Transport.ResponseHandlers responseHandlers,
        ThreadPool threadPool,
        TransportMessageListener messageListener,
        Version version,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.client = client;
        this.node = node;
        this.location = location;
        this.isServer = isServer;
        this.profile = profile;
        this.responseHandlers = responseHandlers;
        this.threadPool = threadPool;
        this.messageListener = messageListener;
        this.version = version;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.connectFuture = new CompletableFuture<>();
        this.closeFuture = new CompletableFuture<>();
        this.connectListeners = new CopyOnWriteArrayList<>();
        this.closeListeners = new CopyOnWriteArrayList<>();
        this.stats = new ChannelStats();
        this.isClosed = false;

        try {
            connectFuture.complete(null);
            notifyConnectListeners();
        } catch (Exception e) {
            connectFuture.completeExceptionally(e);
            notifyConnectListeners();
        }
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            try {
                client.close();
                closeFuture.complete(null);
                notifyCloseListeners();
            } catch (Exception e) {
                closeFuture.completeExceptionally(e);
                notifyCloseListeners();
            }
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
            listener.onFailure(new TransportException("Channel is closed"));
            return;
        }
        try {
            Ticket ticket = serializeToTicket(reference);
            handleInboundStream(ticket, listener);
        } catch (Exception e) {
            listener.onFailure(new TransportException("Failed to send message", e));
        }
    }

    /**
     * Handles inbound streaming responses for the given ticket.
     *
     * @param ticket the Ticket for the stream
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void handleInboundStream(Ticket ticket, ActionListener<Void> listener) {
        if (!isOpen()) {
            logger.warn("Cannot handle inbound stream; channel is closed");
            return;
        }
        // unblock client thread; response handling is done async using FlightClient's thread pool
        threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME).execute(() -> {
            long startTime = threadPool.relativeTimeInMillis();
            ThreadContext threadContext = threadPool.getThreadContext();
            final FlightTransportResponse<? extends TransportResponse> streamResponse = new FlightTransportResponse<>(
                client,
                ticket,
                version,
                namedWriteableRegistry
            );
            try {
                Header header = streamResponse.currentHeader();
                if (header == null) {
                    throw new IOException("Missing header for stream");
                }
                long requestId = header.getRequestId();
                TransportResponseHandler handler = responseHandlers.onResponseReceived(requestId, messageListener);
                if (handler == null) {
                    logger.error("No handler found for requestId [{}]", requestId);
                    return;
                }
                streamResponse.setHandler(handler);
                try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
                    threadContext.setHeaders(header.getHeaders());
                    // remote cluster logic not needed
                    // threadContext.putTransient("_remote_address", getRemoteAddress());
                    final String executor = handler.executor();
                    if (ThreadPool.Names.SAME.equals(executor)) {
                        try {
                            handler.handleStreamResponse(streamResponse);
                        } finally {
                            streamResponse.close();
                        }
                    } else {
                        threadPool.executor(executor).execute(() -> {
                            try {
                                handler.handleStreamResponse(streamResponse);
                            } finally {
                                streamResponse.close();
                            }
                        });
                    }
                }
            } catch (Exception e) {
                streamResponse.close();
                logger.error("Failed to handle inbound stream for ticket [{}]", ticket, e);
            } finally {
                long took = threadPool.relativeTimeInMillis() - startTime;
                long slowLogThresholdMs = 5000; // TODO: Configure
                if (took > slowLogThresholdMs) {
                    logger.warn("Handling inbound stream took [{}ms], exceeding threshold [{}ms]", took, slowLogThresholdMs);
                }
            }
        });
        listener.onResponse(null);
    }

    @Override
    public <T> Optional<T> get(String name, Class<T> clazz) {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "FlightClientChannel{"
            + "node="
            + node.getId()
            + ", remoteAddress="
            + getRemoteAddress()
            + ", profile="
            + profile
            + ", isServer="
            + isServer
            + '}';
    }

    private void notifyConnectListeners() {
        notifyListeners(connectListeners, connectFuture);
    }

    private void notifyCloseListeners() {
        notifyListeners(closeListeners, closeFuture);
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
}

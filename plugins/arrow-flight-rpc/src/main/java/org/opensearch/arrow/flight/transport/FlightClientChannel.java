/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportException;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

public class FlightClientChannel implements TcpChannel {
    private static final Logger logger = LogManager.getLogger(FlightClientChannel.class);

    private final FlightClient client;
    private final DiscoveryNode node;
    private final Location location; // Store the Location used to create the client
    private final boolean isServer;
    private final String profile;
    private final CompletableFuture<Void> connectFuture;
    private final CompletableFuture<Void> closeFuture;
    private final List<ActionListener<Void>> connectListeners;
    private final List<ActionListener<Void>> closeListeners;
    private final ChannelStats stats;
    private volatile boolean isClosed;

    public FlightClientChannel(FlightClient client, DiscoveryNode node, Location location, boolean isServer, String profile) {
        this.client = client;
        this.node = node;
        this.location = location;
        this.isServer = isServer;
        this.profile = profile;
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
        // FlightClient doesn't expose local address;
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
            // Send request via FlightClient.getStream
            FlightStream stream = client.getStream(ticket);
            listener.onResponse(null);
            // stats.markAccessed(threadPool.relativeTimeInMillis());
        } catch (Exception e) {
            listener.onFailure(new TransportException("Failed to send message", e));
        }
    }

    @Override
    public <T> Optional<T> get(String name, Class<T> clazz) {
        return Optional.empty(); // FlightClient doesn't expose pipeline handlers
    }

    @Override
    public String toString() {
        return "FlightTcpChannel{"
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

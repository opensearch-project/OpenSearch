/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.transport.TcpChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TcpChannel implementation for Arrow Flight, optimized for streaming responses with proper batch management.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FlightServerChannel implements TcpChannel {
    private static final String PROFILE_NAME = "flight";

    private final Logger logger = LogManager.getLogger(FlightServerChannel.class);
    private final ServerStreamListener serverStreamListener;
    private final BufferAllocator allocator;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final InetSocketAddress localAddress;
    private final InetSocketAddress remoteAddress;
    private final List<VectorSchemaRoot> pendingRoots = Collections.synchronizedList(new ArrayList<>());
    private final List<ActionListener<Void>> closeListeners = Collections.synchronizedList(new ArrayList<>());

    public FlightServerChannel(ServerStreamListener serverStreamListener, BufferAllocator allocator) {
        this.serverStreamListener = serverStreamListener;
        this.allocator = allocator;
        this.localAddress = new InetSocketAddress("localhost", 0);
        this.remoteAddress = new InetSocketAddress("localhost", 0);
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    /**
     * Sends a batch of data as a VectorSchemaRoot.
     *
     * @param root the VectorSchemaRoot to send, or null for empty batch
     * @param completionListener callback for completion or failure
     */
    public void sendBatch(VectorSchemaRoot root, ActionListener<Void> completionListener) {
        if (!open.get()) {
            if (root != null) {
                root.close();
            }
            completionListener.onFailure(new IOException("Channel is closed"));
            return;
        }
        try {
            if (!serverStreamListener.isReady()) {
                if (root != null) {
                    root.close();
                }
                completionListener.onFailure(new IOException("Client is not ready for batch"));
                return;
            }
            if (root == null) {
                // Empty batch: no data sent, signal completion
                completionListener.onResponse(null);
                return;
            }
            pendingRoots.add(root);
            serverStreamListener.start(root);
            serverStreamListener.putNext();
            completionListener.onResponse(null);
        } catch (Exception e) {
            if (root != null) {
                root.close();
            }
            completionListener.onFailure(new IOException("Failed to send batch", e));
        }
    }

    /**
     * Completes the streaming response and closes all pending roots.
     *
     * @param completionListener callback for completion or failure
     */
    public void completeStream(ActionListener<Void> completionListener) {
        if (!open.compareAndSet(true, false)) {
            completionListener.onResponse(null);
            return;
        }
        try {
            serverStreamListener.completed();
            closeStream();
            completionListener.onResponse(null);
            notifyCloseListeners();
        } catch (Exception e) {
            completionListener.onFailure(new IOException("Failed to complete stream", e));
        }
    }

    /**
     * Sends an error and closes the channel.
     *
     * @param error the error to send
     * @param completionListener callback for completion or failure
     */
    public void sendError(Exception error, ActionListener<Void> completionListener) {
        if (!open.compareAndSet(true, false)) {
            completionListener.onResponse(null);
            return;
        }
        try {
            serverStreamListener.error(
                CallStatus.INTERNAL.withCause(error)
                    .withDescription(error.getMessage() != null ? error.getMessage() : "Stream error")
                    .toRuntimeException()
            );
            closeStream();
            completionListener.onResponse(null);
            notifyCloseListeners();
        } catch (Exception e) {
            completionListener.onFailure(new IOException("Failed to send error", e));
        }
    }

    @Override
    public boolean isServerChannel() {
        return true;
    }

    @Override
    public String getProfile() {
        return PROFILE_NAME;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        listener.onFailure(new UnsupportedOperationException("FlightServerChannel does not support BytesReference"));
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {
        // Assume Arrow Flight is connected
        listener.onResponse(null);
    }

    @Override
    public ChannelStats getChannelStats() {
        return new ChannelStats(); // TODO: Implement stats if needed
    }

    @Override
    public void close() {
        if (open.compareAndSet(true, false)) {
            try {
                serverStreamListener.completed();
                closeStream();
                notifyCloseListeners();
            } catch (Exception e) {
                logger.warn("Error closing FlightServerChannel", e);
            }
        }
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        synchronized (closeListeners) {
            if (!open.get()) {
                listener.onResponse(null);
            } else {
                closeListeners.add(listener);
            }
        }
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    private void closeStream() {
        synchronized (pendingRoots) {
            for (VectorSchemaRoot root : pendingRoots) {
                if (root != null) {
                    root.close();
                }
            }
            pendingRoots.clear();
        }
    }

    private void notifyCloseListeners() {
        for (ActionListener<Void> listener : closeListeners) {
            listener.onResponse(null);
        }
        closeListeners.clear();
    }
}

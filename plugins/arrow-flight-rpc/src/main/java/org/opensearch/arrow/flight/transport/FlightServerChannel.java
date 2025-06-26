/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.flight.stream.ArrowStreamOutput;
import org.opensearch.arrow.flight.stream.VectorStreamOutput;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * TcpChannel implementation for Arrow Flight
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
    private final List<ActionListener<Void>> closeListeners = Collections.synchronizedList(new ArrayList<>());
    private final ServerHeaderMiddleware middleware;
    private final SetOnce<VectorSchemaRoot> root = new SetOnce<>();

    public FlightServerChannel(ServerStreamListener serverStreamListener, BufferAllocator allocator, ServerHeaderMiddleware middleware) {
        this.serverStreamListener = serverStreamListener;
        this.allocator = allocator;
        this.middleware = middleware;
        this.localAddress = new InetSocketAddress("localhost", 0);
        this.remoteAddress = new InetSocketAddress("localhost", 0);
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    /**
     * Sends a batch of data as a VectorSchemaRoot.
     *
     * @param output StreamOutput for the response
     * @param completionListener callback for completion or failure
     */
    public void sendBatch(ByteBuffer header, VectorStreamOutput output, ActionListener<Void> completionListener) {
        if (!open.get()) {
            throw new IllegalStateException("FlightServerChannel already closed.");
        }
        try {

            // Only set for the first batch
            if (root.get() == null) {
                middleware.setHeader(header);
                root.trySet(output.getRoot());
                serverStreamListener.start(root.get());
            } else {
                // placeholder to clear and fill the root with data for the next batch
            }

            // we do not want to close the root right after putNext() call as we do not know the status of it whether
            // its transmitted at transport;  we close them all at complete stream. TODO: optimize this behaviour
            serverStreamListener.putNext();
            completionListener.onResponse(null);
        } catch (Exception e) {
            completionListener.onFailure(new TransportException("Failed to send batch", e));
        }
    }

    /**
     * Completes the streaming response and closes all pending roots.
     *
     * @param completionListener callback for completion or failure
     */
    public void completeStream(ActionListener<Void> completionListener) {
        if (!open.get()) {
            throw new IllegalStateException("FlightServerChannel already closed.");
        }
        try {
            serverStreamListener.completed();
            completionListener.onResponse(null);
        } catch (Exception e) {
            completionListener.onFailure(new TransportException("Failed to complete stream", e));
        }
    }

    /**
     * Sends an error and closes the channel.
     *
     * @param error the error to send
     * @param completionListener callback for completion or failure
     */
    public void sendError(ByteBuffer header, Exception error, ActionListener<Void> completionListener) {
        if (!open.get()) {
            throw new IllegalStateException("FlightServerChannel already closed.");
        }
        try {
            middleware.setHeader(header);
            serverStreamListener.error(
                CallStatus.INTERNAL.withCause(error)
                    .withDescription(error.getMessage() != null ? error.getMessage() : "Stream error")
                    .toRuntimeException()
            );
            // TODO - move to debug log
            logger.error(error);
            completionListener.onFailure(error);
        } catch (Exception e) {
            completionListener.onFailure(new IOException("Failed to send error", e));
        } finally {
            if (root.get() != null) {
                root.get().close();
            }
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
        return new ChannelStats(); // TODO: Implement stats. Add custom stats as needed
    }

    @Override
    public void close() {
        if (!open.get()) {
            return;
        }
        if (root.get() != null) {
            root.get().close();
        }
        notifyCloseListeners();
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

    private void notifyCloseListeners() {
        for (ActionListener<Void> listener : closeListeners) {
            listener.onResponse(null);
        }
        closeListeners.clear();
    }
}

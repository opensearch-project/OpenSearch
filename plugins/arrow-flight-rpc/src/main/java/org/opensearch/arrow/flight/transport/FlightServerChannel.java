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
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.common.SetOnce;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TcpChannel implementation for Arrow Flight
 *
 */
class FlightServerChannel implements TcpChannel {
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
    private final FlightStatsCollector statsCollector;
    private volatile long requestStartTime;

    public FlightServerChannel(
        ServerStreamListener serverStreamListener,
        BufferAllocator allocator,
        ServerHeaderMiddleware middleware,
        FlightStatsCollector statsCollector
    ) {
        this.serverStreamListener = serverStreamListener;
        this.serverStreamListener.setUseZeroCopy(true);
        this.allocator = allocator;
        this.middleware = middleware;
        this.statsCollector = statsCollector;
        this.requestStartTime = System.nanoTime();
        this.localAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        this.remoteAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
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
        long batchStartTime = System.nanoTime();
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
            // its transmitted at transport; we close them all at complete stream. TODO: optimize this behaviour
            serverStreamListener.putNext();
            if (statsCollector != null) {
                statsCollector.incrementServerBatchesSent();
                // Track VectorSchemaRoot size - sum of all vector sizes
                long rootSize = calculateVectorSchemaRootSize(root.get());
                statsCollector.addBytesSent(rootSize);
                // Track batch processing time
                long batchTime = (System.nanoTime() - batchStartTime) / 1_000_000;
                statsCollector.addServerBatchTime(batchTime);
            }
            completionListener.onResponse(null);
        } catch (Exception e) {
            if (statsCollector != null) {
                statsCollector.incrementServerTransportErrors();
            }
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
            if (statsCollector != null) {
                statsCollector.incrementServerStreamsCompleted();
                statsCollector.decrementServerRequestsCurrent();
                // Track total request time from start to completion
                long requestTime = (System.nanoTime() - requestStartTime) / 1_000_000;
                statsCollector.addServerRequestTime(requestTime);
            }
            completionListener.onResponse(null);
        } catch (Exception e) {
            if (statsCollector != null) {
                statsCollector.incrementServerTransportErrors();
            }
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
            logger.debug(error);
            if (statsCollector != null) {
                statsCollector.incrementServerApplicationErrors();
                statsCollector.decrementServerRequestsCurrent();
                // Track request time even for failed requests
                long requestTime = (System.nanoTime() - requestStartTime) / 1_000_000;
                statsCollector.addServerRequestTime(requestTime);
            }
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
        listener.onFailure(new UnsupportedOperationException("FlightServerChannel does not support BytesReference based sendMessage()"));
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

    private long calculateVectorSchemaRootSize(VectorSchemaRoot root) {
        if (root == null) {
            return 0;
        }
        long totalSize = 0;
        // Sum up the buffer sizes of all vectors in the schema root
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            var vector = root.getVector(i);
            if (vector != null) {
                totalSize += vector.getBufferSize();
            }
        }
        return totalSize;
    }
}

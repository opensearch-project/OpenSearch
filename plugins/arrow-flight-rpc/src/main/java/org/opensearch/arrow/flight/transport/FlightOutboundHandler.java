/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtocolOutboundHandler;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.nativeprotocol.NativeOutboundMessage;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Outbound handler for Arrow Flight streaming responses.
 * It must invoke messageListener and relay any exception back to the caller and not supress them
 * @opensearch.internal
 */
class FlightOutboundHandler extends ProtocolOutboundHandler {
    private static final Logger logger = LogManager.getLogger(FlightOutboundHandler.class);
    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;
    private final String nodeName;
    private final Version version;
    private final String[] features;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;

    public FlightOutboundHandler(String nodeName, Version version, String[] features, StatsTracker statsTracker, ThreadPool threadPool) {
        this.nodeName = nodeName;
        this.version = version;
        this.features = features;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
    }

    @Override
    public void sendRequest(
        DiscoveryNode node,
        TcpChannel channel,
        long requestId,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        Version channelVersion,
        boolean compressRequest,
        boolean isHandshake
    ) throws IOException, TransportException {
        throw new UnsupportedOperationException("sendRequest not implemented for FlightOutboundHandler");
    }

    @Override
    public void sendResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final boolean compress,
        final boolean isHandshake
    ) throws IOException {
        throw new UnsupportedOperationException(
            "sendResponse() is not supported for streaming requests in FlightOutboundHandler; use sendResponseBatch()"
        );
    }

    @Override
    public void sendErrorResponse(
        Version nodeVersion,
        Set<String> features,
        TcpChannel channel,
        long requestId,
        String action,
        Exception error
    ) throws IOException {
        throw new UnsupportedOperationException(
            "sendResponse() is not supported for streaming requests in FlightOutboundHandler; use sendResponseBatch()"
        );
    }

    public void sendResponseBatch(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final FlightTransportChannel transportChannel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final boolean compress,
        final boolean isHandshake
    ) throws IOException {
        BatchTask task = new BatchTask(
            nodeVersion,
            features,
            channel,
            transportChannel,
            requestId,
            action,
            response,
            compress,
            isHandshake,
            false,
            false,
            null
        );

        if (!(channel instanceof FlightServerChannel flightChannel)) {
            messageListener.onResponseSent(requestId, action, new IllegalStateException("Expected FlightServerChannel"));
            return;
        }

        // Block the producer thread before queuing the batch so a slow consumer throttles
        // allocation rather than letting the eventloop's queue grow. Note: isReady()
        // reflects only gRPC's outbound buffer, not our own queue depth — see
        // docs/backpressure.md "Known limitation: unbounded eventloop queue".
        flightChannel.awaitReadyOrThrow();

        flightChannel.getExecutor().execute(threadPool.getThreadContext().preserveContext(() -> {
            try (BatchTask ignored = task) {
                processBatchTask(task);
            } catch (Exception e) {
                messageListener.onResponseSent(requestId, action, e);
            }
        }));
    }

    private void processBatchTask(BatchTask task) {
        if (!(task.channel() instanceof FlightServerChannel flightChannel)) {
            Exception error = new IllegalStateException("Expected FlightServerChannel, got " + task.channel().getClass().getName());
            messageListener.onResponseSent(task.requestId(), task.action(), error);
            return;
        }

        try {
            ByteBuffer header = getHeaderBuffer(task.requestId(), task.nodeVersion(), task.features());
            if (task.response() instanceof ArrowBatchResponse arrowResponse) {
                // Native Arrow path: the channel owns the stream root end to end (create/transfer/adopt/free).
                flightChannel.sendArrowBatch(header, arrowResponse.getRoot(), arrowResponse.getMetadata());
                messageListener.onResponseSent(task.requestId(), task.action(), task.response());
            } else {
                // Byte-serialization path: notify before out.close() (the close releases the serialization
                // vector), mirroring the original ordering.
                try (VectorStreamOutput out = VectorStreamOutput.create(flightChannel.getAllocator(), flightChannel.getRoot())) {
                    task.response().writeTo(out);
                    flightChannel.sendBatch(header, out, null);
                    messageListener.onResponseSent(task.requestId(), task.action(), task.response());
                }
            }
        } catch (FlightRuntimeException e) {
            // Fail the stream before notifying the listener so a throwing listener can't leave it un-terminated.
            failStream(flightChannel, task, e);
            messageListener.onResponseSent(task.requestId(), task.action(), FlightErrorMapper.fromFlightException(e));
        } catch (Exception e) {
            failStream(flightChannel, task, e);
            messageListener.onResponseSent(task.requestId(), task.action(), e);
        }
    }

    /**
     * Fails the stream after a batch send error: sends the error so the consumer's
     * {@code FlightStream.getRoot()} surfaces an exception instead of hanging on a never-arriving first
     * frame, then releases the channel so a later {@code completeStream} can't double-terminate the gRPC
     * listener. The stream root is owned by the channel (see {@link FlightServerChannel#sendArrowBatch}),
     * so there is nothing to close here. Best-effort and idempotent; runs inline on the channel executor.
     */
    private void failStream(FlightServerChannel flightChannel, BatchTask task, Exception cause) {
        try {
            Exception flightError = cause instanceof StreamException se ? FlightErrorMapper.toFlightException(se) : cause;
            flightChannel.sendError(getHeaderBuffer(task.requestId(), task.nodeVersion(), task.features()), flightError);
        } catch (Exception suppressed) {
            // Channel already closed/cancelled, or header serialization failed — nothing left to fail.
            logger.debug(new ParameterizedMessage("failStream: could not send error for requestId [{}]", task.requestId()), suppressed);
        } finally {
            // Make the channel terminal so a later completeStream can't double-terminate the listener. Idempotent.
            if (task.transportChannel() != null) {
                task.transportChannel().releaseChannel(true);
            }
        }
    }

    public void completeStream(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final FlightTransportChannel transportChannel,
        final long requestId,
        final String action
    ) {
        BatchTask completeTask = new BatchTask(
            nodeVersion,
            features,
            channel,
            transportChannel,
            requestId,
            action,
            TransportResponse.Empty.INSTANCE,
            false,
            false,
            true,
            false,
            null
        );

        if (!(channel instanceof FlightServerChannel flightChannel)) {
            messageListener.onResponseSent(requestId, action, new IllegalStateException("Expected FlightServerChannel"));
            return;
        }

        flightChannel.getExecutor().execute(threadPool.getThreadContext().preserveContext(() -> {
            try (BatchTask ignored = completeTask) {
                processCompleteTask(completeTask);
            } catch (Exception e) {
                messageListener.onResponseSent(requestId, action, e);
            }
        }));
    }

    private void processCompleteTask(BatchTask task) {
        if (!(task.channel() instanceof FlightServerChannel flightChannel)) {
            Exception error = new IllegalStateException("Expected FlightServerChannel, got " + task.channel().getClass().getName());
            messageListener.onResponseSent(task.requestId(), task.action(), error);
            return;
        }

        try {
            flightChannel.completeStream(getHeaderBuffer(task.requestId(), task.nodeVersion(), task.features()));
            messageListener.onResponseSent(task.requestId(), task.action(), TransportResponse.Empty.INSTANCE);
        } catch (Exception e) {
            messageListener.onResponseSent(task.requestId(), task.action(), e);
        }
    }

    public void sendErrorResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final FlightTransportChannel transportChannel,
        final long requestId,
        final String action,
        final Exception error
    ) {
        BatchTask errorTask = new BatchTask(
            nodeVersion,
            features,
            channel,
            transportChannel,
            requestId,
            action,
            null,
            false,
            false,
            false,
            true,
            error
        );

        if (!(channel instanceof FlightServerChannel flightChannel)) {
            messageListener.onResponseSent(requestId, action, new IllegalStateException("Expected FlightServerChannel"));
            return;
        }

        flightChannel.getExecutor().execute(threadPool.getThreadContext().preserveContext(() -> {
            try (BatchTask ignored = errorTask) {
                processErrorTask(errorTask);
            } catch (Exception e) {
                messageListener.onResponseSent(requestId, action, e);
            }
        }));
    }

    private void processErrorTask(BatchTask task) {
        if (!(task.channel() instanceof FlightServerChannel flightServerChannel)) {
            Exception error = new IllegalStateException("Expected FlightServerChannel, got " + task.channel().getClass().getName());
            messageListener.onResponseSent(task.requestId(), task.action(), error);
            return;
        }

        try {
            Exception flightError = task.error();
            if (task.error() instanceof StreamException se) {
                flightError = FlightErrorMapper.toFlightException(se);
            }
            flightServerChannel.sendError(getHeaderBuffer(task.requestId(), task.nodeVersion(), task.features()), flightError);
            messageListener.onResponseSent(task.requestId(), task.action(), task.error());
        } catch (Exception e) {
            messageListener.onResponseSent(task.requestId(), task.action(), e);
        }
    }

    @Override
    public void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    private ByteBuffer getHeaderBuffer(long requestId, Version nodeVersion, Set<String> features) throws IOException {
        // Just a way( probably inefficient) to serialize header to reuse existing logic present in
        // NativeOutboundMessage.Response#writeVariableHeader()
        NativeOutboundMessage.Response headerMessage = new NativeOutboundMessage.Response(
            threadPool.getThreadContext(),
            features,
            out -> {},
            Version.min(version, nodeVersion),
            requestId,
            false,
            false
        );
        try (BytesStreamOutput bytesStream = new BytesStreamOutput()) {
            BytesReference headerBytes = headerMessage.serialize(bytesStream);
            return ByteBuffer.wrap(headerBytes.toBytesRef().bytes);
        }
    }

    record BatchTask(Version nodeVersion, Set<String> features, TcpChannel channel, FlightTransportChannel transportChannel, long requestId,
        String action, TransportResponse response, boolean compress, boolean isHandshake, boolean isComplete, boolean isError,
        Exception error) implements AutoCloseable {

        @Override
        public void close() {
            if ((isComplete || isError) && transportChannel != null) {
                transportChannel.releaseChannel(isError);
            }
        }
    }
}

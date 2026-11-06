/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.stream;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.transport.grpc.proto.response.search.SearchResponseProtoUtils;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;
import org.opensearch.transport.grpc.spi.StreamPayload;
import org.opensearch.transport.grpc.spi.StreamSearchResponseChannel;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.grpc.stub.ServerCallStreamObserver;

/**
 * {@link StreamSearchResponseChannel} over a server-streaming gRPC call whose responses are raw message streams.
 * <p>
 * A lock serializes sends because {@link ServerCallStreamObserver} is not thread-safe. A {@link ReentrantLock}, unlike
 * {@code synchronized}, keeps the virtual threads that wait for it unmounted on every supported JDK.
 */
public final class GrpcStreamSearchResponseChannel implements StreamSearchResponseChannel {

    private static final Logger logger = LogManager.getLogger(GrpcStreamSearchResponseChannel.class);

    private static final MethodDescriptor.Marshaller<org.opensearch.protobufs.SearchResponse> SUMMARY_MARSHALLER = ProtoLiteUtils
        .marshaller(org.opensearch.protobufs.SearchResponse.getDefaultInstance());
    private static final int PAYLOAD_TAG_SIZE = CodedOutputStream.computeTagSize(PAYLOAD_FIELD_NUMBER);
    private static final Runnable FIRED = () -> {};

    private final ServerCallStreamObserver<InputStream> call;
    @Nullable
    private final Runnable resetStream;
    private final AggregateProtoConverterRegistry aggregateRegistry;
    private final Releasable onClose;
    private final ReentrantLock sendLock = new ReentrantLock();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicBoolean transportClosed = new AtomicBoolean();
    private final AtomicBoolean resourcesReleased = new AtomicBoolean();
    private final AtomicReference<Runnable> cancelCallback = new AtomicReference<>();
    private volatile Runnable readyCallback;
    private boolean closed;

    /**
     * Creates a channel over {@code call}. Create it while the gRPC service method runs, because gRPC accepts call handlers
     * only until the method returns.
     *
     * @param call              the call
     * @param resetStream       resets the call's stream, see {@link StreamReset}; without it, {@link #reset} only ends the
     *                          call
     * @param aggregateRegistry converts aggregations of the summary
     * @param onClose           released once when the call completes, fails, is reset or is cancelled
     */
    public GrpcStreamSearchResponseChannel(
        ServerCallStreamObserver<InputStream> call,
        @Nullable Runnable resetStream,
        AggregateProtoConverterRegistry aggregateRegistry,
        Releasable onClose
    ) {
        this.call = call;
        this.resetStream = resetStream;
        this.aggregateRegistry = aggregateRegistry;
        this.onClose = onClose;
        call.setOnReadyHandler(() -> {
            final Runnable callback = readyCallback;
            if (callback != null) {
                callback.run();
            }
        });
        call.setOnCancelHandler(this::onCancelled);
        call.setOnCloseHandler(() -> transportClosed.set(true));
    }

    @Override
    public boolean isReady() {
        return cancelled.get() == false && call.isReady();
    }

    @Override
    public void setOnReadyHandler(Runnable onReady) {
        this.readyCallback = onReady;
    }

    @Override
    public void setOnCancelHandler(Runnable onCancel) {
        final Runnable previous = cancelCallback.getAndUpdate(current -> current == FIRED ? FIRED : onCancel);
        if (previous == FIRED) {
            onCancel.run();
        }
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    @Override
    public void sendPayload(StreamPayload payload) {
        sendLock.lock();
        try {
            if (closed || cancelled.get()) {
                payload.release();
                return;
            }
            // gRPC drops the messages of a cancelled call without consuming them. Closing the message releases such a payload.
            try (PayloadInputStream message = new PayloadInputStream(payloadPrefix(payload), payload)) {
                call.onNext(message);
            }
        } catch (RuntimeException e) {
            payload.release();
            throw e;
        } finally {
            sendLock.unlock();
        }
    }

    @Override
    public void complete(SearchResponse summary) {
        sendLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (cancelled.get()) {
                return;
            }
            final org.opensearch.protobufs.SearchResponse proto;
            try {
                proto = withoutHits(SearchResponseProtoUtils.toProto(summary, aggregateRegistry));
            } catch (IOException | RuntimeException e) {
                logger.debug("failed to convert streaming search summary", e);
                call.onError(GrpcErrorHandler.convertToGrpcError(e));
                return;
            }
            call.onNext(SUMMARY_MARSHALLER.stream(proto));
            call.onCompleted();
        } finally {
            sendLock.unlock();
            releaseResources();
        }
    }

    @Override
    public void fail(Exception failure) {
        sendLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (cancelled.get() == false) {
                call.onError(GrpcErrorHandler.convertToGrpcError(failure));
            }
        } finally {
            sendLock.unlock();
            releaseResources();
        }
    }

    /**
     * Ends the call with status {@code CANCELLED} if it has not ended yet, and resets its stream. Closing a call only queues
     * its trailers behind the messages the client has not read, so a client that stopped reading would hold them, and their
     * payloads, for as long as its connection lives. Resetting the stream makes Netty release them. Without a stream reset,
     * this only ends the call.
     */
    @Override
    public void reset() {
        sendLock.lock();
        try {
            if (closed == false) {
                closed = true;
                if (cancelled.get() == false) {
                    call.onError(Status.CANCELLED.withDescription("the stream was reset").asRuntimeException());
                }
            }
        } finally {
            sendLock.unlock();
            releaseResources();
        }
        if (resetStream != null && transportClosed.get() == false && cancelled.get() == false) {
            resetStream.run();
        }
    }

    private void onCancelled() {
        if (cancelled.compareAndSet(false, true)) {
            releaseResources();
            final Runnable callback = cancelCallback.getAndSet(FIRED);
            if (callback != null) {
                callback.run();
            }
        }
    }

    private void releaseResources() {
        if (resourcesReleased.compareAndSet(false, true)) {
            onClose.close();
        }
    }

    /** Returns {@code proto} without its hits. The total hits stay, but hits are not part of a streamed response. */
    static org.opensearch.protobufs.SearchResponse withoutHits(org.opensearch.protobufs.SearchResponse proto) {
        if (proto.hasHits() == false || proto.getHits().getHitsCount() == 0) {
            return proto;
        }
        return proto.toBuilder().setHits(proto.getHits().toBuilder().clearHits()).build();
    }

    /** Encodes the protobuf tag and length that precede {@code payload} in its message. */
    static ByteBuffer payloadPrefix(StreamPayload payload) {
        if (payload.length() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("payload of [" + payload.length() + "] bytes exceeds the maximum gRPC message size");
        }
        final int length = (int) payload.length();
        final byte[] prefix = new byte[PAYLOAD_TAG_SIZE + CodedOutputStream.computeUInt32SizeNoTag(length)];
        final CodedOutputStream out = CodedOutputStream.newInstance(prefix);
        try {
            out.writeTag(PAYLOAD_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
            out.writeUInt32NoTag(length);
            out.checkNoSpaceLeft();
        } catch (IOException e) {
            throw new IllegalStateException("prefix buffer is sized exactly", e);
        }
        return ByteBuffer.wrap(prefix);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.flight;

import io.grpc.ClientCall;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A demand-driven replacement for {@link FlightStream} that uses
 * {@code disableAutoInboundFlowControl()} to prevent gRPC from eagerly
 * deserializing messages into the Arrow allocator.
 *
 * <p>Standard FlightStream allows gRPC to deliver and deserialize messages as fast
 * as bytes arrive on the wire. Each deserialization allocates Arrow buffers from the
 * bounded flight allocator pool. Under high-cardinality workloads, this exhausts the pool.
 *
 * <p>This implementation requests exactly 1 message at a time via {@code request(1)}.
 * The gRPC Netty layer only reads and deserializes ONE message from the socket before
 * waiting for the next explicit request. This gives true demand-driven streaming:
 * at most 1 deserialized message in the allocator at any time.
 *
 * <p>This achieves InfluxDB-style lazy streaming within Java gRPC:
 * <ul>
 *   <li>Consumer calls {@link #next()} → triggers {@code request(1)}</li>
 *   <li>gRPC reads and deserializes exactly 1 message</li>
 *   <li>Consumer processes and frees the batch</li>
 *   <li>Next {@code next()} call triggers the next {@code request(1)}</li>
 * </ul>
 */
public class DemandDrivenFlightStream implements AutoCloseable {

    private final BufferAllocator allocator;
    private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>(2);
    private volatile ClientCallStreamObserver<?> requestStream;
    private volatile VectorSchemaRoot root;
    private volatile VectorLoader loader;
    private volatile Schema schema;
    private volatile boolean completed = false;
    private volatile Throwable error;
    private volatile ArrowBuf applicationMetadata;

    private static final Object DONE = new Object();

    DemandDrivenFlightStream(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    /**
     * Gets the DoGet method descriptor from Arrow Flight's binding service.
     * Same package access — no reflection needed.
     */
    private static io.grpc.MethodDescriptor<org.apache.arrow.flight.impl.Flight.Ticket, ArrowMessage> doGetDescriptor(BufferAllocator allocator) {
        return FlightBindingService.getDoGetDescriptor(allocator);
    }

    /**
     * Opens a demand-driven stream using the gRPC channel directly.
     * No reflection — constructs the MethodDescriptor and ClientCall explicitly.
     *
     * @param channel      the gRPC ManagedChannel (from FlightTransport)
     * @param allocator    the Arrow BufferAllocator for deserialization
     * @param ticket       the Flight ticket identifying the stream
     * @param headers      gRPC metadata headers (e.g. correlation ID)
     * @param interceptors additional client interceptors (e.g. response header capture)
     */
    public static DemandDrivenFlightStream openFromChannel(
        io.grpc.Channel channel,
        BufferAllocator allocator,
        Ticket ticket,
        io.grpc.Metadata headers,
        io.grpc.ClientInterceptor... interceptors
    ) {
        var descriptor = doGetDescriptor(allocator);
        java.util.List<io.grpc.ClientInterceptor> allInterceptors = new java.util.ArrayList<>();
        allInterceptors.add(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
        allInterceptors.addAll(java.util.Arrays.asList(interceptors));
        io.grpc.Channel interceptedChannel = io.grpc.ClientInterceptors.intercept(channel, allInterceptors);
        ClientCall<org.apache.arrow.flight.impl.Flight.Ticket, ArrowMessage> call =
            interceptedChannel.newCall(descriptor, io.grpc.CallOptions.DEFAULT);
        return open(allocator, call, ticket.toProtocol());
    }

    /**
     * Creates a demand-driven stream for the given gRPC call.
     * Disables auto inbound flow control and requests messages one at a time.
     */
    static DemandDrivenFlightStream open(
        BufferAllocator allocator,
        ClientCall<org.apache.arrow.flight.impl.Flight.Ticket, ArrowMessage> call,
        org.apache.arrow.flight.impl.Flight.Ticket ticket
    ) {
        DemandDrivenFlightStream stream = new DemandDrivenFlightStream(allocator);

        ClientResponseObserver<org.apache.arrow.flight.impl.Flight.Ticket, ArrowMessage> observer =
            new ClientResponseObserver<>() {
                @Override
                public void beforeStart(ClientCallStreamObserver<org.apache.arrow.flight.impl.Flight.Ticket> reqStream) {
                    stream.requestStream = reqStream;
                    reqStream.disableAutoInboundFlowControl();
                }

                @Override
                public void onNext(ArrowMessage message) {
                    try {
                        stream.queue.put(message);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    stream.error = t;
                    stream.completed = true;
                    try {
                        stream.queue.put(DONE);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                @Override
                public void onCompleted() {
                    stream.completed = true;
                    try {
                        stream.queue.put(DONE);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            };

        // Start the streaming call with our demand-driven observer
        ClientCalls.asyncServerStreamingCall(call, ticket, observer);

        // Request the first message (schema)
        stream.requestStream.request(1);

        return stream;
    }

    /**
     * Advances to the next record batch. Returns false when stream is exhausted.
     * Each call requests exactly 1 message from gRPC → exactly 1 deserialization.
     */
    public boolean next() {
        if (completed && queue.isEmpty()) return false;

        try {
            Object item = queue.poll(60, TimeUnit.SECONDS);
            if (item == null) {
                throw new RuntimeException("Timeout waiting for Flight message");
            }
            if (item == DONE) {
                if (error != null) {
                    if (error instanceof RuntimeException re) throw re;
                    throw new RuntimeException(error);
                }
                return false;
            }

            ArrowMessage message = (ArrowMessage) item;
            processMessage(message);

            // Request the next message AFTER processing the current one.
            // This is the demand-driven signal: gRPC only reads/deserializes
            // the next message when we explicitly ask for it.
            if (!completed) {
                requestStream.request(1);
            }

            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void processMessage(ArrowMessage message) {
        try {
            switch (message.getMessageType()) {
                case SCHEMA:
                    schema = message.asSchemaMessage() != null
                        ? org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeSchema(message.asSchemaMessage())
                        : null;
                    if (schema != null && root == null) {
                        root = VectorSchemaRoot.create(schema, allocator);
                        loader = new VectorLoader(root);
                    }
                    if (!completed) requestStream.request(1);
                    Object next = queue.poll(60, TimeUnit.SECONDS);
                    if (next != null && next != DONE) {
                        processMessage((ArrowMessage) next);
                    }
                    break;

                case RECORD_BATCH:
                    if (root != null) root.clear();
                    ArrowRecordBatch batch = message.asRecordBatch();
                    try {
                        loader.load(batch);
                    } finally {
                        batch.close();
                    }
                    updateMetadata(message);
                    break;

                case DICTIONARY_BATCH:
                    if (!completed) requestStream.request(1);
                    Object dictNext = queue.poll(60, TimeUnit.SECONDS);
                    if (dictNext != null && dictNext != DONE) {
                        processMessage((ArrowMessage) dictNext);
                    }
                    break;

                default:
                    break;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException("Error processing Arrow Flight message", e);
        } finally {
            try {
                message.close();
            } catch (Exception e) {
                // best-effort close
            }
        }
    }

    private void updateMetadata(ArrowMessage message) {
        if (applicationMetadata != null) {
            applicationMetadata.close();
        }
        applicationMetadata = message.getApplicationMetadata();
        if (applicationMetadata != null) {
            applicationMetadata.getReferenceManager().retain();
        }
    }

    /** Returns the current VectorSchemaRoot (reused across batches). */
    public VectorSchemaRoot getRoot() {
        return root;
    }

    /** Returns the schema. */
    public Schema getSchema() {
        return schema;
    }

    /** Returns application metadata from the last batch. */
    public ArrowBuf getLatestMetadata() {
        return applicationMetadata;
    }

    /** Cancel the stream. */
    public void cancel(String reason, Throwable cause) {
        if (requestStream != null) {
            requestStream.cancel(reason, cause);
        }
    }

    @Override
    public void close() throws Exception {
        if (root != null) {
            root.close();
        }
        if (applicationMetadata != null) {
            applicationMetadata.close();
        }
        // Drain remaining messages
        Object remaining;
        while ((remaining = queue.poll()) != null) {
            if (remaining instanceof ArrowMessage msg) {
                msg.close();
            }
        }
    }
}

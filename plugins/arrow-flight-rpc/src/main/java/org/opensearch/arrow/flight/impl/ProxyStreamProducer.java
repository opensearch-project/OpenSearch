/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.ExceptionsHelper;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.common.unit.TimeValue;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ProxyStreamProvider acts as forward proxy for FlightStream.
 * It creates a BatchedJob to handle the streaming of data from the remote FlightStream.
 * This is useful when stream is not present locally and needs to be fetched from a node
 * retrieved using {@link StreamTicket#getNodeId()} where it is present.
 */
public class ProxyStreamProducer implements StreamProducer {

    private final StreamReader remoteStream;

    /**
     * Constructs a new ProxyStreamProducer instance.
     *
     * @param remoteStream The remote FlightStream to be proxied.
     */
    public ProxyStreamProducer(StreamReader remoteStream) {
        this.remoteStream = remoteStream;
    }

    /**
     * Creates a VectorSchemaRoot for the remote FlightStream.
     * @param allocator The allocator to use for creating vectors
     * @return A VectorSchemaRoot representing the schema of the remote FlightStream
     */
    @Override
    public VectorSchemaRoot createRoot(BufferAllocator allocator) throws Exception {
        return remoteStream.getRoot();
    }

    /**
     * Creates a BatchedJob
     * @param allocator The allocator to use for any additional memory allocations
     */
    @Override
    public BatchedJob createJob(BufferAllocator allocator) {
        return new ProxyBatchedJob(remoteStream);
    }

    /**
     * Returns the deadline for the remote FlightStream.
     * Since the stream is not present locally, the deadline is set to -1. It piggybacks on remote stream expiration
     * @return The deadline for the remote FlightStream
     */
    @Override
    public TimeValue getJobDeadline() {
        return TimeValue.MINUS_ONE;
    }

    /**
     * Provides an estimate of the total number of rows that will be produced.
     */
    @Override
    public int estimatedRowCount() {
        // TODO get it from remote flight stream
        return -1;
    }

    /**
     * Task action name
     */
    @Override
    public String getAction() {
        // TODO get it from remote flight stream
        return "";
    }

    /**
     * Closes the remote FlightStream.
     */
    @Override
    public void close() {
        ExceptionsHelper.catchAsRuntimeException(remoteStream::close);
    }

    static class ProxyBatchedJob implements BatchedJob {

        private final StreamReader remoteStream;
        private final AtomicBoolean isCancelled = new AtomicBoolean(false);

        ProxyBatchedJob(StreamReader remoteStream) {
            this.remoteStream = remoteStream;
        }

        @Override
        public void run(VectorSchemaRoot root, FlushSignal flushSignal) throws Exception {
            while (!isCancelled.get() && remoteStream.next()) {
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(1000));
            }
        }

        @Override
        public void onCancel() {
            isCancelled.set(true);
        }

        @Override
        public boolean isCancelled() {
            // Proxy stream don't have any business logic to set this flag,
            // they piggyback on remote stream getting cancelled.
            return isCancelled.get();
        }
    }
}

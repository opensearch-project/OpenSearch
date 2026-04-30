/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Base class for transport responses carrying native Arrow data. Subclasses must provide
 * two constructors — one for sending (wraps a populated root) and one for receiving
 * (takes ownership of vectors from the Flight stream via {@link StreamInput}):
 *
 * <pre>{@code
 * public class MyResponse extends ArrowBatchResponse {
 *     public MyResponse(VectorSchemaRoot root) { super(root); }       // send side
 *     public MyResponse(StreamInput in) throws IOException { super(in); } // receive side
 * }
 * }</pre>
 *
 * <p><b>Send side:</b> The producer populates a {@link VectorSchemaRoot} and wraps it.
 * The framework zero-copy transfers the vectors into the Flight stream via
 * {@link #transferTo(VectorSchemaRoot)} — no memcpy, no serialization.
 *
 * <pre>{@code
 * BufferAllocator allocator = ArrowFlightChannel.from(channel).getAllocator();
 * VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
 * // populate producerRoot...
 * channel.sendResponseBatch(new MyResponse(producerRoot));
 * // producerRoot is now owned by the framework — don't reuse or close it
 * }</pre>
 *
 * <p><b>Receive side:</b> The framework calls {@code handler.read(in)} where {@code in} is
 * a {@link VectorStreamInput.NativeArrow} holding vectors transferred from the Flight stream.
 * The {@link #ArrowBatchResponse(StreamInput)} constructor claims ownership of those vectors.
 *
 * <p><b>Allocator guidelines:</b> The allocator for producer roots must outlive the gRPC
 * stream. gRPC's zero-copy write path retains buffer references beyond stream completion;
 * closing the allocator early causes memory accounting errors. Use the channel allocator
 * ({@code ArrowFlightChannel.from(channel).getAllocator()}) or a long-lived application
 * allocator.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class ArrowBatchResponse extends ActionResponse {

    private final VectorSchemaRoot producerRoot;

    /**
     * Send-side constructor: wraps a root populated by the producer.
     * @param producerRoot the root to send; ownership transfers to the framework
     */
    protected ArrowBatchResponse(VectorSchemaRoot producerRoot) {
        this.producerRoot = producerRoot;
    }

    /**
     * Receive-side constructor: claims ownership of the Arrow vectors from the input.
     * @param in must be a {@link VectorStreamInput.NativeArrow}; throws otherwise
     * @throws IOException if reading fails
     */
    protected ArrowBatchResponse(StreamInput in) throws IOException {
        super(in);
        if (in instanceof VectorStreamInput.NativeArrow nativeIn) {
            this.producerRoot = nativeIn.getRoot();
            nativeIn.claimOwnership();
        } else {
            throw new IllegalStateException(
                "ArrowBatchResponse decoded from a non-native-Arrow StreamInput ("
                    + (in == null ? "null" : in.getClass().getName())
                    + "). Wrapping handlers around ArrowBatchResponseHandler must forward "
                    + "TransportResponseHandler#skipsDeserialization()."
            );
        }
    }

    /** Returns the Arrow root holding the response vectors. */
    public VectorSchemaRoot getRoot() {
        return producerRoot;
    }

    /**
     * Zero-copy transfers the producer's vectors into the target root.
     * @param target the channel's stream root (bound to the Flight stream via start())
     */
    void transferTo(VectorSchemaRoot target) {
        FlightUtils.transferRoot(producerRoot, target);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        // no-op: the framework handles transfer via transferTo()
    }
}

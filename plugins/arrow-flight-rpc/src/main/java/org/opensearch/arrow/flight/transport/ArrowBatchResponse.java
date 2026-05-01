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
 * Base class for transport responses carrying native Arrow data.
 *
 * <p>The producer creates vectors using the channel's allocator and populates them freely
 * on any thread. When the executor processes this batch, it does a zero-copy transfer
 * of the producer's buffers into the channel's shared root — no memcpy, no serialization.
 * After transfer, the framework closes the producer's root, releasing its buffers back
 * to the allocator.
 *
 * <p><b>Allocator guidelines:</b> The allocator used for producer roots must outlive the
 * gRPC stream — do not create and close a child allocator per request. gRPC's zero-copy
 * write path retains buffer references beyond stream completion, and closing the allocator
 * while gRPC still holds these references causes memory accounting errors. Use either the
 * channel allocator (via {@code ArrowFlightChannel.from(channel).getAllocator()}) or a
 * long-lived application allocator. The framework creates the shared root from the
 * producer's allocator to ensure same-allocator transfer, which avoids an Arrow bug with
 * cross-allocator transfer of foreign-backed buffers from C data import.
 *
 * <p>Usage (send side):
 * <pre>{@code
 * BufferAllocator allocator = ArrowFlightChannel.from(channel).getAllocator();
 * VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
 * // populate producerRoot on any thread...
 * channel.sendResponseBatch(new MyResponse(producerRoot));
 * // producerRoot is now owned by the framework — don't reuse or close it
 * }</pre>
 *
 * <p>Usage (receive side):
 * <pre>{@code
 * public class MyResponse extends ArrowBatchResponse {
 *     public MyResponse(VectorSchemaRoot root) { super(root); }
 *     public MyResponse(StreamInput in) throws IOException { super(in); }
 * }
 * }</pre>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class ArrowBatchResponse extends ActionResponse {

    private final VectorSchemaRoot producerRoot;

    /**
     * Creates a response with the given producer root (send side).
     * @param producerRoot the root populated by the producer
     */
    protected ArrowBatchResponse(VectorSchemaRoot producerRoot) {
        this.producerRoot = producerRoot;
    }

    /**
     * Deserializes a response from a StreamInput (receive side).
     * @param in the stream input containing the Arrow root
     * @throws IOException if deserialization fails
     */
    protected ArrowBatchResponse(StreamInput in) throws IOException {
        super(in);
        this.producerRoot = ((VectorStreamInput) in).getRoot();
    }

    /**
     * Returns the producer's root. On the send side, this is the root populated
     * by the producer. On the receive side, this is the root from the Flight stream.
     */
    public VectorSchemaRoot getRoot() {
        return producerRoot;
    }

    /**
     * Zero-copy transfers the producer's vectors into the target root.
     * Called by the framework on the executor thread before {@code putNext()}.
     * After transfer, the producer's buffers are moved to the target — the producer
     * root becomes empty.
     *
     * @param target the channel's shared root (bound to the Flight stream via start())
     */
    void transferTo(VectorSchemaRoot target) {
        FlightUtils.transferRoot(producerRoot, target);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        // no-op: the framework handles transfer via transferTo()
    }
}

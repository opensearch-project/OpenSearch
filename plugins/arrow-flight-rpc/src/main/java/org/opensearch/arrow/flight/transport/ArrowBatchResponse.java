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
 * The framework zero-copy transfers the vectors into the Flight stream — no memcpy,
 * no serialization.
 *
 * <pre>{@code
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
 * <p><b>Allocator rules:</b>
 * <ul>
 *   <li><b>Send side:</b> Use a child of {@link ArrowAllocatorProvider}. All allocators
 *       must share the same root so zero-copy transfers pass Arrow's
 *       {@code AllocationManager} associate check. The framework creates the Flight
 *       stream root from the producer's allocator to ensure same-allocator transfer —
 *       this avoids an Arrow bug with cross-allocator transfer of foreign-backed
 *       buffers from C data import.</li>
 *   <li><b>Send side:</b> Allocators must outlive the gRPC stream — gRPC's zero-copy write
 *       path retains buffer references beyond stream completion. Do not create and close a
 *       child allocator per request.</li>
 *   <li><b>Receive side:</b> The framework transfers vectors from the Flight stream's
 *       allocator into the response. The consumer can then transfer them into its own
 *       allocator — which must also be a child of {@link ArrowAllocatorProvider}.</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class ArrowBatchResponse extends ActionResponse {

    private final VectorSchemaRoot batchRoot;

    /**
     * Send-side constructor: wraps a root populated by the producer.
     * @param batchRoot the root to send; ownership transfers to the transport
     */
    protected ArrowBatchResponse(VectorSchemaRoot batchRoot) {
        this.batchRoot = batchRoot;
    }

    /**
     * Receive-side constructor: claims ownership of the consumer root from the input.
     * @param in must be a {@link VectorStreamInput.NativeArrow}; throws otherwise
     * @throws IOException if reading fails
     */
    protected ArrowBatchResponse(StreamInput in) throws IOException {
        super(in);
        if (in instanceof VectorStreamInput.NativeArrow nativeIn) {
            this.batchRoot = nativeIn.getRoot();
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
        return batchRoot;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        // no-op: the framework transfers vectors directly via FlightUtils.transferRoot()
    }
}

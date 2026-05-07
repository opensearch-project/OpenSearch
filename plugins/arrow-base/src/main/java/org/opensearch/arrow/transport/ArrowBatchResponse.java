/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.transport;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Base class for transport responses carrying native Arrow data. Subclasses must provide
 * two constructors — one for sending (wraps a populated root) and one for receiving
 * (takes ownership of vectors from the transport via {@link StreamInput}):
 *
 * <pre>{@code
 * public class MyResponse extends ArrowBatchResponse {
 *     public MyResponse(VectorSchemaRoot root) { super(root); }       // send side
 *     public MyResponse(StreamInput in) throws IOException { super(in); } // receive side
 * }
 * }</pre>
 *
 * <p><b>Send side:</b> The producer populates a {@link VectorSchemaRoot} and wraps it.
 * An Arrow-aware transport zero-copy transfers the vectors onto the wire — no memcpy,
 * no serialization.
 *
 * <pre>{@code
 * VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
 * // populate producerRoot...
 * channel.sendResponseBatch(new MyResponse(producerRoot));
 * // producerRoot is now owned by the transport — don't reuse or close it
 * }</pre>
 *
 * <p><b>Receive side:</b> The transport calls {@code handler.read(in)} with a
 * {@link StreamInput} that also implements {@link ArrowStreamInput}, carrying vectors
 * transferred from the wire. The {@link #ArrowBatchResponse(StreamInput)} constructor claims
 * ownership of those vectors.
 *
 * <p><b>Allocator rules:</b>
 * <ul>
 *   <li><b>Send side:</b> Use a child of {@link org.opensearch.arrow.memory.ArrowAllocatorService}.
 *       All allocators must share the same root so zero-copy transfers pass Arrow's
 *       {@code AllocationManager} associate check.</li>
 *   <li><b>Send side:</b> Allocators must outlive the transport stream — some transports
 *       (e.g., gRPC zero-copy) retain buffer references beyond stream completion. Do not
 *       create and close a child allocator per request.</li>
 *   <li><b>Receive side:</b> The transport transfers vectors from its own allocator into
 *       the response. The consumer can then transfer them into its own allocator — which
 *       must also be a child of {@link org.opensearch.arrow.memory.ArrowAllocatorService}.</li>
 * </ul>
 *
 * <p><b>Cross-plugin footgun:</b> bypassing {@link org.opensearch.arrow.memory.ArrowAllocatorService}
 * (e.g. {@code new RootAllocator()} inside a plugin) does not fail fast — allocation and
 * single-plugin use still work. But any zero-copy handoff to another plugin's buffers will trip
 * Arrow's {@code AllocationManager.associate()} check, because roots are compared by identity,
 * not by address space.
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
     * Receive-side constructor: claims ownership of the batch from the input.
     * @param in must also implement {@link ArrowStreamInput}; throws otherwise
     * @throws IOException if reading fails
     */
    protected ArrowBatchResponse(StreamInput in) throws IOException {
        super(in);
        if (in instanceof ArrowStreamInput arrowIn) {
            this.batchRoot = arrowIn.getRoot();
            arrowIn.claimOwnership();
        } else {
            throw new IllegalStateException(
                "ArrowBatchResponse decoded from a non-Arrow StreamInput ("
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
        // no-op: the transport transfers vectors directly, bypassing byte serialization.
    }
}

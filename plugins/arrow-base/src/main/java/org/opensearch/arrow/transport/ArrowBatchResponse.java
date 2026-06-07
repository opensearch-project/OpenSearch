/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.transport;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
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
 *   <li><b>Send side:</b> Source the allocator from one of the framework's named pools via
 *       {@link ArrowNativeAllocator#getPoolAllocator(String)}. Pick the pool that matches the
 *       semantics of the producing component:
 *       <ul>
 *         <li>{@code POOL_FLIGHT} — transport-layer producers/consumers (arrow-flight-rpc and
 *             plugins built on top of {@code StreamTransportService}).</li>
 *         <li>{@code POOL_INGEST} — ingest-path producers (parquet-data-format VSR allocators).</li>
 *         <li>{@code POOL_QUERY} — query-execution producers (analytics-engine fragments and
 *             coordinator-side intermediate batches).</li>
 *       </ul>
 *       All allocators must share the same root so zero-copy transfers pass Arrow's
 *       {@code AllocationManager} associate check.</li>
 *   <li><b>Send side:</b> Allocators must outlive the transport stream — some transports
 *       (e.g., gRPC zero-copy) retain buffer references beyond stream completion. Do not
 *       create and close a child allocator per request.</li>
 *   <li><b>Receive side:</b> The transport transfers vectors from its own allocator into
 *       the response. The consumer can then transfer them into its own allocator — which
 *       must also descend from one of the framework's named pools.</li>
 * </ul>
 *
 * <p><b>Cross-plugin footgun:</b> bypassing the framework's named pools
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
    private final byte[] metadata;

    /**
     * Send-side: wraps a root populated by the producer.
     *
     * @param batchRoot the root to send; ownership transfers to the transport
     */
    protected ArrowBatchResponse(VectorSchemaRoot batchRoot) {
        this(batchRoot, null);
    }

    /**
     * Send-side: wraps a root with opaque application metadata that travels on the same
     * Arrow Flight frame ({@code putNext(ArrowBuf)}). Use for per-batch metadata (row
     * offsets, watermarks) or, by attaching to the last batch, stream-terminal payloads
     * (profiling counters, summary stats).
     *
     * @param batchRoot the root to send; ownership transfers to the transport
     * @param metadata opaque bytes to attach to this batch, or {@code null}
     */
    protected ArrowBatchResponse(VectorSchemaRoot batchRoot, byte[] metadata) {
        this.batchRoot = batchRoot;
        this.metadata = metadata;
    }

    /**
     * Receive-side: claims ownership of the batch and pulls any attached metadata.
     *
     * @param in must also implement {@link ArrowStreamInput}
     * @throws IOException if reading fails
     */
    protected ArrowBatchResponse(StreamInput in) throws IOException {
        super(in);
        if (in instanceof ArrowStreamInput arrowIn) {
            this.batchRoot = arrowIn.getRoot();
            this.metadata = arrowIn.getMetadata();
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

    /** Returns the application metadata attached to this batch, or {@code null} if none. */
    public byte[] getMetadata() {
        return metadata;
    }

    @Override
    public final void writeTo(StreamOutput out) {
        // Symmetric fail-fast with the receive-side constructor: an Arrow-aware transport
        // transfers vectors directly and never invokes writeTo. Reaching this method means the
        // response was routed through a non-Arrow transport, which would silently drop the
        // batch — surface it instead of producing an empty response on the wire.
        throw new UnsupportedOperationException("ArrowBatchResponse is serialized by the Arrow-aware transport, not via StreamOutput");
    }
}

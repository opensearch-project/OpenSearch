/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link ResponseCodec} that deserializes the Arrow IPC stream payload carried
 * by {@link FragmentExecutionResponse} into a single consolidated
 * {@link VectorSchemaRoot}. Uses Arrow's {@link ArrowStreamReader} for message
 * sequencing (schema header, record batches, end-of-stream) and
 * {@link FieldVector#makeTransferPair} / {@link FieldVector#copyFromSafe} to
 * move data into a caller-owned root — both are supported by every vector
 * kind, including the view vectors ({@code Utf8View}, {@code BinaryView}) that
 * DataFusion emits for aggregate group keys.
 *
 * <p>Deliberately avoids {@code VectorSchemaRootAppender} — its underlying
 * {@code VectorAppender} rejects view vectors with
 * {@code UnsupportedOperationException}.
 *
 * @opensearch.internal
 */
public final class RowResponseCodec implements ResponseCodec<FragmentExecutionResponse> {

    /** Singleton instance — stateless, thread-safe. */
    public static final RowResponseCodec INSTANCE = new RowResponseCodec();

    private RowResponseCodec() {}

    @Override
    public VectorSchemaRoot decode(FragmentExecutionResponse response, BufferAllocator allocator) {
        if (allocator == null) {
            throw new IllegalArgumentException("BufferAllocator must not be null");
        }
        byte[] payload = response.getIpcPayload();
        if (payload == null || payload.length == 0) {
            return VectorSchemaRoot.create(new Schema(List.of()), allocator);
        }

        List<VectorSchemaRoot> batches = new ArrayList<>();
        Schema schema;
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(payload), allocator)) {
            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
            schema = readerRoot.getSchema();
            while (reader.loadNextBatch()) {
                // Transfer each batch's buffers out of the reader's reused root into an
                // independent root owned by this codec. Transfer works for all vector
                // kinds (including views) and is zero-copy when allocators match.
                VectorSchemaRoot batchRoot = VectorSchemaRoot.create(schema, allocator);
                int rowCount = readerRoot.getRowCount();
                for (int i = 0; i < readerRoot.getFieldVectors().size(); i++) {
                    readerRoot.getVector(i).makeTransferPair(batchRoot.getVector(i)).transfer();
                }
                batchRoot.setRowCount(rowCount);
                batches.add(batchRoot);
            }
        } catch (IOException e) {
            for (VectorSchemaRoot b : batches) b.close();
            throw new IllegalStateException("Failed to decode Arrow IPC payload from fragment response", e);
        }

        if (batches.isEmpty()) {
            return VectorSchemaRoot.create(schema, allocator);
        }
        if (batches.size() == 1) {
            return batches.get(0);
        }
        // Multiple batches — concatenate via per-cell copyFromSafe. Slower than columnar
        // append but is the only operation Arrow Java implements for every vector type
        // (VectorAppender throws on view vectors; see class javadoc).
        int totalRows = batches.stream().mapToInt(VectorSchemaRoot::getRowCount).sum();
        VectorSchemaRoot combined = VectorSchemaRoot.create(schema, allocator);
        try {
            combined.allocateNew();
            for (int f = 0; f < combined.getFieldVectors().size(); f++) {
                FieldVector dst = combined.getVector(f);
                int offset = 0;
                for (VectorSchemaRoot batch : batches) {
                    FieldVector src = batch.getVector(f);
                    int rows = batch.getRowCount();
                    for (int r = 0; r < rows; r++) {
                        dst.copyFromSafe(r, offset + r, src);
                    }
                    offset += rows;
                }
                dst.setValueCount(totalRows);
            }
            combined.setRowCount(totalRows);
            return combined;
        } finally {
            for (VectorSchemaRoot b : batches) b.close();
        }
    }
}

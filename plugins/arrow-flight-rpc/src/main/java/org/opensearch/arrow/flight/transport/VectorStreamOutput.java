/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

class VectorStreamOutput extends StreamOutput {

    private int row = 0;
    private final VarBinaryVector vector;
    private VectorSchemaRoot root;
    private final byte[] tempBuffer = new byte[8192];
    private int tempBufferPos = 0;

    public VectorStreamOutput(BufferAllocator allocator, VectorSchemaRoot root) {
        if (root != null) {
            vector = (VarBinaryVector) root.getVector(0);
            this.root = root;
        } else {
            Field field = new Field("0", new FieldType(true, new ArrowType.Binary(), null, null), null);
            vector = (VarBinaryVector) field.createVector(allocator);
            // Pre-allocate with reasonable capacity to avoid repeated allocations
            vector.setInitialCapacity(16);
            vector.allocateNew();
        }
    }

    @Override
    public void writeByte(byte b) {
        // Buffer small writes to reduce vector operations
        if (tempBufferPos >= tempBuffer.length) {
            flushTempBuffer();
        }
        tempBuffer[tempBufferPos++] = b;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        if (length == 0) {
            return;
        }
        if (b.length < (offset + length)) {
            throw new IllegalArgumentException("Illegal offset " + offset + "/length " + length + " for byte[] of length " + b.length);
        }
        if (tempBufferPos > 0) {
            flushTempBuffer();
        }
        vector.setSafe(row++, b, offset, length);
    }

    private void flushTempBuffer() {
        if (tempBufferPos > 0) {
            vector.setSafe(row++, tempBuffer, 0, tempBufferPos);
            tempBufferPos = 0;
        }
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws IOException {
        row = 0;
        vector.close();
    }

    @Override
    public void reset() {
        row = 0;
        tempBufferPos = 0;
        vector.clear();
    }

    public VectorSchemaRoot getRoot() {
        flushTempBuffer();
        vector.setValueCount(row);
        if (root == null) {
            root = new VectorSchemaRoot(List.of(vector));
        }
        root.setRowCount(row);
        return root;
    }
}

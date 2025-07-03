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
import java.util.Optional;

class VectorStreamOutput extends StreamOutput {

    private int row = 0;
    private final VarBinaryVector vector;
    private Optional<VectorSchemaRoot> root = Optional.empty();

    public VectorStreamOutput(BufferAllocator allocator, Optional<VectorSchemaRoot> root) {
        if (root.isPresent()) {
            vector = (VarBinaryVector) root.get().getVector(0);
            this.root = root;
        } else {
            Field field = new Field("0", new FieldType(true, new ArrowType.Binary(), null, null), null);
            vector = (VarBinaryVector) field.createVector(allocator);
        }
        vector.allocateNew();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        vector.setInitialCapacity(row + 1);
        vector.setSafe(row++, new byte[] { b });
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        vector.setInitialCapacity(row + 1);
        if (length == 0) {
            return;
        }
        if (b.length < (offset + length)) {
            throw new IllegalArgumentException("Illegal offset " + offset + "/length " + length + " for byte[] of length " + b.length);
        }
        vector.setSafe(row++, b, offset, length);
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() throws IOException {
        row = 0;
        vector.close();
    }

    @Override
    public void reset() throws IOException {
        row = 0;
        vector.clear();
    }

    public VectorSchemaRoot getRoot() {
        vector.setValueCount(row);
        if (!root.isPresent()) {
            root = Optional.of(new VectorSchemaRoot(List.of(vector)));
        }
        root.get().setRowCount(row);
        return root.get();
    }
}

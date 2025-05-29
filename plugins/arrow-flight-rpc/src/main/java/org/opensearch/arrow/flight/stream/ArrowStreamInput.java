/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.stream;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.text.Text;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ArrowStreamInput extends StreamInput {
    private final VectorSchemaRoot root;
    private final ArrowStreamOutput.PathManager pathManager;
    private final Map<String, List<FieldVector>> vectorsByPath;
    private final NamedWriteableRegistry registry;

    public ArrowStreamInput(VectorSchemaRoot root, NamedWriteableRegistry registry) {
        this.root = root;
        this.registry = registry;
        this.pathManager = new ArrowStreamOutput.PathManager();
        this.vectorsByPath = new HashMap<>();
        pathManager.row.put(pathManager.getCurrentPath(), 0);
        pathManager.column.put(pathManager.getCurrentPath(), 0);

        for (FieldVector vector : root.getFieldVectors()) {
            String fieldName = vector.getField().getName();
            String parentPath = extractParentPath(fieldName);
            vectorsByPath.computeIfAbsent(parentPath, k -> new ArrayList<>()).add(vector);
        }
    }

    private String extractParentPath(String fieldName) {
        int lastDot = fieldName.lastIndexOf('.');
        return lastDot == -1 ? "root" : fieldName.substring(0, lastDot);
    }

    private FieldVector getVector(String path, int colIndex) {
        List<FieldVector> vectors = vectorsByPath.get(path);
        if (vectors == null || colIndex >= vectors.size()) {
            throw new RuntimeException("No vector found for path: " + path + ", column: " + colIndex);
        }
        return vectors.get(colIndex);
    }

    private <T extends FieldVector, R> R readPrimitive(Class<T> vectorType, ValueExtractor<T, R> extractor) throws IOException {
        int colOrd = pathManager.addChild();
        String path = pathManager.getCurrentPath();
        FieldVector vector = getVector(path, colOrd);
        if (!vectorType.isInstance(vector)) {
            throw new IOException("Expected " + vectorType.getSimpleName() + " for path: " + path + ", column: " + colOrd);
        }
        T typedVector = vectorType.cast(vector);
        int rowIndex = pathManager.getCurrentRow();
        if (rowIndex >= typedVector.getValueCount() || typedVector.isNull(rowIndex)) {
            throw new EOFException("No more data at path: " + path + ", row: " + rowIndex);
        }
        return extractor.extract(typedVector, rowIndex);
    }

    @FunctionalInterface
    private interface ValueExtractor<T extends FieldVector, R> {
        R extract(T vector, int index);
    }

    @Override
    public byte readByte() throws IOException {
        return readPrimitive(TinyIntVector.class, TinyIntVector::get);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        byte[] data = readPrimitive(VarBinaryVector.class, VarBinaryVector::get);
        if (data.length != len) {
            throw new IOException("Expected " + len + " bytes, got " + data.length);
        }
        System.arraycopy(data, 0, b, offset, len);
    }

    @Override
    public String readString() throws IOException {
        return readPrimitive(VarCharVector.class, (vector, index) -> new String(vector.get(index), StandardCharsets.UTF_8));
    }

    @Override
    public int readInt() throws IOException {
        return readPrimitive(IntVector.class, IntVector::get);
    }

    @Override
    public long readLong() throws IOException {
        return readPrimitive(BigIntVector.class, BigIntVector::get);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readPrimitive(BitVector.class, (vector, index) -> vector.get(index) == 1);
    }

    @Override
    public float readFloat() throws IOException {
        return readPrimitive(Float4Vector.class, Float4Vector::get);
    }

    @Override
    public double readDouble() throws IOException {
        return readPrimitive(Float8Vector.class, Float8Vector::get);
    }

    @Override
    public int readVInt() throws IOException {
        return readInt();
    }

    @Override
    public long readVLong() throws IOException {
        return readLong();
    }

    @Override
    public long readZLong() throws IOException {
        return readLong();
    }

    @Override
    public Text readText() throws IOException {
        return new Text(readString());
    }

    @Override
    public <C extends NamedWriteable> C readNamedWriteable(Class<C> categoryClass) throws IOException {
        int colOrd = pathManager.addChild();
        String path = pathManager.getCurrentPath();
        FieldVector vector = getVector(path, colOrd);
        if (!(vector instanceof StructVector structVector)) {
            throw new IOException("Expected StructVector for NamedWriteable at path: " + path + ", column: " + colOrd);
        }
        String name = structVector.getField().getMetadata().getOrDefault("name", "");
        if (name.isEmpty()) {
            throw new IOException("No 'name' metadata found for NamedWriteable at path: " + path + ", column: " + colOrd);
        }
        pathManager.moveToChild(true);
        Writeable.Reader<? extends C> reader = namedWriteableRegistry().getReader(categoryClass, name);
        C result = reader.read(this);
        pathManager.moveToParent();
        return result;
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {}

    @Override
    public NamedWriteableRegistry namedWriteableRegistry() {
        return registry;
    }

    @Override
    public <T> List<T> readList(final Writeable.Reader<T> reader) throws IOException {
        int colOrd = pathManager.addChild();
        String path = pathManager.getCurrentPath();
        FieldVector vector = getVector(path, colOrd);
        if (!(vector instanceof StructVector)) {
            throw new IOException("Expected StructVector for list at path: " + path + ", column: " + colOrd);
        }
        pathManager.moveToChild(true);
        List<T> result = new ArrayList<>();
        List<FieldVector> childVectors = vectorsByPath.getOrDefault(pathManager.getCurrentPath(), Collections.emptyList());
        int maxRows = childVectors.stream().mapToInt(FieldVector::getValueCount).min().orElse(0);
        while (pathManager.getCurrentRow() < maxRows) {
            try {
                result.add(reader.read(this));
                if (!this.readBoolean()) {
                    pathManager.nextRow();
                    break;
                }
                pathManager.nextRow();
            } catch (EOFException e) {
                break;
            }
        }
        pathManager.moveToParent();
        return result;
    }

    @Override
    public Map<String, Object> readMap() throws IOException {
        int colOrd = pathManager.addChild();
        String path = pathManager.getCurrentPath();
        FieldVector vector = getVector(path, colOrd);
        if (!(vector instanceof StructVector structVector)) {
            throw new IOException("Expected StructVector for map at path: " + path + ", column: " + colOrd);
        }
        int rowIndex = pathManager.getCurrentRow();
        if (structVector.isNull(rowIndex)) {
            return Collections.emptyMap();
        } else {
            throw new UnsupportedOperationException("Currently unsupported.");
        }
    }

    @Override
    public void close() throws IOException {
        root.close();
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int available() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() throws IOException {
        pathManager.reset();
        pathManager.row.put(pathManager.getCurrentPath(), 0);
        pathManager.column.put(pathManager.getCurrentPath(), 0);
    }
}

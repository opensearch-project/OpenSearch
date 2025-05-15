/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.stream;

import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Provides serialization and deserialization of data to and from Apache Arrow vectors, implementing OpenSearch's
 * {@link StreamOutput} and {@link org.opensearch.core.common.io.stream.StreamInput} interfaces. This class organizes data in a hierarchical structure
 * using Arrow's {@link VectorSchemaRoot} and {@link FieldVector} to represent columns and nested structures.
 * The serialization process follows a strict column-ordering scheme, where fields are named based on their ordinal
 * position in the serialization order, ensuring deterministic and consistent data layout for both writing and reading.
 *
 * <p><b>Serialization and Deserialization Specification:</b></p>
 * <ol>
 *   <li><b>Primitive Types</b>:
 *       Primitive types (byte, int, long, boolean, float, double, string, and byte arrays) are serialized as individual
 *       columns under the current root path in the {@link VectorSchemaRoot}. Each column is named using the format
 *       <code>{currentPath}.{ordinal}</code>, where <code>ordinal</code> represents the order in which the primitive is
 *       written, starting from 0. For example, if the current root path is <code>"root"</code> and three primitives are
 *       written, their column names will be <code>"root.0"</code>, <code>"root.1"</code>, and <code>"root.2"</code>.
 *       The order of serialization is critical, as it determines the column names and must match during deserialization.
 *       Each column is represented by an appropriate Arrow vector type (e.g., {@link TinyIntVector} for byte,
 *       {@link VarCharVector} for string, etc.), with values appended at the current row index of the root path.
 *   </li>
 *   <li><b>NamedWriteable Types</b>:
 *       {@link NamedWriteable} objects are treated as nested structures and serialized as a single column of type
 *       {@link StructVector} under the current root path. The column is named <code>{currentPath}.{ordinal}</code>,
 *       where <code>ordinal</code> is the next available column index. For example, if the current root path is
 *       <code>"root"</code> and the current column ordinal is 3, the struct column will be named <code>"root.3"</code>.
 *       The struct's fields are serialized under a nested path derived from the column name (e.g., <code>"root.3"</code>),
 *       with subfields named <code>"root.3.0"</code>, <code>"root.3.1"</code>, etc., based on their serialization order.
 *       The struct's metadata includes the <code>name</code> key, set to the {@link NamedWriteable#getWriteableName()}
 *       value, which is used during deserialization to identify the appropriate {@link Writeable.Reader}.
 *       The row index of the nested path inherits the parent path's row index to maintain structural consistency.
 *   </li>
 *   <li><b>List Types</b>:
 *       Lists of {@link Writeable} objects are serialized as a single column of type {@link StructVector} under the
 *       current root path, named <code>{currentPath}.{ordinal}</code>, where <code>ordinal</code> is the next available
 *       column index. For example, if the current root path is <code>"root"</code> and the current ordinal is 4, the
 *       list's struct column will be named <code>"root.4"</code>. The elements of the list are serialized under a nested
 *       path (e.g., <code>"root.4"</code>), with each element's fields named <code>"root.4.0"</code>,
 *       <code>"root.4.1"</code>, etc., based on their serialization order within the element. Each element is written
 *       at a new row index, starting from the parent path's row index, and a boolean flag is written after each element
 *       to indicate whether more elements follow (<code>true</code> for all but the last element, <code>false</code>
 *       for the last). All elements in the list must have the same type and structure to ensure consistent column layout
 *       across rows; otherwise, deserialization may fail due to mismatched schemas.
 *.      <ul>
 *         <li><b>List of Lists</b>:
 *             A list of lists (e.g., <code>List&lt;List&lt;T&gt;&gt;</code>, where <code>T</code> is a {@link Writeable})
 *             is serialized as a nested {@link StructVector} within the outer list's struct column. For example, if the
 *             outer list is serialized under <code>"root.4"</code>, each inner list is treated as a {@link Writeable}
 *             element and serialized as a nested {@link StructVector} column under the path <code>"root.4"</code>. If
 *             the inner list is the first element of the outer list, it occupies column <code>"root.4.0"</code>, with
 *             its fields named <code>"root.4.0.0"</code>, <code>"root.4.0.1"</code>, etc., based on the serialization
 *             order of its elements. Each inner list is written at a new row index under the outer list’s nested path,
 *             starting from the outer list’s row index, and a boolean flag is written after each inner list element to
 *             indicate continuation within the inner list. The outer list’s boolean flags indicate continuation of inner
 *             lists. All inner lists must have the same type and structure, and their elements must also be consistent
 *             in type and structure to ensure a uniform schema across rows. During deserialization, the outer list is
 *             read as a {@link StructVector}, and each inner list is deserialized as a nested {@link StructVector},
 *             with row indices and boolean flags used to determine the boundaries of inner and outer lists.
 *         </li>
 *       </ul>
 *   </li>
 *   <li><b>Map Types</b>:
 *       Maps (key-value pairs are serialized as a single column of type
 *       {@link StructVector} under the current root path, named <code>{currentPath}.{ordinal}</code>.
 *       Currently, only empty or null maps are supported, serialized as a null value in the
 *       struct vector at the current row index. Future implementations may support non-empty maps with key and value
 *       vectors (e.g., {@link VarCharVector} for keys and a uniform type for values).
 *   </li>
 * </ol>
 *
 * <p><b>Usage Notes:</b></p>
 * <ul>
 *   <li>The order of serialization must match the order of deserialization to ensure correct column alignment, as column
 *       names are based on ordinals determined by the sequence of write operations.</li>
 *   <li>All elements in a list must have the same type and structure to maintain a consistent schema across rows.
 *       Inconsistent structures may lead to deserialization errors due to mismatched column types.</li>
 *   <li>Ensure that the {@link org.opensearch.core.common.io.stream.NamedWriteableRegistry} provided to {@link ArrowStreamInput} contains readers for all
 *       {@link NamedWriteable} types serialized by {@link ArrowStreamInput}, using the same
 *       {@link NamedWriteable#getWriteableName()} value.</li>
 * </ul>
 */
class ArrowStreamOutput extends StreamOutput {
    private final BufferAllocator allocator;
    private final Map<String, VectorSchemaRoot> roots;
    private final PathManager pathManager;

    public ArrowStreamOutput(BufferAllocator allocator) {
        this.allocator = allocator;
        this.roots = new HashMap<>();
        this.pathManager = new PathManager();
    }

    private void addColumnToRoot(int colOrd, Field field) {
        String rootPath = pathManager.getCurrentPath();
        VectorSchemaRoot existingRoot = roots.get(rootPath);
        if (existingRoot != null && existingRoot.getFieldVectors().size() > colOrd) {
            throw new IllegalStateException(
                "new column can only be added at the end. "
                    + "Column ordinal passed ["
                    + colOrd
                    + "], total columns ["
                    + existingRoot.getFieldVectors().size()
                    + "]."
            );
        }
        List<Field> newFields = new ArrayList<>();
        List<FieldVector> fieldVectors = new ArrayList<>();
        if (existingRoot != null) {
            newFields.addAll(existingRoot.getSchema().getFields());
            fieldVectors.addAll(existingRoot.getFieldVectors());
        }
        newFields.add(field);
        FieldVector newVector = field.createVector(allocator);
        newVector.allocateNew();
        fieldVectors.add(newVector);
        roots.put(rootPath, new VectorSchemaRoot(newFields, fieldVectors));
    }

    @SuppressWarnings("unchecked")
    private <T extends FieldVector> void writeLeafValue(ArrowType type, BiConsumer<T, Integer> valueSetter) throws IOException {
        int colOrd = pathManager.addChild();
        int row = pathManager.getCurrentRow();
        if (row == 0) {
            // if row is 0, then its first time current column is visited, thus a new one must be created and added to the root.
            Field field = new Field(pathManager.getCurrentPath() + "." + colOrd, new FieldType(true, type, null, null), null);
            addColumnToRoot(colOrd, field);
        }
        T vector = (T) roots.get(pathManager.getCurrentPath()).getVector(colOrd);
        vector.setInitialCapacity(row + 1);
        valueSetter.accept(vector, row);
        vector.setValueCount(row + 1);
        roots.get(pathManager.getCurrentPath()).setRowCount(row + 1);
    }

    @Override
    public void writeByte(byte b) throws IOException {
        writeLeafValue(new ArrowType.Int(8, true), (TinyIntVector vector, Integer index) -> vector.setSafe(index, b));
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        writeLeafValue(new ArrowType.Binary(), (VarBinaryVector vector, Integer index) -> {
            if (length > 0) {
                byte[] data = new byte[length];
                System.arraycopy(b, offset, data, 0, length);
                vector.setSafe(index, data);
            } else {
                vector.setNull(index);
            }
        });
    }

    @Override
    public void writeString(String str) throws IOException {
        writeLeafValue(
            new ArrowType.Utf8(),
            (VarCharVector vector, Integer index) -> vector.setSafe(index, str.getBytes(StandardCharsets.UTF_8))
        );
    }

    @Override
    public void writeInt(int v) throws IOException {
        writeLeafValue(new ArrowType.Int(32, true), (IntVector vector, Integer index) -> vector.setSafe(index, v));
    }

    @Override
    public void writeLong(long v) throws IOException {
        writeLeafValue(new ArrowType.Int(64, true), (BigIntVector vector, Integer index) -> vector.setSafe(index, v));
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        writeLeafValue(new ArrowType.Bool(), (BitVector vector, Integer index) -> vector.setSafe(index, b ? 1 : 0));
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeLeafValue(
            new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
            (Float4Vector vector, Integer index) -> vector.setSafe(index, v)
        );
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLeafValue(
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
            (Float8Vector vector, Integer index) -> vector.setSafe(index, v)
        );
    }

    @Override
    public void writeVInt(int v) throws IOException {
        writeInt(v);
    }

    @Override
    public void writeVLong(long v) throws IOException {
        writeLong(v);
    }

    @Override
    public void writeNamedWriteable(NamedWriteable namedWriteable) throws IOException {
        int colOrd = pathManager.addChild();
        int row = pathManager.getCurrentRow();
        if (row == 0) {
            // setting the name of the writeable in metadata of the field
            Field field = new Field(
                pathManager.getCurrentPath() + "." + colOrd,
                new FieldType(true, new ArrowType.Struct(), null, Map.of("name", namedWriteable.getWriteableName())),
                null
            );
            addColumnToRoot(colOrd, field);
        }
        pathManager.moveToChild(true);
        namedWriteable.writeTo(this);
        pathManager.moveToParent();
    }

    /**
     * All elements of the list should be of same type with same structure and order of inner values even when of complex type
     * otherwise columns will mismatch across rows resulting in error. If that's the case, then loop yourself and write individual elements, that's inefficient but will work.
     * @param list
     * @throws IOException
     */
    @Override
    public void writeList(List<? extends Writeable> list) throws IOException {
        int colOrd = pathManager.addChild();
        int row = pathManager.getCurrentRow();
        if (row == 0) {
            Field field = new Field(pathManager.getCurrentPath() + "." + colOrd, new FieldType(true, new ArrowType.Struct(), null), null);
            addColumnToRoot(colOrd, field);
        }
        pathManager.moveToChild(false);
        for (int i = 0; i < list.size(); i++) {
            list.get(i).writeTo(this);
            this.writeBoolean((i + 1) < list.size());
            pathManager.nextRow();
        }
        pathManager.moveToParent();
    }

    @Override
    public void writeMap(@Nullable Map<String, Object> map) throws IOException {
        int colOrd = pathManager.addChild();
        int row = pathManager.getCurrentRow();
        if (row == 0) {
            Field structField = new Field(pathManager.getCurrentPath() + "." + colOrd, FieldType.nullable(new ArrowType.Struct()), null);
            addColumnToRoot(colOrd, structField);
        }
        StructVector structVector = (StructVector) roots.get(pathManager.getCurrentPath()).getVector(colOrd);
        structVector.setInitialCapacity(row + 1);
        if (map == null || map.isEmpty()) {
            structVector.setNull(row);
        } else {
            throw new UnsupportedOperationException("Currently unsupported.");
        }
        structVector.setValueCount(row + 1);
    }

    public VectorSchemaRoot getUnifiedRoot() {
        List<FieldVector> allFields = new ArrayList<>();
        for (VectorSchemaRoot root : roots.values()) {
            allFields.addAll(root.getFieldVectors());
        }
        return new VectorSchemaRoot(allFields);
    }

    @Override
    public void close() throws IOException {
        roots.values().forEach(VectorSchemaRoot::close);
    }

    @Override
    public void flush() throws IOException {
        throw new UnsupportedOperationException("Currently not supported.");
    }

    @Override
    public void reset() throws IOException {
        for (VectorSchemaRoot root : roots.values()) {
            root.close();
        }
        roots.clear();
        pathManager.reset();
    }

    static class PathManager {
        private String currentPath;
        final Map<String, Integer> row;
        final Map<String, Integer> column;

        PathManager() {
            this.currentPath = "root";
            this.row = new HashMap<>();
            this.column = new HashMap<>();
        }

        String getCurrentPath() {
            return currentPath;
        }

        int getCurrentRow() {
            return row.get(currentPath);
        }

        /**
         * Adds the child at the next available ordinal at current path
         * It increments the column and keeps the row same.
         * @return leaf ordinal
         */
        int addChild() {
            column.putIfAbsent(currentPath, 0);
            row.putIfAbsent(currentPath, 0);
            column.put(currentPath, column.get(currentPath) + 1);
            return column.get(currentPath) - 1;
        }

        /**
         * Ensure {@link #addChild()} is called before
         */
        void moveToChild(boolean propagateRow) {
            String parentPath = currentPath;
            currentPath = currentPath + "." + (column.get(currentPath) - 1);
            column.put(currentPath, 0);
            if (propagateRow) {
                row.put(currentPath, row.get(parentPath));
            }
        }

        void moveToParent() {
            currentPath = currentPath.substring(0, currentPath.lastIndexOf("."));
        }

        void nextRow() {
            row.put(currentPath, row.get(currentPath) + 1);
            column.put(currentPath, 0);
        }

        public void reset() {
            currentPath = "root";
            row.clear();
            column.clear();
        }
    }
}

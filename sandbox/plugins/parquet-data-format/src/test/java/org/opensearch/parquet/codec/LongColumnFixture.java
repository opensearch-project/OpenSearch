/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.nio.file.Path;
import java.util.List;

/**
 * Test helper that writes a real single-column {@code int64} Parquet file via {@link NativeParquetWriter},
 * so codec tests can read it back through the native cursor. Every {@code nullEvery}-th row is left null
 * when {@code nullEvery > 0}.
 */
public final class LongColumnFixture {

    private LongColumnFixture() {}

    /** The value written at a present row; a simple non-trivial function of the row index. */
    public static long valueAt(long row) {
        return row * 7 + 1;
    }

    public static void write(Path file, BufferAllocator allocator, String column, int rowCount, int nullEvery) throws Exception {
        FieldType fieldType = nullEvery > 0
            ? FieldType.nullable(new ArrowType.Int(64, true))
            : FieldType.notNullable(new ArrowType.Int(64, true));
        Schema schema = new Schema(List.of(new Field(column, fieldType, null)));

        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, schemaExport);
        try (ArrowExport export = new ArrowExport(null, schemaExport)) {
            writer.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector vector = (BigIntVector) root.getVector(column);
            vector.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (nullEvery > 0 && i % nullEvery == 0) {
                    vector.setNull(i);
                } else {
                    vector.setSafe(i, valueAt(i));
                }
            }
            vector.setValueCount(rowCount);
            root.setRowCount(rowCount);

            ArrowArray arrayExport = ArrowArray.allocateNew(allocator);
            ArrowSchema dataSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, arrayExport, dataSchema);
            try (ArrowExport export = new ArrowExport(arrayExport, dataSchema)) {
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
            }
        }
        writer.flush();
    }
}

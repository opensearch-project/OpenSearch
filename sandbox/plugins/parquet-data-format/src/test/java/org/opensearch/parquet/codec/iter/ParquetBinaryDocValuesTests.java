/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetColumnReader;
import org.opensearch.parquet.bridge.ParquetSortConfig;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.codec.ParquetPhysicalType;
import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * Unit tests for {@link ParquetBinaryDocValues}: per-document byte value and presence correctness
 * for a single-valued BYTE_ARRAY (text/binary) Parquet column, including null rows.
 */
public class ParquetBinaryDocValuesTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
        schema = new Schema(List.of(new Field("t", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testBinaryValuesAndNulls() throws Exception {
        Path file = writeStrings(new String[] { "hello", null, "world" });
        try (
            BufferPool pool = new BufferPool();
            ParquetColumnReader r = ParquetColumnReader.open(file, "t", ParquetPhysicalType.BYTE_ARRAY, false, pool)
        ) {
            ParquetBinaryDocValues dv = new ParquetBinaryDocValues(r, 3);

            assertTrue(dv.advanceExact(0));
            assertEquals(new BytesRef("hello"), BytesRef.deepCopyOf(dv.binaryValue()));

            assertFalse(dv.advanceExact(1)); // null row

            assertTrue(dv.advanceExact(2));
            assertEquals(new BytesRef("world"), BytesRef.deepCopyOf(dv.binaryValue()));
        }
    }

    private Path writeStrings(String[] values) throws Exception {
        Path file = createTempDir().resolve("text.parquet");
        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, schemaExport);
        try (ArrowExport s = new ArrowExport(null, schemaExport)) {
            writer.initialize("test-index", s.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarCharVector vec = (VarCharVector) root.getVector("t");
            for (int i = 0; i < values.length; i++) {
                if (values[i] == null) {
                    vec.setNull(i);
                } else {
                    vec.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
                }
            }
            root.setRowCount(values.length);
            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            try (ArrowExport export = new ArrowExport(array, arrowSchema)) {
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
            }
        }
        writer.flush();
        return file;
    }
}

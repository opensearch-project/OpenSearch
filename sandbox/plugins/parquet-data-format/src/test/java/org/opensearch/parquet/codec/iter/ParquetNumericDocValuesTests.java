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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetColumnReader;
import org.opensearch.parquet.bridge.ParquetSortConfig;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.codec.ParquetPhysicalType;
import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;

/**
 * Unit tests for {@link ParquetNumericDocValues}: per-document value and presence correctness for
 * a single-valued Parquet primitive column, including null rows and the end-of-range boundary.
 */
public class ParquetNumericDocValuesTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
        schema = new Schema(List.of(new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testAdvanceExactReturnsValuesAndPresence() throws Exception {
        Path file = writeLongs(new Long[] { 10L, null, 30L, 40L });
        try (
            BufferPool pool = new BufferPool();
            ParquetColumnReader r = ParquetColumnReader.open(file, "v", ParquetPhysicalType.INT64, false, pool)
        ) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(r, 4);

            assertTrue(dv.advanceExact(0));
            assertEquals(10L, dv.longValue());

            // Row 1 is null: advanceExact must report absent.
            assertFalse(dv.advanceExact(1));

            assertTrue(dv.advanceExact(2));
            assertEquals(30L, dv.longValue());

            assertTrue(dv.advanceExact(3));
            assertEquals(40L, dv.longValue());
        }
    }

    public void testAdvanceSkipsNullRows() throws Exception {
        Path file = writeLongs(new Long[] { null, 20L, null, 40L });
        try (
            BufferPool pool = new BufferPool();
            ParquetColumnReader r = ParquetColumnReader.open(file, "v", ParquetPhysicalType.INT64, false, pool)
        ) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(r, 4);
            assertEquals(1, dv.nextDoc());
            assertEquals(20L, dv.longValue());
            assertEquals(3, dv.advance(2));
            assertEquals(40L, dv.longValue());
            assertEquals(ParquetNumericDocValues.NO_MORE_DOCS, dv.nextDoc());
        }
    }

    public void testAdvanceExactBeyondMaxDoc() throws Exception {
        Path file = writeLongs(new Long[] { 1L, 2L });
        try (
            BufferPool pool = new BufferPool();
            ParquetColumnReader r = ParquetColumnReader.open(file, "v", ParquetPhysicalType.INT64, false, pool)
        ) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(r, 2);
            assertFalse(dv.advanceExact(2));
            assertEquals(ParquetNumericDocValues.NO_MORE_DOCS, dv.docID());
        }
    }

    private Path writeLongs(Long[] values) throws Exception {
        Path file = createTempDir().resolve("numeric.parquet");
        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, schemaExport);
        try (ArrowExport s = new ArrowExport(null, schemaExport)) {
            writer.initialize("test-index", s.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector vec = (BigIntVector) root.getVector("v");
            for (int i = 0; i < values.length; i++) {
                if (values[i] == null) {
                    vec.setNull(i);
                } else {
                    vec.setSafe(i, values[i]);
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

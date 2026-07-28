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
import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * Unit tests for {@link OrdinalTable#buildSingleValued}: it must assign globally sorted ordinals
 * (Lucene's per-segment-ordinal contract — ords ascend with term order), map each row to its
 * ordinal (or -1 for a null row), and round-trip {@code lookupOrd} back to the term bytes.
 */
public class OrdinalTableTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
        schema = new Schema(List.of(new Field("k", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testSingleValuedOrdinalsAreSortedAndRoundTrip() throws Exception {
        // Rows: "banana", "apple", "banana", null, "cherry".
        // Sorted distinct terms → ordinals: apple=0, banana=1, cherry=2.
        Path file = writeKeywords(new String[] { "banana", "apple", "banana", null, "cherry" });
        try (
            BufferPool pool = new BufferPool();
            ParquetColumnReader r = ParquetColumnReader.open(file, "k", ParquetPhysicalType.BYTE_ARRAY, false, pool)
        ) {
            OrdinalTable table = OrdinalTable.buildSingleValued(r, 5);

            assertEquals("distinct term count", 3, table.valueCount());
            assertEquals(1, table.ordForRow(0)); // banana
            assertEquals(0, table.ordForRow(1)); // apple
            assertEquals(1, table.ordForRow(2)); // banana
            assertEquals(-1, table.ordForRow(3)); // null row
            assertEquals(2, table.ordForRow(4)); // cherry

            assertEquals(new BytesRef("apple"), table.lookupOrd(0));
            assertEquals(new BytesRef("banana"), table.lookupOrd(1));
            assertEquals(new BytesRef("cherry"), table.lookupOrd(2));
        }
    }

    private Path writeKeywords(String[] values) throws Exception {
        Path file = createTempDir().resolve("keyword.parquet");
        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, schemaExport);
        try (ArrowExport s = new ArrowExport(null, schemaExport)) {
            writer.initialize("test-index", s.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarCharVector vec = (VarCharVector) root.getVector("k");
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

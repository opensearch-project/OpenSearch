/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * End-to-end tests of the native bit-packed row ID mapping produced by sort-on-close:
 * Rust packs forward+reverse at bpv bits/value, Java reads it zero-copy through
 * {@link NativePackedRowIdMapping}, and close() releases the native buffer exactly once.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class NativePackedRowIdMappingTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "native-mapping-test-index";
    private BufferAllocator allocator;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
        schema = new Schema(
            List.of(
                new Field("timestamp", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("__row_id__", FieldType.nullable(new ArrowType.Int(64, true)), null)
            )
        );
        NativeSettings settings = NativeSettings.builder().indexName(INDEX_NAME).compressionType("LZ4_RAW").build();
        RustBridge.onSettingsUpdate(settings);
    }

    @Override
    public void tearDown() throws Exception {
        RustBridge.removeSettings(INDEX_NAME);
        allocator.close();
        super.tearDown();
    }

    public void testSortedFlushProducesValidNativeMapping() throws Exception {
        // Unsorted input: sorting by timestamp yields a non-identity permutation.
        long[] timestamps = { 500, 100, 400, 200, 300 };
        String[] messages = { "e", "a", "d", "b", "c" };

        NativeParquetWriter writer = writeSortedFile("sorted.parquet", timestamps, messages);
        RowIdMapping mapping = writer.getRowIdMapping();

        assertNotNull("Sorted flush must produce a row ID mapping", mapping);
        assertTrue("Mapping must be native-packed", mapping instanceof NativePackedRowIdMapping);
        assertEquals(5, mapping.size());
        assertTrue(mapping.isNewToOldSupported());

        // Sorted order by timestamp: 100,200,300,400,500 → oldId 1,3,4,2,0.
        // Forward: mapping[oldId] = position of that row in sorted output.
        long[] expectedNewIds = { 4, 0, 3, 1, 2 };
        for (int oldId = 0; oldId < 5; oldId++) {
            assertEquals("forward mapping for oldId " + oldId, expectedNewIds[oldId], mapping.getNewRowId(oldId));
            // Reverse must invert forward exactly.
            assertEquals("reverse of forward for oldId " + oldId, oldId, mapping.getOldRowId(mapping.getNewRowId(oldId)));
        }

        // Out-of-range lookups return -1, matching PackedRowIdMapping semantics.
        assertEquals(-1L, mapping.getNewRowId(-1));
        assertEquals(-1L, mapping.getNewRowId(5));
        assertEquals(-1L, mapping.getOldRowId(-1));
        assertEquals(-1L, mapping.getOldRowId(5));

        writer.releaseRowIdMapping();
    }

    public void testCloseFreesNativeMemoryAndInvalidatesAccess() throws Exception {
        long before = NativePackedRowIdMapping.outstandingNativeBytes();

        NativeParquetWriter writer = writeSortedFile("close.parquet", new long[] { 300, 100, 200 }, new String[] { "c", "a", "b" });
        NativePackedRowIdMapping mapping = (NativePackedRowIdMapping) writer.getRowIdMapping();
        assertNotNull(mapping);

        assertTrue("mapping must hold native bytes", mapping.nativeBytesUsed() > 0);
        assertEquals(before + mapping.nativeBytesUsed(), NativePackedRowIdMapping.outstandingNativeBytes());

        mapping.close();
        assertEquals("native bytes must return to baseline after close", before, NativePackedRowIdMapping.outstandingNativeBytes());

        // Use-after-close must fail loudly, never read freed memory.
        expectThrows(IllegalStateException.class, () -> mapping.getNewRowId(0));
        expectThrows(IllegalStateException.class, () -> mapping.getOldRowId(0));

        // close() is idempotent.
        mapping.close();
        assertEquals(before, NativePackedRowIdMapping.outstandingNativeBytes());
    }

    public void testReleaseRowIdMappingIsIdempotentAndSafeWithoutMapping() throws Exception {
        // Writer that was never initialized has no mapping — release must be a no-op.
        NativeParquetWriter writer = new NativeParquetWriter(createTempDir().resolve("none.parquet").toString());
        writer.releaseRowIdMapping();

        // Writer with a mapping: double release must be safe.
        NativeParquetWriter sortedWriter = writeSortedFile("release.parquet", new long[] { 200, 100 }, new String[] { "b", "a" });
        assertNotNull(sortedWriter.getRowIdMapping());
        sortedWriter.releaseRowIdMapping();
        sortedWriter.releaseRowIdMapping();
    }

    public void testLargerPermutationRoundTrip() throws Exception {
        // Enough rows to exercise bpv > 8 and bit-boundary straddling within packed bytes.
        int n = 1000;
        long[] timestamps = new long[n];
        String[] messages = new String[n];
        // Descending timestamps → sort fully reverses the order.
        for (int i = 0; i < n; i++) {
            timestamps[i] = n - i;
            messages[i] = "m" + i;
        }

        NativeParquetWriter writer = writeSortedFile("large.parquet", timestamps, messages);
        RowIdMapping mapping = writer.getRowIdMapping();
        assertNotNull(mapping);
        assertEquals(n, mapping.size());

        // Full reversal: oldId i lands at newId n-1-i.
        for (int i = 0; i < n; i++) {
            assertEquals(n - 1 - i, mapping.getNewRowId(i));
            assertEquals(i, mapping.getOldRowId(n - 1 - i));
        }

        writer.releaseRowIdMapping();
    }

    /**
     * Writes a file with a __row_id__ column and a timestamp sort config through the
     * full native pipeline (createWriter → write → finalize) and returns the writer.
     */
    private NativeParquetWriter writeSortedFile(String name, long[] timestamps, String[] messages) throws Exception {
        String filePath = createTempDir().resolve(name).toString();
        ParquetSortConfig sortConfig = new ParquetSortConfig(List.of("timestamp"), List.of(false), List.of(false));

        try (ArrowExport schemaExport = exportSchema()) {
            NativeParquetWriter writer = new NativeParquetWriter(filePath);
            writer.initialize(INDEX_NAME, schemaExport.getSchemaAddress(), sortConfig, 0L);
            try (ArrowExport dataExport = exportData(timestamps, messages)) {
                writer.write(dataExport.getArrayAddress(), dataExport.getSchemaAddress());
            }
            writer.flush();
            return writer;
        }
    }

    private ArrowExport exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }

    private ArrowExport exportData(long[] timestamps, String[] messages) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector tsVec = (BigIntVector) root.getVector("timestamp");
            VarCharVector msgVec = (VarCharVector) root.getVector("message");
            BigIntVector rowIdVec = (BigIntVector) root.getVector("__row_id__");
            for (int i = 0; i < timestamps.length; i++) {
                tsVec.setSafe(i, timestamps[i]);
                msgVec.setSafe(i, messages[i].getBytes(StandardCharsets.UTF_8));
                rowIdVec.setSafe(i, i);
            }
            root.setRowCount(timestamps.length);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            return new ArrowExport(array, arrowSchema);
        }
    }
}

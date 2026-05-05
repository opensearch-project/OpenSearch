/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

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
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

public class ParquetMergeIntegrationTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "merge-test-index";
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
                new Field("message", FieldType.nullable(new ArrowType.Utf8()), null)
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testMergeSortedFiles() throws Exception {
        // 1. Push settings
        NativeSettings settings = NativeSettings.builder().indexName(INDEX_NAME).compressionType("LZ4_RAW").compressionLevel(2).build();
        RustBridge.onSettingsUpdate(settings);

        Path tempDir = createTempDir();

        // 2. Create 3 sorted files with non-overlapping timestamp ranges
        String file1 = createSortedFile(tempDir, "f1.parquet", new long[] { 100, 200, 300 }, new String[] { "a", "b", "c" });
        String file2 = createSortedFile(tempDir, "f2.parquet", new long[] { 400, 500, 600 }, new String[] { "d", "e", "f" });
        String file3 = createSortedFile(tempDir, "f3.parquet", new long[] { 700, 800, 900 }, new String[] { "g", "h", "i" });

        // Verify individual files
        assertEquals(3, RustBridge.getFileMetadata(file1).numRows());
        assertEquals(3, RustBridge.getFileMetadata(file2).numRows());
        assertEquals(3, RustBridge.getFileMetadata(file3).numRows());

        // 3. Merge
        String mergedFile = tempDir.resolve("merged.parquet").toString();
        RustBridge.mergeParquetFilesInRust(List.of(Path.of(file1), Path.of(file2), Path.of(file3)), mergedFile, INDEX_NAME);

        // 4. Verify merged output
        ParquetFileMetadata mergedMeta = RustBridge.getFileMetadata(mergedFile);
        assertEquals(9, mergedMeta.numRows());

        // 5. Cleanup
        RustBridge.removeSettings(INDEX_NAME);
    }

    public void testMergeWithInterleavedTimestamps() throws Exception {
        NativeSettings settings = NativeSettings.builder().indexName(INDEX_NAME).compressionType("LZ4_RAW").build();
        RustBridge.onSettingsUpdate(settings);

        Path tempDir = createTempDir();

        // Interleaved ranges — merge must sort globally
        String file1 = createSortedFile(tempDir, "f1.parquet", new long[] { 100, 300, 500 }, new String[] { "a", "c", "e" });
        String file2 = createSortedFile(tempDir, "f2.parquet", new long[] { 200, 400, 600 }, new String[] { "b", "d", "f" });

        String mergedFile = tempDir.resolve("merged.parquet").toString();
        RustBridge.mergeParquetFilesInRust(List.of(Path.of(file1), Path.of(file2)), mergedFile, INDEX_NAME);

        assertEquals(6, RustBridge.getFileMetadata(mergedFile).numRows());

        RustBridge.removeSettings(INDEX_NAME);
    }

    public void testMergeSingleFile() throws Exception {
        NativeSettings settings = NativeSettings.builder().indexName(INDEX_NAME).compressionType("LZ4_RAW").build();
        RustBridge.onSettingsUpdate(settings);

        Path tempDir = createTempDir();
        String file1 = createSortedFile(tempDir, "f1.parquet", new long[] { 10, 20, 30 }, new String[] { "x", "y", "z" });

        String mergedFile = tempDir.resolve("merged.parquet").toString();
        RustBridge.mergeParquetFilesInRust(List.of(Path.of(file1)), mergedFile, INDEX_NAME);

        assertEquals(3, RustBridge.getFileMetadata(mergedFile).numRows());

        RustBridge.removeSettings(INDEX_NAME);
    }

    /**
     * Creates a sorted Parquet file via the full Rust writer pipeline:
     * createWriter (with sort config) → write → finalizeWriter.
     */
    private String createSortedFile(Path dir, String name, long[] timestamps, String[] messages) throws Exception {
        String filePath = dir.resolve(name).toString();
        ParquetSortConfig sortConfig = new ParquetSortConfig(List.of("timestamp"), List.of(false), List.of(false));

        try (ArrowExport schemaExport = exportSchema()) {
            NativeParquetWriter writer = new NativeParquetWriter(filePath, INDEX_NAME, schemaExport.getSchemaAddress(), sortConfig);

            try (ArrowExport dataExport = exportData(timestamps, messages)) {
                writer.write(dataExport.getArrayAddress(), dataExport.getSchemaAddress());
            }

            writer.flush();
        }
        return filePath;
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
            for (int i = 0; i < timestamps.length; i++) {
                tsVec.setSafe(i, timestamps[i]);
                msgVec.setSafe(i, messages[i].getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(timestamps.length);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            return new ArrowExport(array, arrowSchema);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.encryption.PmeDataKey;
import org.opensearch.parquet.encryption.PmeFileEncryptionInputs;
import org.opensearch.parquet.encryption.PmeFileKeyMetadata;
import org.opensearch.parquet.encryption.PmeKeyDerivation;
import org.opensearch.test.OpenSearchTestCase;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.arrow.c.Data.importField;

/**
 * End-to-end test: write a PME-encrypted Parquet file, then query it through DataFusion's
 * SQL execution path with decryption keys supplied to {@link ReaderHandle}.
 *
 * <p>This is the encrypted companion of {@link DataFusionQueryExecutionTests}. The test:
 * <ol>
 *   <li>Writes an encrypted Parquet file using {@link NativeParquetWriter} with a derived
 *       PME footer key (as the real write path does via {@link PmeFileEncryptionInputs}).</li>
 *   <li>Parses the metadata and re-derives the footer key + AAD prefix from the same data key
 *       (simulating what the read path does via {@code PmeContext}).</li>
 *   <li>Creates a {@link ReaderHandle} that supplies the footer key and AAD prefix to
 *       DataFusion's decryption factory.</li>
 *   <li>Runs the same SQL queries as {@link DataFusionQueryExecutionTests} and asserts
 *       identical results.</li>
 * </ol>
 */
public class DataFusionEncryptedQueryExecutionTests extends OpenSearchTestCase {

    private static final String TABLE = "enc_table";
    private static final String FILE = "encrypted.parquet";

    private NativeRuntimeHandle runtimeHandle;
    private Path dataDir;
    private Arena configArena;
    private long queryConfigPtr;

    /** The 32-byte data key used to derive per-file footer keys. */
    private byte[] dataKey;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        runtimeHandle = new NativeRuntimeHandle(
            NativeBridge.createGlobalRuntime(128 * 1024 * 1024, 0L, spillDir.toString(), 64 * 1024 * 1024)
        );

        // Build a valid WireConfigSnapshot so Rust's from_ffm_ptr never sees a null pointer.
        configArena = Arena.ofConfined();
        MemorySegment configSegment = configArena.allocate(WireConfigSnapshot.BYTE_SIZE);
        WireConfigSnapshot.builder().build().writeTo(configSegment);
        queryConfigPtr = configSegment.address();

        dataKey = randomByteArrayOfLength(PmeKeyDerivation.DATA_KEY_BYTES);
        dataDir = createTempDir("datafusion-encrypted");
        writeEncryptedParquet(dataDir.resolve(FILE).toString());
    }

    @Override
    public void tearDown() throws Exception {
        configArena.close();
        runtimeHandle.close();
        super.tearDown();
    }

    // ---- tests ----

    public void testSelectAllEncrypted() throws Exception {
        try (ReaderHandle reader = encryptedReader()) {
            List<Object[]> rows = executeQuery(reader, "SELECT id, score FROM " + TABLE);
            assertEquals(3, rows.size());
            assertEquals(1L, rows.get(0)[0]);
            assertEquals(100L, rows.get(0)[1]);
            assertEquals(2L, rows.get(1)[0]);
            assertEquals(200L, rows.get(1)[1]);
            assertEquals(3L, rows.get(2)[0]);
            assertEquals(300L, rows.get(2)[1]);
        }
    }

    public void testFilterEncrypted() throws Exception {
        try (ReaderHandle reader = encryptedReader()) {
            List<Object[]> rows = executeQuery(reader, "SELECT id FROM " + TABLE + " WHERE id > 1");
            assertEquals(2, rows.size());
            assertEquals(2L, rows.get(0)[0]);
            assertEquals(3L, rows.get(1)[0]);
        }
    }

    public void testAggregationEncrypted() throws Exception {
        try (ReaderHandle reader = encryptedReader()) {
            List<Object[]> rows = executeQuery(reader, "SELECT SUM(score) as total, COUNT(*) as cnt FROM " + TABLE);
            assertEquals(1, rows.size());
            assertEquals(600L, rows.get(0)[0]); // 100 + 200 + 300
            assertEquals(3L, rows.get(0)[1]);
        }
    }

    /**
     * Verifies that DataFusion rejects the encrypted file when no decryption key is provided.
     * An unencrypted {@link ReaderHandle} must fail to query the file.
     */
    public void testUnencryptedReaderFailsOnEncryptedFile() {
        try (ReaderHandle reader = new ReaderHandle(dataDir.toString(), new String[] { FILE })) {
            expectThrows(Exception.class, () -> executeQuery(reader, "SELECT id FROM " + TABLE));
        }
    }

    // ---- helpers ----

    /**
     * Writes a small encrypted Parquet file to {@code filePath} with three rows:
     * (id=1, score=100), (id=2, score=200), (id=3, score=300).
     *
     * <p>Uses {@link NativeParquetWriter} with a {@link PmeFileEncryptionInputs} derived from
     * {@link #dataKey}, mirroring the production write path.
     */
    private void writeEncryptedParquet(String filePath) throws Exception {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("score", FieldType.nullable(new ArrowType.Int(64, true)), null)
            )
        );

        PmeFileEncryptionInputs enc = PmeFileEncryptionInputs.create(new PmeDataKey(dataKey));

        try (
            RootAllocator allocator = new RootAllocator();
            ArrowExport schemaExport = exportSchema(allocator, schema)
        ) {
            NativeParquetWriter writer = new NativeParquetWriter(filePath, schemaExport.getSchemaAddress(), enc);

            int[][] rows = { { 1, 100 }, { 2, 200 }, { 3, 300 } };
            for (int[] r : rows) {
                try (ArrowExport batch = exportRow(allocator, schema, r[0], r[1])) {
                    writer.write(batch.getArrayAddress(), batch.getSchemaAddress());
                }
            }
            writer.flush();
        }
    }

    /**
     * Creates a {@link ReaderHandle} with the derived footer key and AAD prefix, read from
     * the Parquet file's key_metadata footer field and derived via the same data key.
     */
    private ReaderHandle encryptedReader() throws Exception {
        String filePath = dataDir.resolve(FILE).toString();
        byte[] keyMetadataBytes = RustBridge.readParquetKeyMetadata(filePath);
        assertNotNull("Encrypted file must contain key_metadata", keyMetadataBytes);

        PmeFileKeyMetadata meta = PmeFileKeyMetadata.parse(keyMetadataBytes);
        assertEquals(PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID, meta.dataKeyId());

        byte[] footerKey = PmeKeyDerivation.deriveFooterKey(dataKey, meta.messageId());
        byte[] aadPrefix = PmeKeyDerivation.buildAadPrefix(meta.messageId());

        return new ReaderHandle(
            dataDir.toString(),
            new String[] { FILE },
            new String[] { FILE },
            new byte[][] { footerKey },
            new String[] { FILE },
            new byte[][] { aadPrefix }
        );
    }

    private List<Object[]> executeQuery(ReaderHandle reader, String sql) {
        byte[] substraitBytes = NativeBridge.sqlToSubstrait(reader.getPointer(), TABLE, sql, runtimeHandle.get());
        assertNotNull(substraitBytes);
        assertTrue(substraitBytes.length > 0);

        long streamPtr = asyncCall(
            listener -> NativeBridge.executeQueryAsync(reader.getPointer(), TABLE, substraitBytes, runtimeHandle.get(), 0L, queryConfigPtr, listener)
        );
        assertTrue(streamPtr != 0);

        try (
            StreamHandle streamHandle = new StreamHandle(streamPtr, runtimeHandle);
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()
        ) {
            long schemaAddr = asyncCall(listener -> NativeBridge.streamGetSchema(streamHandle.getPointer(), listener));
            Schema schema = new Schema(importField(allocator, ArrowSchema.wrap(schemaAddr), dictProvider).getChildren(), null);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            List<Object[]> rows = new ArrayList<>();

            while (true) {
                long arrayAddr = asyncCall(
                    listener -> NativeBridge.streamNext(runtimeHandle.get(), streamHandle.getPointer(), listener)
                );
                if (arrayAddr == 0) break;
                Data.importIntoVectorSchemaRoot(allocator, ArrowArray.wrap(arrayAddr), root, dictProvider);
                int cols = root.getFieldVectors().size();
                for (int r = 0; r < root.getRowCount(); r++) {
                    Object[] row = new Object[cols];
                    for (int c = 0; c < cols; c++) {
                        row[c] = root.getFieldVectors().get(c).getObject(r);
                    }
                    rows.add(row);
                }
            }
            root.close();
            return rows;
        }
    }

    private long asyncCall(Consumer<ActionListener<Long>> call) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        call.accept(new ActionListener<>() {
            @Override
            public void onResponse(Long v) { future.complete(v); }
            @Override
            public void onFailure(Exception e) { future.completeExceptionally(e); }
        });
        return future.join();
    }

    private static ArrowExport exportSchema(RootAllocator allocator, Schema schema) {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }

    private static ArrowExport exportRow(RootAllocator allocator, Schema schema, long id, long score) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            org.apache.arrow.vector.BigIntVector idVec = (org.apache.arrow.vector.BigIntVector) root.getVector("id");
            org.apache.arrow.vector.BigIntVector scoreVec = (org.apache.arrow.vector.BigIntVector) root.getVector("score");
            idVec.setSafe(0, id);
            scoreVec.setSafe(0, score);
            root.setRowCount(1);
            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            return new ArrowExport(array, arrowSchema);
        }
    }
}




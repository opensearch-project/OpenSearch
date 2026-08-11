/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Regression test for the "Writer already exists" recovery failure.
 *
 * <p>When the native Parquet writer is left un-finalized/un-closed on the Rust side (e.g. a
 * Rust-side failure aborts a flush before {@code finalize}, or the shard fails without a clean
 * writer close), the native registry keeps the entry. On shard recovery the engine re-creates a
 * writer for the <em>same</em> generation — and therefore the same underlying file/temp name.
 * With the filename-keyed native registry this second creation throws
 * {@code "Writer already exists for this file"}, so the shard can never come back up.
 *
 * <p>This test reproduces exactly that sequence at the Java↔Rust boundary: initialize a writer for
 * a file and deliberately never close/finalize it (the abandoned/dangling writer), then initialize
 * a second writer for the same file (the recovery re-create). Recovery must succeed.
 *
 * <p><b>Expected:</b> FAILS on the current (filename-keyed) implementation — the second
 * {@code initialize} throws {@code IOException("Writer already exists for this file")}. Once the
 * native writer lifecycle is owned by Java via opaque handles (no filename collision), it PASSES.
 * This test is intentionally left unchanged across that fix so it acts as the regression guard.
 *
 * <p>Note: it does not close the writers (no {@code close()} exists pre-fix); post-fix the handles
 * are reclaimed by the {@code Cleaner} backstop when the writers become unreachable.
 */
public class NativeParquetWriterRecoveryTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
        schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testRecoveryReCreatesWriterForSameFileAfterUnclosedWriter() throws Exception {
        String filePath = createTempDir().resolve("recovery.parquet").toString();

        // Writer 1: initialize the native writer, then abandon it WITHOUT finalize/close —
        // simulating a Rust-side failure where cleanup never ran.
        NativeParquetWriter writer1 = new NativeParquetWriter(filePath);
        try (ArrowExport export = exportSchema()) {
            writer1.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        assertTrue("writer1 should initialize", writer1.isInitialized());

        // Recovery: a fresh writer for the SAME file (same generation -> same temp file name).
        // On the current filename-keyed native registry this throws "Writer already exists",
        // which is what leaves a recovering shard stuck red. It must be allowed to succeed.
        NativeParquetWriter writer2 = new NativeParquetWriter(filePath);
        try (ArrowExport export = exportSchema()) {
            writer2.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }
        assertTrue(
            "recovery must be able to re-create a native writer for the same file after an unclosed writer",
            writer2.isInitialized()
        );
    }

    private ArrowExport exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }
}

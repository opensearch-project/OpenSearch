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
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.common.io.PathUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

/**
 * Smoke test for the coordinator-reduce FFM wrappers added by the datafusion-coordinator-reduce spec.
 *
 * <p>Exercises each new {@link NativeBridge} wrapper against a real native library + global
 * runtime. Mirrors the lifecycle pattern used by {@link DataFusionNativeBridgeTests} — each test
 * creates its own per-test runtime and closes it at the end.
 *
 * <p>Pointer handling follows the plugin convention: raw pointers returned by {@link NativeBridge}
 * are wrapped in {@link org.opensearch.analytics.backend.jni.NativeHandle} subclasses
 * ({@link NativeRuntimeHandle}, {@link DatafusionLocalSession}) so they are registered in the
 * live-handle set that {@link NativeBridge}'s {@code validatePointer} guards check.
 */
public class NativeBridgeLocalSessionTests extends OpenSearchTestCase {

    // Spill dirs handed to the native runtime, deleted best-effort in tearDown. NOT Lucene temp dirs
    // (see createRuntime) — the test owns their teardown so it never races the native cleanup thread.
    private final List<Path> spillDirs = new ArrayList<>();

    private NativeRuntimeHandle createRuntime() {
        NativeBridge.initTokioRuntimeManager(2);
        // Deliberately NOT createTempDir(): createGlobalRuntime renames the spill dir's entries to
        // "<name>.stale" and deletes them on a detached background thread that closeGlobalRuntime does
        // not join. Lucene's TestRuleTemporaryFilesCleanup does a strict recursive rm of every temp
        // dir at suite end; if the spill dir were Lucene-tracked, that rm races the native deleter and
        // fails with NoSuchFileException on a "*.stale" file. Using an untracked OS temp dir makes the
        // native thread the sole owner; we reap it best-effort in tearDown.
        // Root at java.io.tmpdir (an explicit location, as forbidden-apis requires) rather than the
        // no-location Files.createTempDirectory(String) overload — and NOT Lucene's createTempDir.
        Path spillDir;
        try {
            spillDir = Files.createTempDirectory(PathUtils.get(System.getProperty("java.io.tmpdir")), "datafusion-spill");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        spillDirs.add(spillDir);
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        assertTrue("runtime ptr non-zero", runtimePtr != 0);
        return new NativeRuntimeHandle(runtimePtr);
    }

    @Override
    public void tearDown() throws Exception {
        // Best-effort, race-tolerant delete. The runtime is already closed by each test, so the native
        // cleanup thread has been triggered but may still be unlinking "*.stale" entries concurrently.
        // We must NOT use the strict IOUtils.rm here (it throws NoSuchFileException on a file the native
        // thread deleted first — the very bug this test hit). Swallow per-entry failures; the OS reaps
        // the temp root either way.
        for (Path dir : spillDirs) {
            deleteBestEffort(dir);
        }
        spillDirs.clear();
        super.tearDown();
    }

    private static void deleteBestEffort(Path root) {
        if (Files.notExists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // Concurrent native cleanup may have removed it, or a child reappeared mid-walk.
                }
            });
        } catch (IOException ignored) {
            // Directory vanished under us (native thread finished the wipe) — nothing left to do.
        }
    }

    /**
     * Bare {@code SELECT * FROM "input-0"} substrait whose lowered output schema is a single
     * BIGINT column named {@code x} — used as the producer-side plan that
     * {@code registerPartitionStream} / {@code registerMemtable} now derive their input
     * schema from.
     */
    private static byte[] passthroughSubstrait(String inputId) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("x", bigintNullable).build();

        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), inputId, rowType);

        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(NativeBridgeLocalSessionTests.class.getClassLoader());
            SimpleExtension.ExtensionCollection ext = DefaultExtensionCatalog.DEFAULT_COLLECTION;
            return new DataFusionFragmentConvertor(ext).convertFragment(scan);
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    public void testCreateLocalSessionReturnsNonZeroPtr() {
        NativeRuntimeHandle runtimeHandle = createRuntime();
        try {
            DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
            assertTrue("session ptr non-zero", session.getPointer() != 0);
            session.close();
        } finally {
            runtimeHandle.close();
        }
    }

    public void testCloseLocalSessionToleratesZero() {
        // Must not throw.
        NativeBridge.closeLocalSession(0L);
    }

    public void testSenderCloseToleratesZero() {
        NativeBridge.senderClose(0L);
    }

    public void testRegisterPartitionStreamAndSenderClose() {
        NativeRuntimeHandle runtimeHandle = createRuntime();
        try {
            DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
            try {
                NativeBridge.RegisteredInput registered = NativeBridge.registerPartitionStream(
                    session.getPointer(),
                    "input-0",
                    passthroughSubstrait("input-0")
                );
                assertTrue("sender ptr non-zero", registered.pointer() != 0);
                assertNotNull("schema IPC bytes returned", registered.schemaIpc());
                assertTrue("schema IPC non-empty", registered.schemaIpc().length > 0);
                NativeBridge.senderClose(registered.pointer());
            } finally {
                session.close();
            }
        } finally {
            runtimeHandle.close();
        }
    }

    public void testRegisterMemtableAcceptsZeroBatches() {
        NativeRuntimeHandle runtimeHandle = createRuntime();
        try {
            DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
            try {
                NativeBridge.RegisteredInput registered = NativeBridge.registerMemtable(
                    session.getPointer(),
                    "input-0",
                    passthroughSubstrait("input-0"),
                    new long[0],
                    new long[0]
                );
                assertNotNull("schema IPC bytes returned", registered.schemaIpc());
                assertTrue("schema IPC non-empty", registered.schemaIpc().length > 0);
            } finally {
                session.close();
            }
        } finally {
            runtimeHandle.close();
        }
    }

    public void testRegisterMemtableImportsBatch() {
        NativeRuntimeHandle runtimeHandle = createRuntime();
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
            try {
                Schema schema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
                VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, alloc);
                vsr.allocateNew();
                BigIntVector col = (BigIntVector) vsr.getVector(0);
                col.setSafe(0, 1L);
                col.setSafe(1, 2L);
                col.setValueCount(2);
                vsr.setRowCount(2);
                try (ArrowArray array = ArrowArray.allocateNew(alloc); ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc)) {
                    Data.exportVectorSchemaRoot(alloc, vsr, null, array, arrowSchema);
                    NativeBridge.registerMemtable(
                        session.getPointer(),
                        "input-0",
                        passthroughSubstrait("input-0"),
                        new long[] { array.memoryAddress() },
                        new long[] { arrowSchema.memoryAddress() }
                    );
                } finally {
                    vsr.close();
                }
            } finally {
                session.close();
            }
        } finally {
            runtimeHandle.close();
        }
    }

    public void testRegisterMemtableRejectsLengthMismatch() {
        NativeRuntimeHandle runtimeHandle = createRuntime();
        try {
            DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
            try {
                expectThrows(
                    IllegalArgumentException.class,
                    () -> NativeBridge.registerMemtable(
                        session.getPointer(),
                        "input-0",
                        passthroughSubstrait("input-0"),
                        new long[] { 1L, 2L },
                        new long[] { 1L }
                    )
                );
            } finally {
                session.close();
            }
        } finally {
            runtimeHandle.close();
        }
    }
}

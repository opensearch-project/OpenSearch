/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

/**
 * Unit tests for {@link DatafusionReduceSink}.
 *
 * <p>The sink is exercised at two levels:
 * <ul>
 *   <li>Lightweight assertions that don't touch the native library (encoding helper,
 *       fixed input-id constant).</li>
 *   <li>A real end-to-end feed/drain round trip against a live native runtime:
 *       build Substrait bytes via {@link DataFusionFragmentConvertor}, construct the
 *       sink, feed Arrow batches, close, and assert the downstream sink received the
 *       reduced result.</li>
 * </ul>
 */
public class DatafusionReduceSinkTests extends OpenSearchTestCase {

    public void testArrowSchemaIpcEncodesSchema() {
        Schema schema = new Schema(List.of(new Field("message", FieldType.notNullable(new ArrowType.Int(64, true)), null)));
        byte[] ipc = ArrowSchemaIpc.toBytes(schema);
        assertNotNull("ipc bytes should be non-null", ipc);
        assertTrue("ipc bytes should be non-empty", ipc.length > 0);
    }

    public void testInputIdConstantMatchesDesign() {
        assertEquals("Single-input reduce uses the synthetic id 'input-0'", "input-0", DatafusionReduceSink.INPUT_ID);
    }

    /**
     * End-to-end feed + drain: feeds three Arrow batches (values 1..9) into a real
     * {@link DatafusionReduceSink} running a {@code SELECT SUM(x) FROM "input-0"}
     * Substrait plan, then asserts the downstream sink received a single-row batch
     * containing 45.
     *
     * <p>Mirrors the Rust integration test {@code test_execute_sum_substrait}; the
     * Java side proves the FFI ownership + drain wiring works against the same plan.
     */
    public void testFeedDrainsSumToDownstream() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        assertTrue("runtime ptr non-zero", runtimePtr != 0);
        // Wrap in NativeRuntimeHandle so the pointer is registered in the
        // NativeHandle live-set that validatePointer consults.
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);
        ExecutorService drainExec = java.util.concurrent.Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            byte[] substrait = buildSumSubstraitBytes(DatafusionReduceSink.INPUT_ID);

            CapturingSink downstream = new CapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-1",
                0,
                substrait,
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, inputSchema)),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle, drainExec);
            try {
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 1L, 2L, 3L }));
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 4L, 5L, 6L }));
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 7L, 8L, 9L }));
            } finally {
                sink.close();
            }

            // Downstream is NOT closed by the reduce sink — its lifecycle is owned by
            // the walker/orchestrator, which reads results after the sink drains.
            assertFalse("downstream must NOT be closed by the reduce sink", downstream.closed);
            assertTrue("downstream should receive at least one row, got " + downstream.totalRows, downstream.totalRows >= 1);
            assertEquals("SUM(1..9) should be 45", 45L, downstream.total);
        } finally {
            drainExec.shutdownNow();
            runtimeHandle.close();
        }
    }

    /**
     * Verifies that the drain task — submitted to the executor at sink construction —
     * runs concurrently with feeds, so producers complete every batch even when the
     * total volume exceeds the bounded native input mpsc's capacity.
     */
    public void testDrainTaskKeepsUpWithProducer() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);
        ExecutorService drainExec = java.util.concurrent.Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            byte[] substrait = buildSumSubstraitBytes(DatafusionReduceSink.INPUT_ID);

            CapturingSink downstream = new CapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-drain",
                0,
                substrait,
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, inputSchema)),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle, drainExec);
            final int totalBatches = 12; // intentionally > native input mpsc capacity
            try {
                for (int i = 0; i < totalBatches; i++) {
                    sink.feed(makeBatch(alloc, inputSchema, new long[] { (long) i }));
                }
            } finally {
                sink.close();
            }

            assertEquals("all " + totalBatches + " feeds should have completed", totalBatches, sink.feedCount());
            assertTrue("downstream should receive at least one row, got " + downstream.totalRows, downstream.totalRows >= 1);
            assertEquals("SUM(0..11) should be 66", 66L, downstream.total);
        } finally {
            drainExec.shutdownNow();
            runtimeHandle.close();
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Builds Substrait bytes for {@code SELECT SUM(x) FROM "input-0"} using the
     * production {@link DataFusionFragmentConvertor} path — the same conversion
     * {@code FragmentConversionDriver} invokes for a coordinator-reduce stage at
     * runtime.
     */
    private static byte[] buildSumSubstraitBytes(String inputId) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("x", bigintNullable).build();

        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), inputId, rowType);

        AggregateCall sumCall = AggregateCall.create(SqlStdOperatorTable.SUM, false, List.of(0), -1, bigintNullable, "total");
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));

        return new DataFusionFragmentConvertor(loadExtensions()).convertFinalAggFragment(agg);
    }

    /**
     * Loads the Substrait extension catalog with the test classloader as TCCL —
     * mirrors the swap performed by {@code DataFusionPlugin#loadSubstraitExtensions}
     * so Jackson polymorphic deserialization can resolve plugin-local Substrait classes.
     */
    private static SimpleExtension.ExtensionCollection loadExtensions() {
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DatafusionReduceSinkTests.class.getClassLoader());
            return DefaultExtensionCatalog.DEFAULT_COLLECTION;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    private static VectorSchemaRoot makeBatch(BufferAllocator alloc, Schema schema, long[] values) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        root.allocateNew();
        BigIntVector col = (BigIntVector) root.getVector(0);
        for (int i = 0; i < values.length; i++) {
            col.setSafe(i, values[i]);
        }
        col.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    /**
     * Reads each fed batch's single BIGINT column into {@link #total} + closes the batch.
     * Values are extracted synchronously during {@code feed} so the test can assert on
     * {@link #total} after {@code close()} has released all Arrow buffers.
     */
    private static final class CapturingSink implements ExchangeSink {
        long total;
        int totalRows;
        boolean closed;

        @Override
        public synchronized void feed(VectorSchemaRoot batch) {
            try {
                BigIntVector col = (BigIntVector) batch.getVector(0);
                int rows = batch.getRowCount();
                totalRows += rows;
                // DataFusion may omit the validity buffer when there are no nulls; read raw.
                for (int i = 0; i < rows; i++) {
                    total += col.getDataBuffer().getLong((long) i * BigIntVector.TYPE_WIDTH);
                }
            } finally {
                batch.close();
            }
        }

        @Override
        public synchronized void close() {
            closed = true;
        }
    }
}

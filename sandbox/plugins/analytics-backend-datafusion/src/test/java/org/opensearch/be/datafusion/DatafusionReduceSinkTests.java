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
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
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

    // ── rescaleTimestamp ───────────────────────────────────────────────────────────────

    public void testRescaleTimestampIdentityWhenUnitsMatch() {
        assertEquals(1_234L, DatafusionReduceSink.rescaleTimestamp(1_234L, TimeUnit.MILLISECOND, TimeUnit.MILLISECOND));
        assertEquals(0L, DatafusionReduceSink.rescaleTimestamp(0L, TimeUnit.NANOSECOND, TimeUnit.NANOSECOND));
        assertEquals(-42L, DatafusionReduceSink.rescaleTimestamp(-42L, TimeUnit.SECOND, TimeUnit.SECOND));
    }

    public void testRescaleTimestampUpscale() {
        // SECOND → MILLI: ×1_000
        assertEquals(5_000L, DatafusionReduceSink.rescaleTimestamp(5L, TimeUnit.SECOND, TimeUnit.MILLISECOND));
        // MILLI → MICRO: ×1_000
        assertEquals(2_000L, DatafusionReduceSink.rescaleTimestamp(2L, TimeUnit.MILLISECOND, TimeUnit.MICROSECOND));
        // MILLI → NANO: ×1_000_000
        assertEquals(7_000_000L, DatafusionReduceSink.rescaleTimestamp(7L, TimeUnit.MILLISECOND, TimeUnit.NANOSECOND));
        // SECOND → NANO: ×1_000_000_000
        assertEquals(3_000_000_000L, DatafusionReduceSink.rescaleTimestamp(3L, TimeUnit.SECOND, TimeUnit.NANOSECOND));
        // Negative values preserved
        assertEquals(-1_500L, DatafusionReduceSink.rescaleTimestamp(-1500L, TimeUnit.MILLISECOND, TimeUnit.MILLISECOND));
        assertEquals(-2_000L, DatafusionReduceSink.rescaleTimestamp(-2L, TimeUnit.MILLISECOND, TimeUnit.MICROSECOND));
    }

    public void testRescaleTimestampDownscaleTruncatesTowardNegativeInfinity() {
        // NANO → MILLI: ÷1_000_000
        assertEquals(1L, DatafusionReduceSink.rescaleTimestamp(1_000_000L, TimeUnit.NANOSECOND, TimeUnit.MILLISECOND));
        assertEquals(1L, DatafusionReduceSink.rescaleTimestamp(1_999_999L, TimeUnit.NANOSECOND, TimeUnit.MILLISECOND));
        // Math.floorDiv: -1 (not 0) for -1ns truncated to millis. Matches Arrow/DataFusion semantics.
        assertEquals(-1L, DatafusionReduceSink.rescaleTimestamp(-1L, TimeUnit.NANOSECOND, TimeUnit.MILLISECOND));
        assertEquals(-1L, DatafusionReduceSink.rescaleTimestamp(-999_999L, TimeUnit.NANOSECOND, TimeUnit.MILLISECOND));
        // MICRO → SECOND
        assertEquals(2L, DatafusionReduceSink.rescaleTimestamp(2_500_000L, TimeUnit.MICROSECOND, TimeUnit.SECOND));
    }

    public void testRescaleTimestampOverflowThrows() {
        // 2^63-1 in MILLI cannot be upscaled to MICRO without overflow.
        expectThrows(
            ArithmeticException.class,
            () -> DatafusionReduceSink.rescaleTimestamp(Long.MAX_VALUE / 100L, TimeUnit.MILLISECOND, TimeUnit.MICROSECOND)
        );
    }

    // ── describeType ───────────────────────────────────────────────────────────────────

    public void testDescribeTypeRendersTimestampUnitAndTimezone() {
        assertEquals("Timestamp(MILLISECOND,UTC)", DatafusionReduceSink.describeType(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")));
        assertEquals("Timestamp(MICROSECOND,null)", DatafusionReduceSink.describeType(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    }

    public void testDescribeTypeFallsBackToTypeIdForOtherTypes() {
        assertEquals("Int", DatafusionReduceSink.describeType(new ArrowType.Int(64, true)));
        assertEquals("Utf8", DatafusionReduceSink.describeType(new ArrowType.Utf8()));
    }

    // ── coerceToDeclaredSchema: Timestamp → Timestamp ─────────────────────────────────

    public void testCoerceTimestampMilliUtcToMicroUtc() {
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema srcSchema = singleField("ts", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
            Schema dstSchema = singleField("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
            VectorSchemaRoot srcRoot = VectorSchemaRoot.create(srcSchema, alloc);
            srcRoot.allocateNew();
            TimeStampMilliTZVector srcVec = (TimeStampMilliTZVector) srcRoot.getVector(0);
            srcVec.setSafe(0, 1L);
            srcVec.setNull(1);
            srcVec.setSafe(2, 1_700_000_000_000L);
            srcRoot.setRowCount(3);

            // invokeCoerce takes ownership of srcRoot and closes it before returning.
            try (VectorSchemaRoot out = invokeCoerce(srcRoot, dstSchema, alloc)) {
                TimeStampMicroTZVector outVec = (TimeStampMicroTZVector) out.getVector(0);
                assertEquals(3, out.getRowCount());
                assertEquals(1_000L, outVec.get(0));
                assertTrue("row 1 stays null", outVec.isNull(1));
                assertEquals(1_700_000_000_000_000L, outVec.get(2));
            }
        }
    }

    public void testCoerceTimestampSecondToMillisecond() {
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema srcSchema = singleField("ts", new ArrowType.Timestamp(TimeUnit.SECOND, null));
            Schema dstSchema = singleField("ts", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
            VectorSchemaRoot srcRoot = VectorSchemaRoot.create(srcSchema, alloc);
            srcRoot.allocateNew();
            TimeStampSecVector srcVec = (TimeStampSecVector) srcRoot.getVector(0);
            srcVec.setSafe(0, 1L);
            srcVec.setSafe(1, 0L);
            srcRoot.setRowCount(2);

            try (VectorSchemaRoot out = invokeCoerce(srcRoot, dstSchema, alloc)) {
                TimeStampMilliTZVector outVec = (TimeStampMilliTZVector) out.getVector(0);
                assertEquals(1_000L, outVec.get(0));
                assertEquals(0L, outVec.get(1));
            }
        }
    }

    public void testCoerceTimestampNanoToMilliTruncates() {
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema srcSchema = singleField("ts", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));
            Schema dstSchema = singleField("ts", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
            VectorSchemaRoot srcRoot = VectorSchemaRoot.create(srcSchema, alloc);
            srcRoot.allocateNew();
            TimeStampNanoVector srcVec = (TimeStampNanoVector) srcRoot.getVector(0);
            srcVec.setSafe(0, 1_999_999L);
            srcVec.setSafe(1, 2_500_000L);
            srcRoot.setRowCount(2);

            try (VectorSchemaRoot out = invokeCoerce(srcRoot, dstSchema, alloc)) {
                TimeStampMilliTZVector outVec = (TimeStampMilliTZVector) out.getVector(0);
                // 1_999_999 ns = 1.999999 ms → truncates toward 0 → 1
                assertEquals(1L, outVec.get(0));
                // 2_500_000 ns = 2.5 ms → truncates toward 0 → 2
                assertEquals(2L, outVec.get(1));
            }
        }
    }

    public void testCoerceTimestampNoTimezoneToTimezoneTreatedAsValuePreserving() {
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema srcSchema = singleField("ts", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));
            Schema dstSchema = singleField("ts", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
            VectorSchemaRoot srcRoot = VectorSchemaRoot.create(srcSchema, alloc);
            srcRoot.allocateNew();
            TimeStampMilliVector srcVec = (TimeStampMilliVector) srcRoot.getVector(0);
            srcVec.setSafe(0, 1_700_000_000_000L);
            srcRoot.setRowCount(1);

            try (VectorSchemaRoot out = invokeCoerce(srcRoot, dstSchema, alloc)) {
                TimeStampMilliTZVector outVec = (TimeStampMilliTZVector) out.getVector(0);
                assertEquals(1_700_000_000_000L, outVec.get(0));
            }
        }
    }

    public void testCoerceErrorMessageRendersTimestampPrecisionAndTimezone() {
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // Force an unsupported (Timestamp → Int) pair to confirm describeType renders the
            // full Timestamp(unit,tz) shape rather than the indistinguishable bare type ID.
            Schema srcSchema = singleField("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
            Schema dstSchema = singleField("ts", new ArrowType.Int(64, true));
            VectorSchemaRoot srcRoot = VectorSchemaRoot.create(srcSchema, alloc);
            srcRoot.allocateNew();
            srcRoot.setRowCount(0);
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> invokeCoerce(srcRoot, dstSchema, alloc));
            assertTrue(
                "error message should include Timestamp(unit,tz): " + e.getMessage(),
                e.getMessage().contains("Timestamp(MICROSECOND,UTC)")
            );
        }
    }

    private static Schema singleField(String name, ArrowType type) {
        return new Schema(List.of(new Field(name, FieldType.nullable(type), null)));
    }

    private static VectorSchemaRoot invokeCoerce(VectorSchemaRoot batch, Schema declared, BufferAllocator alloc) {
        return DatafusionReduceSink.coerceToDeclaredSchema(batch, declared, alloc);
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

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
            try {
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 1L, 2L, 3L }));
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 4L, 5L, 6L }));
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 7L, 8L, 9L }));
            } finally {
                sink.close();
            }

            // Downstream is NOT closed by the reduce sink — its lifecycle is owned by
            // the walker/orchestrator, which reads buffered batches after the sink drains.
            assertFalse("downstream must NOT be closed by the reduce sink", downstream.closed);
            assertTrue("downstream should receive at least one row, got " + downstream.totalRows, downstream.totalRows >= 1);
            assertEquals("SUM(1..9) should be 45", 45L, downstream.total);
        } finally {
            runtimeHandle.close();
        }
    }

    /**
     * Demonstrates that producers wedge past the input mpsc capacity (4) when no
     * consumer is draining — and proves that no consumer IS draining during the
     * feed phase, because the CPU executor's spawned task only fires on the first
     * poll of the output stream, which only happens inside {@code close()} via
     * {@code drainOutputIntoDownstream → streamNext}.
     *
     * <p>Expected log signature when this test runs:
     * <pre>
     *   [partition_stream] send_blocking enter — channel capacity remaining: 4
     *   [partition_stream] send_blocking returned ok=true
     *   [partition_stream] send_blocking enter — channel capacity remaining: 3
     *   [partition_stream] send_blocking returned ok=true
     *   ... 4 successful sends ...
     *   [partition_stream] send_blocking enter — channel capacity remaining: 0
     *   (no return — parked)
     *   (no [cross_rt_stream] driver polled message before close — proves CPU never started)
     *   ...test asserts producer parked at 4 feeds...
     *   ...test calls close()...
     *   [cross_rt_stream] driver polled for first time — submitting CPU spawn
     *   [cross_rt_stream] CPU task started — beginning to pull from input stream
     * </pre>
     *
     * <p>The logs prove: producers are blocked, CPU executor hasn't spawned yet,
     * and the spawn only fires when close() drains. Run with
     * {@code -Dtests.logger.level=DEBUG} to see partition_stream logs.
     */
    public void testProducersDoNotWedgePastCapacity() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            byte[] substrait = buildSumSubstraitBytes(DatafusionReduceSink.INPUT_ID);

            CapturingSink downstream = new CapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-wedge",
                0,
                substrait,
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, inputSchema)),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);

            final int totalBatches = 12;     // intentionally > capacity (4)
            java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
            Thread producer = new Thread(() -> {
                for (int i = 0; i < totalBatches; i++) {
                    attempts.incrementAndGet();
                    sink.feed(makeBatch(alloc, inputSchema, new long[] { (long) i }));
                }
            }, "test-producer-wedge");
            producer.setDaemon(true);
            producer.start();

            // Give the producer plenty of wall-clock time to push every batch if it weren't blocked.
            // 4 should land in the mpsc immediately; the 5th will park indefinitely.
            Thread.sleep(1500);

            long completed = sink.feedCount();
            int attempted = attempts.get();
            Thread.State state = producer.getState();
            logger.info("After 1500ms wait: completed={}, attempted={}, producerState={}", completed, attempted, state);

            // Channel capacity is 1 (intentionally reduced for diagnostic clarity). If no
            // consumer is draining concurrently with feeds, we'd expect:
            // completed = 1 (first push lands), attempted = 2 (second push parked),
            // state = WAITING/TIMED_WAITING.
            // If a consumer IS draining concurrently (e.g. RepartitionExec spawned a
            // task during DataFusion plan setup), we'd expect:
            // completed = totalBatches, state = TERMINATED.
            // The actual outcome tells us which mental model is correct.
            // After Part 1 (drain thread) is in place, the drain thread polls the output
            // stream which cascades down to our partition stream's receiver — so even
            // without RepartitionExec (target_partitions=1), there's a concurrent consumer.
            // EXPECTATION: completed == totalBatches, producer terminated.
            //
            // Without the drain thread (and without RepartitionExec), we'd see:
            // completed == 1, attempted == 2, state in {RUNNABLE (FFI-blocked), WAITING}.
            // Note: a Java thread blocked inside an FFI call shows up as RUNNABLE in
            // Thread.getState() because the JVM doesn't see Rust-level parking — the
            // thread is "running native code" from the JVM's perspective.
            assertEquals(
                "with the drain thread, all " + totalBatches + " feeds should complete; got " + completed,
                totalBatches,
                completed
            );
            assertEquals("producer thread should be TERMINATED after completing all feeds; got " + state, Thread.State.TERMINATED, state);
            assertEquals("attempted should equal completed", completed, attempted);

            // Cleanup: close() drops the sender, which fails the parked tx.send futures with
            // "receiver dropped". The producer thread errors out of senderSend; the lock-free
            // feed catches the runtime exception when closed=true. close() then drains the
            // (now empty) output stream and tears down. Producer thread becomes joinable.
            sink.close();
            producer.join(5_000);
            assertFalse("producer thread should have exited after sink.close()", producer.isAlive());

            // Final accounting: feedCount reflects only the feeds that actually deposited
            // before the parked one was unblocked-by-error. Anywhere from 4..5 inclusive.
            logger.info("After close: feedCount={}, downstream rows={}", sink.feedCount(), downstream.totalRows);
        } finally {
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

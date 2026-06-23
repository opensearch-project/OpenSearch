/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
 *
 * <p>{@link ThreadLeakScope.Scope#NONE}: the async-completion path fires upcalls on Tokio IO-runtime
 * worker threads (process-lifetime singletons that attach to the JVM and persist after the test).
 * They cannot be shut down per-test without breaking other classes sharing the JVM — same rationale
 * as {@code DataFusionNativeBridgeTests}.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
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
     * The benign "consumer finished first" case (a {@code LimitExec} satisfied its fetch and
     * dropped the receiver while producers were still feeding) is signalled by
     * {@code df_sender_send_async} returning the positive sentinel
     * {@link NativeBridge#SENDER_SEND_RECEIVER_DROPPED} rather than a stringly-typed error —
     * {@code feedToSender} keys off this code. The sentinel must be positive (so {@code checkResult}
     * passes it through the success half of the return contract instead of treating it as an error
     * pointer) and distinct from a normal send ({@code 0}). The Rust↔Java agreement on the value is
     * enforced by the matching {@code SENDER_SEND_RECEIVER_DROPPED} in {@code ffm.rs}; the structural
     * "only a dropped receiver maps here" guarantee is covered by the Rust
     * {@code try_send_reports_receiver_dropped} unit test.
     */
    public void testReceiverDroppedSentinelIsPositiveAndDistinctFromSuccess() {
        assertTrue(
            "sentinel must be positive so checkResult treats it as success, not an error pointer",
            NativeBridge.SENDER_SEND_RECEIVER_DROPPED > 0
        );
        assertNotEquals("sentinel must be distinct from a normal send (0)", 0L, NativeBridge.SENDER_SEND_RECEIVER_DROPPED);
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
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            byte[] substrait = buildSumSubstraitBytes(DatafusionReduceSink.INPUT_ID);

            CapturingSink downstream = new CapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-1",
                0,
                0L,
                substrait,
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID))),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
            // Mimic ReduceExecution body: spawn reduce on a VT so drain
            // runs concurrently with feeds (the drain asserts it's on a virtual thread).
            PlainActionFuture<Void> drainDone = PlainActionFuture.newFuture();
            Thread.ofVirtual().start(() -> sink.reduce(drainDone));
            try {
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 1L, 2L, 3L }));
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 4L, 5L, 6L }));
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 7L, 8L, 9L }));
                // Signal input EOF (single-input → close the only per-child wrapper).
                sink.sinkForChild(0).close();
                drainDone.actionGet(10, TimeUnit.SECONDS);
            } finally {
                sink.close();
            }

            assertFalse("downstream must NOT be closed by the reduce sink", downstream.closed);
            assertTrue("downstream should receive at least one row, got " + downstream.totalRows, downstream.totalRows >= 1);
            assertEquals("SUM(1..9) should be 45", 45L, downstream.total);
        } finally {
            runtimeHandle.close();
        }
    }

    /**
     * Coordinator reduce running {@code SELECT x FROM "input-0" LIMIT 3} produces exactly the
     * limited output and tears down cleanly. Feeds far more rows than the limit through a real
     * native reduce; the {@code LimitExec} emits 3 rows and the drain ends.
     *
     * <p>NOTE: this does NOT yet assert reduce-input <em>early termination</em> (the sink reporting
     * {@code isConsumerDone()} so feeders stop early). With the reduce session's
     * {@code target_partitions > 1}, the physical plan inserts a {@code RepartitionExec} below the
     * limit that eagerly drains the whole input rather than letting the {@code LimitExec} drop the
     * receiver after N rows — so the receiver-drop signal does not surface for this shape. The
     * shard-side reaction to {@code isConsumerDone()} is covered by
     * {@code ShardFragmentStageExecutionTests.testStreamStopsAndTaskSucceedsWhenConsumerDone}, and
     * the receiver-drop sentinel contract by {@code testReceiverDroppedSentinelIsPositiveAndDistinctFromSuccess}.
     * Asserting the end-to-end early-stop here is blocked on the planner sort/limit work; this test
     * pins correctness of the LIMIT-over-reduce path in the meantime.
     */
    public void testReduceWithLimitProducesLimitedOutput() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            final int limit = 3;
            CapturingSink downstream = new CapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-limit-reduce",
                0,
                7777L,
                buildLimitSubstraitBytes(DatafusionReduceSink.INPUT_ID, limit),
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID))),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
            PlainActionFuture<Void> drainDone = PlainActionFuture.newFuture();
            Thread.ofVirtual().start(() -> sink.reduce(drainDone));

            try {
                for (int i = 0; i < 20; i++) {
                    sink.feed(makeBatch(alloc, inputSchema, new long[] { (long) i }));
                }
                sink.sinkForChild(0).close(); // input EOF
                drainDone.actionGet(10, TimeUnit.SECONDS);
            } finally {
                sink.close();
            }

            assertEquals("LIMIT " + limit + " must yield exactly " + limit + " rows downstream", limit, downstream.totalRows);
        } finally {
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

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            byte[] substrait = buildSumSubstraitBytes(DatafusionReduceSink.INPUT_ID);

            CapturingSink downstream = new CapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-drain",
                0,
                0L,
                substrait,
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID))),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
            PlainActionFuture<Void> drainDone = PlainActionFuture.newFuture();
            Thread.ofVirtual().start(() -> sink.reduce(drainDone));
            final int totalBatches = 12; // intentionally > native input mpsc capacity
            try {
                for (int i = 0; i < totalBatches; i++) {
                    sink.feed(makeBatch(alloc, inputSchema, new long[] { (long) i }));
                }
                sink.sinkForChild(0).close();
                drainDone.actionGet(10, TimeUnit.SECONDS);
            } finally {
                sink.close();
            }

            assertEquals("all " + totalBatches + " feeds should have completed", totalBatches, sink.feedCount());
            assertTrue("downstream should receive at least one row, got " + downstream.totalRows, downstream.totalRows >= 1);
            assertEquals("SUM(0..11) should be 66", 66L, downstream.total);
        } finally {
            runtimeHandle.close();
        }
    }

    /**
     * Pipelining regression net: with a passthrough reduce plan (1-in → 1-out, no
     * blocking aggregate), the first output batch must reach downstream BEFORE the
     * sink is closed. {@code close()} signals input EOF — if drain only produced
     * output at EOF, this assertion would time out, proving the reduce wasn't
     * incremental.
     *
     * <p>Stronger than {@link #testDrainTaskKeepsUpWithProducer} (which only proves
     * drain consumes input concurrently — SUM is a blocking aggregate that buffers
     * all rows before emitting). This test proves drain *produces* output concurrently.
     */
    public void testReduceProducesOutputIncrementallyForPipelinedPlan() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            // Reduce-side AND child-side both passthrough — each input batch pipelines
            // straight through to a downstream feed, no aggregation buffering.
            byte[] reduceSubstrait = buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID);
            byte[] childSubstrait = buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID);

            LatchingCapturingSink downstream = new LatchingCapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-pipelined",
                0,
                0L,
                reduceSubstrait,
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, childSubstrait)),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
            PlainActionFuture<Void> drainDone = PlainActionFuture.newFuture();
            Thread.ofVirtual().start(() -> sink.reduce(drainDone));
            // > native input mpsc channel capacity (4). If drain isn't consuming
            // concurrently the channel saturates and feed #5 deadlocks in send_blocking.
            final int totalBatches = 8;
            ExecutorService feedExec = Executors.newSingleThreadExecutor();
            try {
                Future<?> feedAll = feedExec.submit(() -> {
                    for (int i = 0; i < totalBatches; i++) {
                        sink.feed(makeBatch(alloc, inputSchema, new long[] { (long) i }));
                    }
                });
                try {
                    feedAll.get(10, TimeUnit.SECONDS);
                } catch (TimeoutException te) {
                    fail("feeds deadlocked — drain is not consuming the input mpsc concurrently");
                }
                // Pipelining: first output batch reaches downstream BEFORE we signal EOF.
                assertTrue(
                    "first output batch did not reach downstream before EOF signal — drain not pipelined",
                    downstream.firstBatchLatch.await(5, TimeUnit.SECONDS)
                );
                sink.sinkForChild(0).close();
                drainDone.actionGet(10, TimeUnit.SECONDS);
            } finally {
                sink.close();
                feedExec.shutdownNow();
            }

            assertEquals("all " + totalBatches + " batches must reach downstream", totalBatches, downstream.batchCount);
            assertEquals("each input batch had one row", totalBatches, downstream.totalRows);
        } finally {
            runtimeHandle.close();
        }
    }

    /**
     * Regression: {@code close()} while a feeder is parked on a full input channel must NOT
     * deadlock.
     *
     * <p>Old bug: {@code closeImpl} closed {@code session} first, which needed the write lock on
     * the partition sender's {@code NativeHandle}. But a feeder parked inside {@code sender.send}
     * (driving a full-channel {@code tx.send().await} via {@code block_on}) held the read lock —
     * so the write lock blocked forever. Fix: {@code closeImpl} closes {@code outStream} FIRST,
     * which drops the native receiver; the parked {@code tx.send} then returns immediately
     * ({@code ReceiverDropped}), the feeder releases the read lock, and {@code close()} proceeds.
     *
     * <p>Setup mirrors that race: a blocking downstream sink stalls the reduce drain so the bounded
     * native input channel (capacity 4) fills and a dedicated feeder thread parks; then we call
     * {@code close()} and assert it returns well within a timeout.
     */
    public void testCloseWhileFeederParkedOnFullChannelDoesNotDeadlock() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            BlockingCapturingSink downstream = new BlockingCapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-close-deadlock",
                0,
                9999L,
                buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID),
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID))),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
            PlainActionFuture<Void> reduceDone = PlainActionFuture.newFuture();
            Thread.ofVirtual().start(() -> sink.reduce(reduceDone));

            // Feeder thread: flood the input. The drain is stalled in downstream.feed (blocked on
            // `release`), so once a batch reaches the output the bounded input channel backs up and
            // this thread parks inside sink.feed -> sender.send (holding the sender read lock).
            // Once close() tears the sink down, further feeds throw (closed sink / closed allocator)
            // — that's expected; the feeder swallows it and exits.
            Thread feeder = new Thread(() -> {
                try {
                    for (int i = 0; i < 100; i++) {
                        sink.feed(makeBatch(alloc, inputSchema, new long[] { (long) i }));
                    }
                } catch (RuntimeException expectedOnceClosed) {
                    // sink/allocator closed underneath us — stop feeding.
                }
            }, "deadlock-feeder");
            feeder.setDaemon(true);
            feeder.start();

            // Wait until the drain has produced its first output batch (drain now stalled) and give
            // the feeder time to fill the channel and park.
            assertTrue("drain should produce a first batch", downstream.firstBatchLatch.await(10, TimeUnit.SECONDS));
            Thread.sleep(300);

            // close() from this thread must not deadlock against the parked feeder. This is the
            // crux of the regression: with the old teardown ordering close() would block forever
            // on the sender write lock held by the parked feeder.
            Thread closer = new Thread(sink::close, "deadlock-closer");
            closer.setDaemon(true);
            closer.start();
            closer.join(15_000);
            assertFalse("close() deadlocked against a feeder parked on the full channel", closer.isAlive());

            // reduce() is in flight (state=REDUCING), so close() fires cancel and DEFERS teardown to
            // reduce()'s finally — torndown is set only after the drain unwinds. Release the drain,
            // wait for reduce to settle, then assert teardown ran exactly once.
            downstream.release.countDown();
            try {
                reduceDone.actionGet(10, TimeUnit.SECONDS);
            } catch (Exception expected) {
                // reduce may complete or fail depending on cancel timing — either is fine here.
            }
            feeder.join(10_000);
            assertFalse("feeder thread should have unparked and exited", feeder.isAlive());
            assertTrue("teardown must have run after reduce unwound", sink.torndown.get());
        } finally {
            runtimeHandle.close();
        }
    }

    /**
     * Cancel-before-first-batch: drain is parked in stream_next waiting for input.
     * {@code close()} fires {@code cancel_query} on the registered taskId — the cancellation
     * token wakes the {@code cancellable_or}'s select, drain returns sentinel, reduce()
     * unwinds cleanly. No leak.
     */
    public void testCancelBeforeFirstBatchUnwindsDrain() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            byte[] substrait = buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID);
            CapturingSink downstream = new CapturingSink();
            // Non-zero taskId so the Rust QUERY_REGISTRY actually wires cancellation.
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-cancel-pre",
                0,
                4242L,
                substrait,
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID))),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
            PlainActionFuture<Void> reduceDone = PlainActionFuture.newFuture();
            CountDownLatch reduceEntered = new CountDownLatch(1);
            Thread.ofVirtual().start(() -> {
                reduceEntered.countDown();
                sink.reduce(reduceDone);
            });
            // Ensure reduce() has progressed past its READY→REDUCING CAS before we close —
            // otherwise close runs inline (READY→DONE) and reduce later fails with
            // "sink closed before reduce" instead of the cancel-during-drain path we want.
            assertTrue(reduceEntered.await(5, TimeUnit.SECONDS));
            Thread.sleep(50);
            // Drain is parked waiting for first batch. Fire cancel via close().
            // close() sees state=REDUCING, calls cancelQuery(4242L), returns immediately.
            sink.close();
            reduceDone.actionGet(5, TimeUnit.SECONDS);

            assertEquals("no rows should be delivered (cancel before any feed)", 0, downstream.totalRows);
            // Regression: the cancel-during-REDUCING path used to leak outStream/session
            // because close() set the base's `closed` flag, then reduce()'s finally called
            // super.close() which short-circuited on that flag — closeImpl never ran a
            // second time and teardown never happened. reduce() now calls closeImpl directly.
            assertTrue("teardown must run on the cancel-during-REDUCING path", sink.torndown.get());
        } finally {
            runtimeHandle.close();
        }
    }

    /**
     * Cancel-after-first-batch: drain has consumed at least one row, then {@code close()}
     * fires {@code cancel_query}. The drain returns partial output and unwinds cleanly.
     */
    public void testCancelAfterFirstBatchUnwindsDrain() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            byte[] substrait = buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID);
            LatchingCapturingSink downstream = new LatchingCapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-cancel-post",
                0,
                4243L,
                substrait,
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID))),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
            PlainActionFuture<Void> reduceDone = PlainActionFuture.newFuture();
            Thread.ofVirtual().start(() -> sink.reduce(reduceDone));
            try {
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 99L }));
                // Wait until the drain produced the first batch to downstream — proves
                // we're past the empty-stream-park state and in the steady-pull state.
                assertTrue("first batch did not reach downstream within 5s", downstream.firstBatchLatch.await(5, TimeUnit.SECONDS));
                sink.close();  // cancel mid-stream
                reduceDone.actionGet(5, TimeUnit.SECONDS);
            } finally {
                sink.close();  // idempotent
            }

            assertTrue("downstream should have at least 1 row from before cancel", downstream.totalRows >= 1);
        } finally {
            runtimeHandle.close();
        }
    }

    /**
     * Double-close is idempotent: calling {@code close()} multiple times (including from
     * different threads, or after {@code reduce()} already tore down) must not throw or
     * double-free native resources. The {@code torndown} CAS ensures the teardown body
     * runs exactly once regardless of how many paths call {@code closeImpl}.
     */
    public void testDoubleCloseIsIdempotent() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            byte[] substrait = buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID);
            CapturingSink downstream = new CapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-double-close",
                0,
                7777L,
                substrait,
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, buildPassthroughSubstraitBytes(DatafusionReduceSink.INPUT_ID))),
                downstream
            );

            DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
            // First close tears down.
            sink.close();
            assertTrue("teardown must run on first close", sink.torndown.get());

            // Second close must be a no-op (no exception, no double-free).
            sink.close();
            assertTrue("torndown still true after second close", sink.torndown.get());

            // Close from REDUCING state: simulate reduce() already set state, then close
            // arrives concurrently. Teardown already ran so torndown CAS fails — no-op.
            sink.state.set(DatafusionReduceSink.SinkState.REDUCING);
            sink.close();
            assertTrue("third close (from REDUCING) is still idempotent", sink.torndown.get());
        } finally {
            runtimeHandle.close();
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Builds Substrait bytes for a plain {@code SELECT * FROM "input-0"} — used as
     * the producer-side plan in {@link ExchangeSinkContext.ChildInput#producerPlanBytes()}.
     * The lowered output schema is the bare leaf row type (single BIGINT column {@code x})
     * which is what the reduce sink registers as the input partition's declared schema.
     */
    private static byte[] buildPassthroughSubstraitBytes(String inputId) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("x", bigintNullable).build();

        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), inputId, rowType);

        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(scan);
    }

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

        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(agg);
    }

    /**
     * Builds Substrait bytes for {@code SELECT x FROM "input-0" LIMIT n} — a streaming
     * {@code Fetch} over the passthrough scan (the way {@code head n} lowers: a
     * {@code LogicalSort} with empty collation + {@code fetch=n}). Unlike an aggregate, a
     * {@code Fetch} is NOT a pipeline breaker: the native {@code LimitExec} stops pulling and
     * drops the input receiver as soon as it has emitted {@code n} rows — which is exactly the
     * reduce-input early-termination condition.
     */
    private static byte[] buildLimitSubstraitBytes(String inputId, int limit) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("x", bigintNullable).build();

        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), inputId, rowType);
        // Empty collation + fetch=limit → pure streaming LIMIT (no global sort), lowered to Fetch.
        RelNode fetch = org.apache.calcite.rel.logical.LogicalSort.create(
            scan,
            org.apache.calcite.rel.RelCollations.EMPTY,
            null,
            rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(limit))
        );

        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(fetch);
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
    /**
     * Downstream that fires {@link #firstBatchLatch} on the first {@code feed()} —
     * used by {@link #testReduceProducesOutputIncrementallyForPipelinedPlan} to
     * assert drain produces output mid-stream rather than only at close-time EOF.
     */
    private static final class LatchingCapturingSink implements ExchangeSink {
        final CountDownLatch firstBatchLatch = new CountDownLatch(1);
        volatile int batchCount;
        volatile int totalRows;

        @Override
        public synchronized void feed(VectorSchemaRoot batch) {
            try {
                batchCount++;
                totalRows += batch.getRowCount();
                firstBatchLatch.countDown();
            } finally {
                batch.close();
            }
        }

        @Override
        public void close() {}
    }

    /**
     * Downstream sink that blocks the drain thread inside {@code feed} until released. Used to
     * stall the reduce-output drain so the bounded native input channel fills and a feeder parks
     * on a full channel — the precondition for the close-vs-parked-feeder deadlock regression.
     */
    private static final class BlockingCapturingSink implements ExchangeSink {
        final CountDownLatch firstBatchLatch = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile int batchCount;

        @Override
        public void feed(VectorSchemaRoot batch) {
            try {
                batchCount++;
                firstBatchLatch.countDown();
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                batch.close();
            }
        }

        @Override
        public void close() {}
    }

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

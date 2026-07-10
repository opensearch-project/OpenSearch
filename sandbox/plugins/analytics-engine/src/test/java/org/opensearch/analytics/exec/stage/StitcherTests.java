/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Leak-focused tests for {@link Stitcher}. The stitcher pre-allocates its {@code output}
 * VectorSchemaRoot on the supplied allocator (in production, the coordinator allocator when
 * {@code analytics.coordinator.buffer_limit=0}) and holds it for the whole fetch-and-stitch.
 * These tests assert {@code allocator.getAllocatedMemory() == 0} after every terminal path —
 * success, feed-throws mid-emit, and the "finish never runs" cancel path — so a regression that
 * strands {@code output} on the coordinator allocator (the "query pool never goes down" symptom)
 * is caught.
 */
public class StitcherTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    /** Single-column output; the response batch adds the ___row_id helper the stitcher skips. */
    private List<Field> outputFields() {
        return List.of(new Field("val", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    }

    /** A fetch response batch: [___row_id, val], as the data node returns for LM fetch. */
    private VectorSchemaRoot responseBatch(Object[] rowIds, Object[] vals) {
        List<Field> fields = List.of(
            new Field(OpenSearchLateMaterialization.ROW_ID_FIELD, FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            new Field("val", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        );
        VectorSchemaRoot vsr = VectorSchemaRoot.create(new Schema(fields), allocator);
        vsr.allocateNew();
        int rows = vals.length;
        VarCharVector rid = (VarCharVector) vsr.getVector(0);
        VarCharVector val = (VarCharVector) vsr.getVector(1);
        for (int r = 0; r < rows; r++) {
            rid.setSafe(r, rowIds[r].toString().getBytes(StandardCharsets.UTF_8));
            val.setSafe(r, vals[r].toString().getBytes(StandardCharsets.UTF_8));
        }
        rid.setValueCount(rows);
        val.setValueCount(rows);
        vsr.setRowCount(rows);
        return vsr;
    }

    /** ExchangeSink that takes ownership on feed() and closes what it receives on close(). */
    private static final class OwningSink implements ExchangeSink {
        private final List<VectorSchemaRoot> received = new ArrayList<>();
        boolean closed = false;

        @Override
        public void feed(VectorSchemaRoot batch) {
            received.add(batch);
        }

        @Override
        public void close() {
            closed = true;
            for (VectorSchemaRoot b : received) {
                b.close();
            }
            received.clear();
        }
    }

    /** ExchangeSink whose feed() always throws — simulates the emit-time failure window. */
    private static final class ThrowingSink implements ExchangeSink {
        @Override
        public void feed(VectorSchemaRoot batch) {
            throw new IllegalStateException("downstream feed rejected");
        }

        @Override
        public void close() {}
    }

    /**
     * Success path: the single shard reports one batch, {@code shardComplete} triggers
     * {@code finish}, and the output is fed to the sink. Closing the sink (owner) releases the
     * output. Baseline that the happy path leaves nothing stranded.
     */
    public void testSuccessPathTransfersOwnershipAndLeavesNoLeak() {
        OwningSink sink = new OwningSink();
        AtomicInteger completes = new AtomicInteger();
        Stitcher stitcher = new Stitcher(allocator, outputFields(), 1, 1, sink, completes::incrementAndGet);

        VectorSchemaRoot resp = responseBatch(new Object[] { "0" }, new Object[] { "a" });
        stitcher.acceptBatch(resp, new int[] { 0 }, 0);
        resp.close(); // listener owns the response batch; stitcher only copied out of it
        stitcher.shardComplete();

        assertEquals("onComplete fired once", 1, completes.get());
        assertTrue("sink took ownership and was closed by finish", sink.closed);
        assertEquals("output released once the owning sink closed it", 0, allocator.getAllocatedMemory());

        // A defensive stage-terminal close() must be a no-op after ownership transferred.
        stitcher.close();
        assertEquals(0, allocator.getAllocatedMemory());
    }

    /**
     * Emit-time failure window: {@code parentSink.feed(output)} throws before ownership transfers.
     * The pre-fix code left {@code output} unclosed (finally ran only {@code onComplete}); the fix
     * closes it in {@code finish}'s finally. Assert the allocator returns to zero.
     */
    public void testFeedThrowDuringFinishDoesNotLeakOutput() {
        ThrowingSink sink = new ThrowingSink();
        AtomicInteger completes = new AtomicInteger();
        Stitcher stitcher = new Stitcher(allocator, outputFields(), 1, 1, sink, completes::incrementAndGet);

        VectorSchemaRoot resp = responseBatch(new Object[] { "0" }, new Object[] { "a" });
        stitcher.acceptBatch(resp, new int[] { 0 }, 0);
        resp.close();

        // finish() runs on shardComplete; feed() throws inside it. The throw still propagates (as it
        // did before the fix — the caller surfaces it), but the finally must free output first.
        expectThrows(IllegalStateException.class, stitcher::shardComplete);

        assertEquals("onComplete still fires via finish's finally", 1, completes.get());
        assertEquals("output freed despite feed() throwing before ownership transfer", 0, allocator.getAllocatedMemory());
    }

    /**
     * "finish never runs" window: a shard's fetch stream neither completes nor fails (dropped
     * listener / node lost / cancelled before terminal), so {@code pendingShards} never reaches
     * zero and {@code finish} is never called. The stage's terminal transition calls
     * {@code Stitcher.close()}, which must release the pre-allocated output.
     */
    public void testCloseReleasesOutputWhenFinishNeverRuns() {
        OwningSink sink = new OwningSink();
        // Two shards pending; only one reports, so finish() never fires on its own.
        Stitcher stitcher = new Stitcher(allocator, outputFields(), 2, 2, sink, () -> {});

        VectorSchemaRoot resp = responseBatch(new Object[] { "0" }, new Object[] { "a" });
        stitcher.acceptBatch(resp, new int[] { 0 }, 0);
        resp.close();
        assertTrue("output holds buffers while stitch is in flight", allocator.getAllocatedMemory() > 0);

        // Stage terminal (cancel/timeout) → close() the stitcher.
        stitcher.close();

        assertEquals("stage-terminal close() frees the never-emitted output", 0, allocator.getAllocatedMemory());

        // Idempotent: a late shardComplete (if it ever arrived) or a second close must not throw.
        stitcher.close();
        assertEquals(0, allocator.getAllocatedMemory());
    }
}

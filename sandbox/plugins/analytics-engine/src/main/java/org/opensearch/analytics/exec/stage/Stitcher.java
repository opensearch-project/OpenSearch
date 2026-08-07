/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.analytics.exec.VectorUtils;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.spi.ExchangeSink;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-LM-stage stitcher. Owns one pre-allocated output {@link VectorSchemaRoot} sized to K
 * (the survivor row count) and accumulates fetch-by-rowid response batches from N shards
 * into it. Per-shard listeners pass the position-array slice corresponding to each batch;
 * the stitcher copies each response row to the output via {@link FieldVector#copyFromSafe}.
 *
 * <p>TODO incremental emission. Today the entire stitched VSR is emitted in a single
 * {@code parentSink.feed} call after every shard's stream terminates — the post-LM stage
 * (Stage 3) cannot start until LM's terminal SUCCEEDED transition. This forces Stage 3 to
 * use the buffered MemTable sink. To enable streaming Stage 3 (which would let Camp-A
 * post-LM ops — Filter / Project / hash Aggregate — process rows as they arrive), the
 * stitcher would need to emit per-shard sub-batches at each shard's completion rather
 * than holding everything in {@code output} until the end. The natural emission unit is
 * "rows whose entire shard response has arrived" — those land at non-contiguous positions
 * across the K-row range, so the wire-side position-sortedness invariant would have to be
 * dropped (downstream sees rows in shard-completion order, not sort order). Camp-B post-LM
 * ops (Sort / TopN / global-frame Aggregate) need the full set anyway and would still buffer.
 * Defer until a real workload demands it.
 *
 * <p>Position is derived from the order contract with the data node:
 * {@code AnalyticsSearchService.executeFetchByRowIds} requires ascending {@code rowIds} in
 * the request and the native side returns rows in the same ascending order, so the
 * shard's per-shard {@code positions[]} array (built during Phase B in the same order as
 * the request's {@code rowIds[]}) lines up positionally with the response. The listener
 * tracks where its previous batch left off via {@code rowsCopiedSoFar}; this stitcher reads
 * {@code positions[rowsCopiedSoFar .. rowsCopiedSoFar + batchRows)}.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #acceptBatch} — listener calls per response batch from a shard. Synchronized
 *       so concurrent shards can't interleave Arrow buffer writes.</li>
 *   <li>{@link #shardComplete} — listener calls once when its shard's stream terminates.
 *       When all shards complete, the stitcher feeds the output to {@code parentSink} and
 *       runs {@code onComplete}.</li>
 *   <li>{@link #shardFailed} — listener calls on per-shard failure. Failures are collected
 *       (first surfaced as primary, others added as suppressed); completion path runs once
 *       and emits failure to the LM stage's outer listener.</li>
 * </ol>
 *
 * @opensearch.internal
 */
public final class Stitcher {

    private static final Logger logger = LogManager.getLogger(Stitcher.class);

    private final VectorSchemaRoot output;
    private final int totalRows;
    private final int aboveColCount;
    private final ExchangeSink parentSink;
    private final AtomicInteger pendingShards;
    private final ConcurrentLinkedQueue<Exception> failures = new ConcurrentLinkedQueue<>();
    private final Runnable onComplete;
    private final Object outputLock = new Object();
    /**
     * Guards the output VSR's disposition, mutated only under {@link #outputLock}. Set once, either
     * to {@code true} when {@link #finish} hands output to {@code parentSink} (ownership transferred
     * — this stitcher must not close it), or left {@code false} so exactly one of {@link #finish}'s
     * finally or {@link #close} frees it. Prevents a double-close race between the last shard's
     * {@code finish} and a concurrent terminal-transition {@code close}.
     */
    private boolean outputDisposed = false;

    public Stitcher(
        BufferAllocator allocator,
        List<Field> outputFields,
        int totalRows,
        int shardCount,
        ExchangeSink parentSink,
        Runnable onComplete
    ) {
        this.output = VectorSchemaRoot.create(new Schema(outputFields), allocator);
        // TODO pre-size vectors to totalRows via setInitialCapacity to avoid copyFromSafe
        // re-allocs during stitch. Today we let Arrow grow buffers dynamically — fine for
        // small K but wasteful for large K (varchar buffers in particular grow geometrically).
        this.output.allocateNew();
        this.totalRows = totalRows;
        this.aboveColCount = outputFields.size();
        this.parentSink = parentSink;
        this.pendingShards = new AtomicInteger(shardCount);
        this.onComplete = onComplete;
    }

    /**
     * Copies one shard's response batch into the output VSR. Reads positions starting at
     * {@code rowsCopiedSoFar} (the listener's per-shard "rows copied so far" counter) and
     * places each response row at {@code positions[rowsCopiedSoFar + srcRow]}.
     *
     * <p>The response batch's column layout is {@code [___row_id, fetch-col-0, fetch-col-1, ...]} —
     * fetch cols ordered as the request's {@code columns[]} (excluding the helper). The
     * helper column is identified by name and skipped during copy.
     */
    public void acceptBatch(VectorSchemaRoot batch, int[] positions, int rowsCopiedSoFar) {
        synchronized (outputLock) {
            int rowIdIdx = batch.getSchema().getFields().indexOf(batch.getSchema().findField(OpenSearchLateMaterialization.ROW_ID_FIELD));
            if (rowIdIdx < 0) {
                throw new IllegalStateException(
                    "Fetch response missing " + OpenSearchLateMaterialization.ROW_ID_FIELD + " column; got " + batch.getSchema()
                );
            }
            int batchRows = batch.getRowCount();
            if (rowsCopiedSoFar + batchRows > positions.length) {
                throw new IllegalStateException(
                    "Shard returned more rows than requested: positions.length="
                        + positions.length
                        + " rowsCopiedSoFar="
                        + rowsCopiedSoFar
                        + " batchRows="
                        + batchRows
                );
            }
            int responseColCount = batch.getSchema().getFields().size();
            for (int srcRow = 0; srcRow < batchRows; srcRow++) {
                int dstRow = positions[rowsCopiedSoFar + srcRow];
                int outCol = 0;
                for (int srcCol = 0; srcCol < responseColCount; srcCol++) {
                    if (srcCol == rowIdIdx) continue;
                    if (outCol >= aboveColCount) {
                        throw new IllegalStateException(
                            "Response column count exceeds output schema: outCol=" + outCol + " aboveColCount=" + aboveColCount
                        );
                    }
                    FieldVector srcVec = batch.getVector(srcCol);
                    FieldVector dstVec = output.getVector(outCol);
                    dstVec.copyFromSafe(srcRow, dstRow, srcVec);
                    outCol++;
                }
            }
        }
    }

    /** Signals that one shard's stream has terminated successfully. Triggers emit when last. */
    public void shardComplete() {
        if (pendingShards.decrementAndGet() == 0) {
            finish();
        }
    }

    /**
     * Records a per-shard failure and proceeds via the completion path. All shard failures
     * are retained — the first becomes the primary surfaced exception, the rest land as
     * suppressed exceptions so diagnostics aren't lost.
     *
     * <p>FIXME: today other in-flight shards continue running until they too complete or fail
     * — we don't fast-cancel. When the LM stage gains a {@code cancel(reason)} path that
     * propagates to in-flight transports, hook it here.
     */
    public void shardFailed(Exception e) {
        failures.offer(e);
        if (pendingShards.decrementAndGet() == 0) {
            finish();
        }
    }

    private void finish() {
        // Ownership of the output VSR transfers to parentSink only once feed() returns normally
        // (see ExchangeSink#feed — the sink then owns and releases it). Until then this stitcher
        // owns output's buffers, which live on the coordinator allocator; if setRowCount /
        // sanitizeNullViewSlots / feed throws before the hand-off, the finally must free them or
        // they leak onto the long-lived coordinator allocator (the pool never goes back down).
        boolean ownershipTransferred = false;
        try {
            if (failures.isEmpty()) {
                output.setRowCount(totalRows);
                VectorUtils.sanitizeNullViewSlots(output);
                parentSink.feed(output);
                // Mark disposed under the lock BEFORE anything else can observe it, so a concurrent
                // close() (from the stage's terminal transition) sees ownership has moved and won't
                // close the VSR parentSink now owns.
                synchronized (outputLock) {
                    outputDisposed = true;
                }
                ownershipTransferred = true;
                // Guard the sink close like the failure branch below: feed() already transferred
                // ownership of output, so a close-time failure must not propagate out of finish()
                // (it runs on a shard's GatherListener callback thread) — log and swallow it.
                try {
                    parentSink.close();
                } catch (Exception e) {
                    logger.warn(new ParameterizedMessage("[Stitcher] parentSink.close() failed after emit for {} rows", totalRows), e);
                }
                logger.debug("[Stitcher] emitted rows={}", totalRows);
            } else {
                try {
                    parentSink.close();
                } catch (Exception ignore) {}
            }
        } finally {
            if (ownershipTransferred == false) {
                closeOutputOnce();
            }
            onComplete.run();
        }
    }

    /**
     * Releases the pre-allocated output VSR if it was never emitted. Idempotent and safe to call
     * from the owning LM stage's terminal / cancel path to cover the window where {@link #finish}
     * never runs — e.g. a shard's fetch stream neither completes nor fails (dropped listener, node
     * lost mid-fetch, task cancelled before the stream terminates), so {@link #pendingShards} never
     * reaches zero. Without this, {@code output}'s buffers stay pinned on the coordinator allocator
     * for the node's lifetime. A no-op once ownership has transferred to {@code parentSink} via
     * {@link #finish}, and a no-op on repeated calls.
     */
    public void close() {
        closeOutputOnce();
    }

    /**
     * Closes {@code output} at most once, and never after ownership transferred to {@code parentSink}.
     * The {@code outputDisposed} flag is read-and-set under {@link #outputLock} so the last shard's
     * {@link #finish} and a concurrent terminal-transition {@link #close} cannot double-close (or
     * close a VSR the sink now owns).
     */
    private void closeOutputOnce() {
        synchronized (outputLock) {
            if (outputDisposed) {
                return;
            }
            outputDisposed = true;
            output.close();
        }
    }

    /**
     * Returns the surfaceable failure: the first one collected, with subsequent failures
     * attached as {@code addSuppressed}. {@code null} if no shard failed.
     */
    public Exception surfaceableFailure() {
        Exception primary = failures.poll();
        if (primary == null) return null;
        for (Exception next; (next = failures.poll()) != null;) {
            primary.addSuppressed(next);
        }
        return primary;
    }
}

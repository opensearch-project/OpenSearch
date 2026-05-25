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
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.spi.ExchangeSink;

import java.util.ArrayList;
import java.util.List;
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
    private final List<Exception> failures = new ArrayList<>();
    private final Runnable onComplete;

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
    public synchronized void acceptBatch(VectorSchemaRoot batch, int[] positions, int rowsCopiedSoFar) {
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
                FieldVector srcVec = (FieldVector) batch.getVector(srcCol);
                FieldVector dstVec = output.getVector(outCol);
                // FIXME [RemoveBeforeMainMerge] diagnostics for Stitcher copyFromSafe — log every
                // column on the first row of the first batch so we see all type pairings.
                if (srcRow == 0 && rowsCopiedSoFar == 0) {
                    logger.info(
                        "FIXME [RemoveBeforeMainMerge] Stitcher col {}: src(name={}, minorType={}, type={}) → dst(name={}, minorType={}, type={})",
                        outCol,
                        srcVec.getName(),
                        srcVec.getMinorType(),
                        srcVec.getField().getType(),
                        dstVec.getName(),
                        dstVec.getMinorType(),
                        dstVec.getField().getType()
                    );
                }
                try {
                    dstVec.copyFromSafe(srcRow, dstRow, srcVec);
                } catch (RuntimeException e) {
                    logger.error(
                        "FIXME [RemoveBeforeMainMerge] copyFromSafe FAILED at col {}: src(minorType={}, type={}) → dst(minorType={}, type={})",
                        outCol,
                        srcVec.getMinorType(),
                        srcVec.getField().getType(),
                        dstVec.getMinorType(),
                        dstVec.getField().getType(),
                        e
                    );
                    throw e;
                }
                outCol++;
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
    public synchronized void shardFailed(Exception e) {
        failures.add(e);
        if (pendingShards.decrementAndGet() == 0) {
            finish();
        }
    }

    private void finish() {
        try {
            if (failures.isEmpty()) {
                output.setRowCount(totalRows);
                // FIXME [FixBeforeMainMerge] Arrow leak: the per-query allocator reports 256 bytes
                // leaked at QueryContext.close after a successful run. Likely the output VSR's
                // ownership transfer to parentSink.feed leaves a buffer un-closed somewhere down
                // the parent's drain path. Reproduce, trace allocator ownership, and ensure every
                // FieldVector reaches close().
                parentSink.feed(output);
                parentSink.close();
                logger.debug("[Stitcher] emitted rows={}", totalRows);
            } else {
                output.close();
                try {
                    parentSink.close();
                } catch (Exception ignore) {}
            }
        } finally {
            onComplete.run();
        }
    }

    /**
     * Returns the surfaceable failure: the first one collected, with subsequent failures
     * attached as {@code addSuppressed}. {@code null} if no shard failed.
     */
    public synchronized Exception surfaceableFailure() {
        if (failures.isEmpty()) return null;
        Exception primary = failures.get(0);
        for (int i = 1; i < failures.size(); i++) {
            primary.addSuppressed(failures.get(i));
        }
        return primary;
    }
}

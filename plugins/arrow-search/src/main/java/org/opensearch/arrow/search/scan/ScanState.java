/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.arrow.search.Column;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * The state that the slices of a shard scan share while its query phase runs, and that the scan leaves for the end of the
 * stream.
 * <p>
 * Each slice of the query phase has its own collector, which builds and sends its own batches. The slices share the shard's
 * row limit, its stop conditions, and the bookkeeping of the batches and compute slots they hold, so a failed query phase
 * releases them all. The end of the stream reads the state once the query phase is over and no slice runs.
 */
final class ScanState implements Releasable {

    /** Sends a full batch to the consumer, taking ownership of it, and waits until the consumer can take more. */
    @FunctionalInterface
    interface BatchSender {
        void send(VectorSchemaRoot batch) throws Exception;
    }

    /** Hands out compute slots. A slice releases its slot while it waits for its consumer, and as soon as it fails. */
    @FunctionalInterface
    interface ComputeSlots {
        /** Returns a compute slot, or {@code null} if none freed up within {@code waitNanos}. */
        @Nullable
        Releasable acquire(long waitNanos) throws InterruptedException;
    }

    final ArrowScanSpec spec;
    final List<Column> columns;
    final Schema schema;
    final BufferAllocator allocator;
    /** Whether field-level security lets the caller see a field; the fields it hides read as nulls. */
    final Predicate<String> fieldVisible;
    /** Sends batches; slices call it concurrently. */
    final BatchSender sender;
    final ComputeSlots computeSlots;
    /** Whether the consumer went away or the scan's task was cancelled. */
    final BooleanSupplier consumerGone;
    /** The compute time each slice may spend, excluding waits for the consumer and for a compute slot, or -1 for no limit. */
    final long computeBudgetNanos;
    /** Collects the slices of the query phase, or {@code null} to collect segments one after another. */
    @Nullable
    final Executor sliceExecutor;
    /** The rows the shard may still stream, which the slices claim for the rows they keep. */
    final RowBudget rowBudget;

    /** Rows the slices sent during collection, not counting the rows left for the end of the stream. */
    final AtomicLong rowsSent = new AtomicLong();
    /** Whether a slice spent its compute budget, which stops the others. */
    volatile boolean timedOut;
    /** Whether the row limit may have cut the results. */
    volatile boolean terminatedEarly;
    /** Whether a slice failed, which stops the others. */
    volatile boolean failed;
    /**
     * Whether the scan's collectors collected the query phase. Another plugin's collector or query phase searcher may have
     * collected it instead.
     */
    volatile boolean collected;

    /** The batches being filled or left unsent, which a failed query phase releases. */
    private final Set<VectorSchemaRoot> batches = ConcurrentHashMap.newKeySet();
    /** The compute slots the slices hold, which a failed query phase releases. */
    private final Set<Releasable> computeSlotsHeld = ConcurrentHashMap.newKeySet();

    ScanState(
        ArrowScanSpec spec,
        BufferAllocator allocator,
        Predicate<String> fieldVisible,
        BatchSender sender,
        ComputeSlots computeSlots,
        BooleanSupplier consumerGone,
        long computeBudgetNanos,
        @Nullable Executor sliceExecutor
    ) {
        this.spec = spec;
        this.columns = spec.columns();
        this.schema = Column.toSchema(columns);
        this.allocator = allocator;
        this.fieldVisible = fieldVisible;
        this.sender = sender;
        this.computeSlots = computeSlots;
        this.consumerGone = consumerGone;
        this.computeBudgetNanos = computeBudgetNanos;
        this.sliceExecutor = sliceExecutor;
        this.rowBudget = new RowBudget(spec.shardRowLimit());
        this.terminatedEarly = spec.shardRowLimit() <= 0;
    }

    /** Whether the slices stop collecting: the shard reached its row limit, a slice spent its compute budget, or a slice failed. */
    boolean stopped() {
        return failed || timedOut || rowBudget.exhausted();
    }

    /** Tracks a new batch, so that a failed query phase releases it. */
    void building(VectorSchemaRoot batch) {
        batches.add(batch);
    }

    /** Stops tracking a batch that a slice hands to the consumer. */
    void sending(VectorSchemaRoot batch) {
        batches.remove(batch);
    }

    /** Takes a compute slot, or returns {@code null} if none freed up within {@code waitNanos}. */
    @Nullable
    Releasable tryAcquireComputeSlot(long waitNanos) throws InterruptedException {
        final Releasable slot = computeSlots.acquire(waitNanos);
        if (slot != null) {
            computeSlotsHeld.add(slot);
        }
        return slot;
    }

    /** Releases a compute slot, unless {@link #close} already released it. */
    void releaseComputeSlot(Releasable slot) {
        if (computeSlotsHeld.remove(slot)) {
            slot.close();
        }
    }

    /**
     * Takes the batches with rows not sent yet, which the last messages of the stream carry, and closes the empty ones. The
     * caller owns the batches returned.
     */
    List<VectorSchemaRoot> takeRemainingRows() {
        final List<VectorSchemaRoot> remaining = new ArrayList<>();
        for (VectorSchemaRoot batch : List.copyOf(batches)) {
            batches.remove(batch);
            if (batch.getRowCount() == 0) {
                batch.close();
            } else {
                remaining.add(batch);
            }
        }
        return remaining;
    }

    @Override
    public void close() {
        final List<Releasable> held = new ArrayList<>(computeSlotsHeld);
        computeSlotsHeld.removeAll(held);
        final List<VectorSchemaRoot> unsent = List.copyOf(batches);
        batches.removeAll(unsent);
        Releasables.close(held);
        unsent.forEach(VectorSchemaRoot::close);
    }

    /**
     * The rows a shard may stream, shared by its slices. A slice claims the rows it keeps in a batch once it has read them, so
     * the shard never streams more than its row limit, whichever slices find the matches. Claims are final, so once the limit
     * is reached it stays reached.
     */
    static final class RowBudget {
        private final long limit;
        private final AtomicLong claimed = new AtomicLong();

        RowBudget(long limit) {
            this.limit = limit;
        }

        /** Claims up to {@code rows} rows and returns how many the caller may keep; 0 once the limit is reached. */
        int claim(int rows) {
            while (true) {
                final long current = claimed.get();
                final long granted = Math.min(rows, limit - current);
                if (granted <= 0) {
                    return 0;
                }
                if (claimed.compareAndSet(current, current + granted)) {
                    return (int) granted;
                }
            }
        }

        /** Returns the rows not claimed yet. */
        long remaining() {
            return Math.max(0, limit - claimed.get());
        }

        /** Whether the slices claimed every row of the limit. */
        boolean exhausted() {
            return claimed.get() >= limit;
        }
    }
}

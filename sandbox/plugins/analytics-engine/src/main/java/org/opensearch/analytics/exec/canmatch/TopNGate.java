/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ShardSortBounds;

import java.util.List;

/**
 * Keeps the best {@code K} sort keys seen so far so the coordinator can tell whether a
 * not-yet-dispatched shard could still contribute a row.
 *
 * <p>Keys only, no rows — the real top-N is still assembled by the coordinator's blocking
 * {@code SortExec: TopK}. This is a cheap side tally that answers one question: is this shard's
 * whole range worse than the worst of the {@code K} keys already in hand?
 *
 * <p>{@link #bottom()} only ever improves, so once a shard is eliminable it stays eliminable.
 * That is why the verdict can be taken on the dispatch path, at any time, on any thread.
 *
 * <p>Every uncertainty answers "keep the shard": a wrong keep costs one shard scan, a wrong
 * eliminate drops rows from the result.
 *
 * <p>Two entry points, one per side of the query. {@link #feed} takes a whole shard response and
 * reads its sort column; {@link #canEliminate} asks the resulting bar about a shard not yet sent.
 * {@link #offer} is the per-key seam between them, useful on its own in tests.
 *
 * <p>Every method that touches the heap is {@code synchronized}: feeding runs on stream-response
 * threads, one per shard, and the verdict is read on whichever thread frees a dispatch slot. The
 * critical section is a few {@code long} compares. {@link #feed} deliberately is <em>not</em>
 * synchronized as a whole — it would hold the lock for an entire batch and stall dispatch — so it
 * reads its batch outside the lock and takes it once per key.
 *
 * @opensearch.internal
 */
public final class TopNGate {

    private static final Logger logger = LogManager.getLogger(TopNGate.class);

    /**
     * Largest {@code K} worth gating. The heap costs {@code 8·K} bytes, and a limit this large
     * rarely fills up anyway — above it the gate would be pure overhead, so we build none.
     */
    static final int MAX_CAPACITY = 100_000;

    private final long[] heap;
    private final int capacity;
    private final boolean descending;

    /** Sort column to read out of each response batch, from the same {@link SortSpec} as the heap. */
    private final String column;

    private int size;

    /** Once set, every question answers "keep the shard". See {@link #disable}. */
    private boolean disabled;

    /**
     * Heap-only ctor for tests that drive {@link #offer} directly and never touch {@link #feed},
     * so the column name is immaterial.
     */
    TopNGate(int capacity, boolean descending) {
        this(capacity, descending, "@timestamp");
    }

    TopNGate(int capacity, boolean descending, String column) {
        if (capacity <= 0 || capacity > MAX_CAPACITY) {
            throw new IllegalArgumentException("capacity must be in 1.." + MAX_CAPACITY + ", got " + capacity);
        }
        this.heap = new long[capacity];
        this.capacity = capacity;
        this.descending = descending;
        this.column = column;
    }

    /**
     * Builds a gate for {@code sortSpec}, or {@code null} when this shape isn't gateable — no spec,
     * or a limit past {@link #MAX_CAPACITY}.
     *
     * <p>Carries no value domain of its own: every bound it is asked about has already been checked
     * against every other in {@code ShardFragmentStageExecution.setupSortGate}, which builds no gate
     * at all when they disagree. See doc 16 Stage 7.
     */
    public static TopNGate create(SortSpec sortSpec) {
        if (sortSpec == null || sortSpec.limit() <= 0 || sortSpec.limit() > MAX_CAPACITY) {
            return null;
        }
        return new TopNGate(sortSpec.limit(), sortSpec.descending(), sortSpec.column());
    }

    /**
     * Retires the gate for the rest of the query — every shard is kept from here on.
     *
     * <p>Called when the arriving keys can't be trusted to measure the same thing as the bounds:
     * the sort column missing from a response schema, or nulls a shard claimed it did not have.
     * That's the one failure mode that would give wrong results rather than a lost optimisation, so
     * there is no re-enable.
     */
    public synchronized void disable() {
        disabled = true;
    }

    /** True once {@link #disable} has been called. */
    public synchronized boolean isDisabled() {
        return disabled;
    }

    /**
     * Offers one sort key. Kept while the heap isn't full, or if it beats the current worst;
     * otherwise dropped.
     */
    public synchronized void offer(long key) {
        if (disabled) {
            return;
        }
        if (size < capacity) {
            heap[size] = key;
            siftUp(size);
            size++;
            return;
        }
        if (outranks(key, heap[0])) {
            heap[0] = key;
            siftDown(0);
        }
    }

    /**
     * True when {@code bounds} describes a shard that provably cannot place a row in the
     * top-{@code K} already collected.
     *
     * <p>The comparison is <b>strict</b>: a shard whose best value ties {@link #bottom()} may hold
     * a row the wait-for-all baseline would surface, so it is kept. Same rule as vanilla's
     * {@code FieldSortBuilder.isBottomSortShardDisjoint}.
     */
    public synchronized boolean canEliminate(ShardSortBounds bounds) {
        if (disabled) {
            return false;
        }
        // Calcite maps DESC to NULLS FIRST, so one null outranks every real value and puts the
        // shard in the top-K on its own.
        if (bounds == null || bounds.hasNulls()) {
            return false;
        }
        // Fewer than K keys means there is no bar yet to be worse than.
        if (size < capacity) {
            return false;
        }
        long shardBest = descending ? bounds.max() : bounds.min();
        return outranks(heap[0], shardBest);
    }

    // ── reading keys out of a shard response ──────────────────────────────

    /**
     * Reads every row's sort key out of {@code vsr} and offers it to the heap.
     *
     * <p>Must be called <b>before</b> the batch is handed to the sink: the sink takes ownership of
     * the {@link VectorSchemaRoot} and may close it.
     *
     * <p>The keys are compared against bounds read from parquet statistics, so both must be the same
     * quantity in the same units. Cross-shard agreement on the value domain is settled before any
     * response arrives, in {@code ShardFragmentStageExecution.setupSortGate}; what this method checks
     * is what only the data can show — a sort column absent from the response schema, or nulls a
     * shard claimed it did not have. Either {@linkplain #disable() retires the gate}, because both
     * would give wrong results rather than just lose the optimisation.
     *
     * <p>Called from many stream-response threads, one per shard. The column is resolved against the
     * schema of each batch rather than cached: the index is a fact about one response, and the
     * per-batch cost is a few string compares amortised over every row.
     */
    public void feed(VectorSchemaRoot vsr) {
        if (isDisabled()) {
            return;
        }
        int rows = vsr.getRowCount();
        if (rows == 0) {
            // Not a schema worth judging — an empty batch must not retire the gate.
            return;
        }
        int index = indexOf(vsr, column);
        if (index < 0) {
            // Expected, not a bug. The spec's column comes from the Sort's INPUT row type, but a
            // Project above the Sort can drop it before the rows reach the wire. Single-shard
            // queries are where that happens: with no exchange to stay above, the projection sinks
            // into the fragment. `sort - ts | head 3 | fields host` on a one-shard index sends
            // Schema<host> only. Multi-shard keeps the Project on the coordinator — it has to merge
            // by the key — so the column is still there.
            //
            // No keys to feed, so the gate can never arm; the query runs as it did before this
            // feature. Skipping the check instead would hand getVector(-1) an out-of-range index and
            // throw from the stream handler, failing a working query and stranding the batch this
            // call was supposed to hand to the sink.
            retire("sort column '" + column + "' absent from the response schema " + vsr.getSchema());
            return;
        }
        FieldVector vector = vsr.getVector(index);
        if (vector.getNullCount() > 0) {
            // canEliminate trusts the bounds' hasNulls=false claim; data contradicting it
            // invalidates the whole predicate, not just this batch.
            retire("sort column '" + column + "' contains nulls the shard bounds did not report");
            return;
        }
        readInto(vector, rows);
    }

    /**
     * Position of {@code name} in the batch schema, or {@code -1} when absent. Not
     * {@code Schema.findField}: that throws, and a miss is expected here.
     */
    private static int indexOf(VectorSchemaRoot vsr, String name) {
        List<Field> fields = vsr.getSchema().getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (name.equals(fields.get(i).getName())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Per-row primitive reads. Not {@code ArrowValues.toJavaValue}: that boxes to {@code Object}
     * and converts timestamps to {@code Instant}, and this runs once per row of every response.
     *
     * <p>Covers the value domains the shard check can report bounds for: parquet {@code Int32}/{@code Int64},
     * and {@code Int64} annotated millis or nanos. Millis and nanos are the only timestamp units
     * OpenSearch writes ({@code date} and {@code date_nanos}); micros is a parquet logical type we
     * never produce, so a {@code TimeStampMicroVector} arm would be unreachable.
     *
     * <p>A type not listed here contributes nothing and leaves the heap short of {@code K}, so the
     * gate can never eliminate — the fail-open direction. {@code short} and {@code byte} columns land
     * here today ({@code SmallIntVector} / {@code TinyIntVector}); see doc 16 Stage 6.
     */
    private void readInto(FieldVector vector, int rows) {
        // Most-specific type first. The TimeStamp*Vector classes aren't BigIntVector subclasses,
        // but the explicit order documents the intended dispatch.
        if (vector instanceof TimeStampMilliVector millis) {
            for (int i = 0; i < rows; i++) {
                offer(millis.get(i));
            }
        } else if (vector instanceof TimeStampNanoVector nanos) {
            for (int i = 0; i < rows; i++) {
                offer(nanos.get(i));
            }
        } else if (vector instanceof BigIntVector longs) {
            for (int i = 0; i < rows; i++) {
                offer(longs.get(i));
            }
        } else if (vector instanceof IntVector ints) {
            for (int i = 0; i < rows; i++) {
                offer(ints.get(i));
            }
        }
    }

    private void retire(String reason) {
        disable();
        logger.debug("sort-et: gate disabled — {}", reason);
    }

    /**
     * {@code K} — how many keys the gate needs before it can eliminate anything. Public for
     * {@code SortGateWiringTests}, which asserts the gate was built from the sort spec's limit.
     */
    public int capacity() {
        return capacity;
    }

    /** True once {@code K} keys have been collected — before that nothing can be eliminated. */
    public synchronized boolean isArmed() {
        return size == capacity;
    }

    /**
     * Worst key among the best {@code K} seen so far — the bar a shard has to clear.
     *
     * @throws IllegalStateException if fewer than {@code K} keys have been observed, since then
     *                               there is no bar
     */
    public synchronized long bottom() {
        if (size < capacity) {
            throw new IllegalStateException("bottom() is undefined until " + capacity + " keys are observed (have " + size + ")");
        }
        return heap[0];
    }

    /**
     * True when {@code a} is a better sort key than {@code b}: larger for {@code DESC}, smaller
     * for {@code ASC}. Strict, so equal keys never outrank each other.
     */
    private boolean outranks(long a, long b) {
        return descending ? a > b : a < b;
    }

    // The heap keeps the WORST of the top-K at the root, so admitting a key is one compare against
    // heap[0] and bottom() is O(1). A long[] rather than a PriorityQueue<Long> because this runs
    // once per row of every shard response, where boxing would cost more than the gate saves.

    private void siftUp(int index) {
        while (index > 0) {
            int parent = (index - 1) >>> 1;
            if (outranks(heap[parent], heap[index]) == false) {
                break; // parent already no better than the child — heap order holds
            }
            swap(parent, index);
            index = parent;
        }
    }

    private void siftDown(int index) {
        while (true) {
            int left = (index << 1) + 1;
            if (left >= size) {
                return;
            }
            int right = left + 1;
            // Descend toward the worse child, so the root keeps holding the overall worst.
            int worst = (right < size && outranks(heap[left], heap[right])) ? right : left;
            if (outranks(heap[index], heap[worst]) == false) {
                return;
            }
            swap(index, worst);
            index = worst;
        }
    }

    private void swap(int i, int j) {
        long tmp = heap[i];
        heap[i] = heap[j];
        heap[j] = tmp;
    }
}

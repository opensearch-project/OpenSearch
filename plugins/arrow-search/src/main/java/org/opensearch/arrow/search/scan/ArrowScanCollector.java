/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.arrow.search.Column;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Collects the matches of one slice of a shard into Arrow batches, and sends each batch from within collection as soon as
 * it is full.
 * <p>
 * Sending waits while the consumer is not ready, which pauses the slice in place. The scorer and doc-values iterators stay
 * open, and collection resumes on the next match once the consumer takes the batch. So the query is evaluated once, however
 * many batches it produces.
 * <p>
 * The slice buffers matches in chunks and reads their doc values column by column. It sizes each chunk from the bytes per
 * row seen so far. A batch that overshoots its byte budget keeps the rows that fit, and the other rows start the next batch.
 * <p>
 * Once the shard reaches its row limit, a slice keeps scoring until its next match, which shows that the limit cut the
 * results. A regular search also reports early termination only once a match past its limit arrives. The slice keeps
 * scoring for that match only while it holds a compute slot and has compute budget left. A slice that has no compute
 * slot, or whose compute budget runs out first, stops and reports that the limit may have cut the results. It reports no
 * timeout, since the shard streamed every row it may.
 * <p>
 * Each slice of the query phase has its own collector, which only the thread that collects the slice uses. The slices
 * share the shard's {@link ScanState}. A slice holds a compute slot while it reads a segment, but not while it waits to send.
 */
final class ArrowScanCollector implements Collector {

    /** The most matches a chunk buffers before their doc values are read. */
    static final int MAX_CHUNK = 1024;
    /** The docs a chunk or a pass holds before any row was measured. */
    static final int SEED_CHUNK = 32;
    /** How many matches the slice buffers between two checks of its limits. */
    private static final int CLOCK_INTERVAL = 64;
    /** The fewest rows a new batch presizes its vectors for before any row was measured. */
    private static final int MIN_INITIAL_ROWS = 16;
    /** The assumed bytes of a variable-width value until values are measured. */
    private static final double DEFAULT_VALUE_BYTES = 16;
    /** The assumed values per row of a list column until lists are measured. */
    private static final double DEFAULT_VALUES_PER_LIST = 2;
    /** How often a slice that waits for a compute slot checks whether its scan stopped or was cancelled. */
    private static final long SLOT_RECHECK_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    /** The collector of the slice that the current thread collects, which {@link #budgetCheck} applies to. */
    private static final ThreadLocal<ArrowScanCollector> COLLECTING = new ThreadLocal<>();

    private final ScanState state;
    private final QueryShardContext context;
    private final int[] chunk = new int[MAX_CHUNK];
    private final ColumnWriters.Scratch scratch = new ColumnWriters.Scratch();
    private final ColumnWriters.Allowance allowance = new ColumnWriters.Allowance();

    private Weight weight;
    /** Whether the slice stopped collecting, because the shard stopped or this slice spent its compute budget. */
    private boolean done;

    /** The batch being filled, or {@code null} until the slice reads its next rows. */
    private VectorSchemaRoot root;
    private int rows;
    /** The message bytes of the batch. */
    private long bytes;
    /** The bytes the batch may take, which double with each batch the slice sends, up to the configured batch size. */
    private long budget;
    /** How many matches to buffer before their doc values are read. */
    private int chunkTarget;

    private LeafReaderContext leaf;
    private ColumnWriters.LeafColumnWriter[] writers;
    /** How many matches at the start of {@link #chunk} are not in the batch yet. */
    private int buffered;

    private int batchesSent;
    /** Running estimate of the body bytes of a row, which sizes chunks and vectors; 0 until measured. */
    private double bytesPerRow;
    /**
     * Per column, the estimate that presizes its vectors: the average bytes of a value of a variable-width column, or the
     * average values per row of a list column. 0 until measured.
     */
    private final double[] bytesPerValue;
    /** Per list column of variable-width values, the average bytes of a value; 0 until measured. */
    private final double[] itemBytesPerValue;
    /** The body bytes of a row as estimated from the schema, which size the first batch until rows are measured. */
    private final double schemaRowBytes;

    /**
     * The compute slot the slice holds while it reads a segment, or {@code null} when it holds none, e.g. while it waits for
     * its consumer or for a slot.
     */
    private Releasable computeSlot;
    /** The compute time the slice spent up to when it last released its compute slot. */
    private long computeNanos;
    /** When the slice last took its compute slot. */
    private long resumedAt;
    private int clock;
    /** Bytes per row of the docs the last flush pass read. */
    private double passRowBytes;
    /** Whether the searcher is scoring a segment: its scorer is set and the segment is not finished. */
    private boolean scoring;
    /** Whether a flush is reading doc values into the batch. */
    private boolean flushing;

    ArrowScanCollector(ScanState state, QueryShardContext context) {
        this.state = state;
        this.context = context;
        this.bytesPerValue = new double[state.columns.size()];
        this.itemBytesPerValue = new double[state.columns.size()];
        this.schemaRowBytes = schemaRowBytes(state.columns);
        this.budget = state.spec.batchBudget(0);
        this.bytes = emptyBatchBytes();
        this.done = state.stopped();
        this.chunkTarget = chunkTarget();
    }

    /**
     * Returns the check that the searcher runs while it scores. The check ends the slice that the current thread collects once
     * the slice's compute budget is spent or the shard stopped.
     */
    static Runnable budgetCheck(ScanState state) {
        return () -> {
            final ArrowScanCollector collector = COLLECTING.get();
            if (collector != null && collector.state == state) {
                collector.checkBudgetWhileScoring();
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void setWeight(Weight weight) {
        this.weight = weight;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext leafContext) throws IOException {
        if (weight != null && weight.count(leafContext) == 0) {
            throw new CollectionTerminatedException();
        }
        if (done) {
            throw stopCollecting();
        }
        try {
            if (acquireComputeSlot() == false) {
                throw stopCollecting();
            }
            checkLimits();
            if (done) {
                releaseComputeSlot();
                throw stopCollecting();
            }
            leaf = leafContext;
            writers = openWriters(leafContext);
        } catch (CollectionTerminatedException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            failed();
            throw e;
        }
        buffered = 0;
        COLLECTING.set(this);
        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {
                // The searcher sets the scorer right before it scores the segment.
                scoring = true;
            }

            @Override
            public void collect(int doc) throws IOException {
                try {
                    ArrowScanCollector.this.collect(doc);
                } catch (CollectionTerminatedException e) {
                    throw e;
                } catch (IOException | RuntimeException e) {
                    failed();
                    throw e;
                }
            }

            @Override
            public void finish() throws IOException {
                scoring = false;
                COLLECTING.remove();
                try {
                    flush();
                } catch (IOException | RuntimeException e) {
                    failed();
                    throw e;
                }
                writers = null;
                releaseComputeSlot();
            }
        };
    }

    /**
     * Ends collection while docs of the segment that may match are left unscored. If the shard reached its row limit, it
     * records that the limit may have cut the results.
     */
    private CollectionTerminatedException stopCollecting() {
        done = true;
        if (state.rowBudget.exhausted()) {
            state.terminatedEarly = true;
        }
        return new CollectionTerminatedException();
    }

    /**
     * Fails the scan when one of its slices fails outside the collector, e.g. in the query's scorer. It runs on the failed
     * slice's thread, and also fails the slice that the thread collects, if that slice belongs to the scan.
     */
    static void sliceFailed(ScanState state) {
        state.failed = true;
        final ArrowScanCollector collector = COLLECTING.get();
        if (collector != null && collector.state == state) {
            collector.failed();
        }
    }

    /**
     * Fails the slice. The slice frees its compute slot at once, since the query phase ends only once every slice ends. It
     * also stops the other slices of the shard, whose results no longer matter.
     */
    private void failed() {
        state.failed = true;
        COLLECTING.remove();
        releaseComputeSlot();
    }

    private ColumnWriters.LeafColumnWriter[] openWriters(LeafReaderContext leafContext) throws IOException {
        final List<Column> columns = state.columns;
        final ColumnWriters.LeafColumnWriter[] leafWriters = new ColumnWriters.LeafColumnWriter[columns.size()];
        for (int c = 0; c < leafWriters.length; c++) {
            final Column column = columns.get(c);
            leafWriters[c] = ColumnWriters.open(column, leafContext, context, state.fieldVisible.test(column.name()), scratch);
        }
        return leafWriters;
    }

    private void collect(int doc) throws IOException {
        if (done || state.rowBudget.exhausted()) {
            // Past the shard's row limit, this match shows that the limit cut the results.
            throw stopCollecting();
        }
        chunk[buffered++] = doc;
        if (buffered >= chunkTarget) {
            flush();
        } else if (++clock % CLOCK_INTERVAL == 0) {
            checkLimits();
        }
        if (done && awaitsMatchPastLimit() == false) {
            if (doc == leaf.reader().maxDoc() - 1) {
                // The segment has no doc left to score.
                throw new CollectionTerminatedException();
            }
            throw stopCollecting();
        }
    }

    /**
     * Whether the slice keeps scoring past the shard's row limit, within its compute budget, so that its next match, if any,
     * shows that the limit cut the results. It does so if the shard reached its row limit with no match cut so far, the slice
     * holds a compute slot and no docs past the limit, and the scan has not failed, timed out, been cancelled or lost its
     * consumer.
     */
    private boolean awaitsMatchPastLimit() {
        return computeSlot != null
            && buffered == 0
            && state.rowBudget.exhausted()
            && state.terminatedEarly == false
            && state.failed == false
            && state.timedOut == false
            && state.consumerGone.getAsBoolean() == false;
    }

    /**
     * Reads the doc values of the buffered docs into the batch, and sends the batch each time it is full.
     * <p>
     * Each pass reads only as many docs as fit by the size of the rows read last. It ends early at the row whose values take
     * more than the bytes the batch has left. So rows that suddenly grow cost at most about one batch of overshoot per column
     * of variable-size values before the estimate catches up.
     */
    private void flush() throws IOException {
        flushing = true;
        try {
            while (buffered > 0) {
                if (root == null) {
                    root = newRoot();
                }
                final int offset = rows;
                final int count = Math.min(buffered, passTarget());
                final long bytesBefore = bytes;
                int written = count;
                try {
                    final List<FieldVector> vectors = root.getFieldVectors();
                    allowance.reset(budget - bytes, count);
                    for (int c = 0; c < writers.length; c++) {
                        // A column stops after the doc whose row, counting the columns before it, overdraws the allowance, which ends the
                        // pass there. rowsThatFit then cuts the batch to the rows that fit.
                        written = writers[c].write(chunk, written, vectors.get(c), offset, allowance);
                        allowance.nextColumn(written);
                    }
                    ColumnWriters.setRowCount(root, offset + written);
                } catch (OutOfMemoryException e) {
                    throw rejection(e);
                }
                final int total = offset + written;
                int emitted = total;
                long totalBytes = BatchBytes.messageBytes(root, total);
                passRowBytes = Math.max(1, (double) (totalBytes - bytesBefore) / written);
                if (totalBytes > budget) {
                    emitted = rowsThatFit(offset, total);
                    if (emitted == 0) {
                        // A batch holds at least one row, so a row larger than the budget still goes out on its own.
                        emitted = 1;
                        final long rowBytes = BatchBytes.messageBytes(root, 1);
                        if (rowBytes > state.spec.maxRowBytes()) {
                            throw new ArrowScanDataException(
                                "a row of ["
                                    + new ByteSizeValue(rowBytes)
                                    + "] exceeds the maximum Arrow search row size of ["
                                    + new ByteSizeValue(state.spec.maxRowBytes())
                                    + "]"
                            );
                        }
                    }
                    ColumnWriters.setRowCount(root, emitted);
                    totalBytes = BatchBytes.messageBytes(root, emitted);
                }
                int consumed = emitted - offset;
                // The shard's other slices may have taken the rest of its rows.
                final int granted = state.rowBudget.claim(consumed);
                if (granted < consumed) {
                    consumed = granted;
                    emitted = offset + granted;
                    ColumnWriters.setRowCount(root, emitted);
                    totalBytes = BatchBytes.messageBytes(root, emitted);
                }
                rows = emitted;
                bytes = totalBytes;
                if (rows > 0) {
                    measure();
                }
                if (state.rowBudget.exhausted()) {
                    // The docs not kept are past the shard's row limit. The batch goes out with the end of the stream.
                    if (buffered > consumed) {
                        state.terminatedEarly = true;
                    }
                    buffered = 0;
                    done = true;
                    break;
                }
                // Some columns read docs [consumed, count), which did not fit. No column read the docs past count yet.
                final boolean readPastBatch = consumed < count;
                System.arraycopy(chunk, consumed, chunk, 0, buffered - consumed);
                buffered -= consumed;
                if (readPastBatch || bytes >= budget || rows >= state.spec.maxBatchRows()) {
                    sendBatch();
                    if (readPastBatch) {
                        // The doc-values iterators already passed the docs that did not fit, so new writers read them again.
                        writers = openWriters(leaf);
                    }
                }
            }
        } finally {
            flushing = false;
        }
        checkLimits();
        chunkTarget = chunkTarget();
    }

    /** Returns how many buffered docs the next pass of a flush reads: as many as fit by the size of the latest rows. */
    private int passTarget() {
        final double rowBytes = Math.max(bytesPerRow, passRowBytes);
        final long byBytes = rowBytes > 0 ? (long) ((budget - bytes) / rowBytes) : SEED_CHUNK;
        final long byRows = Math.min(state.spec.maxBatchRows() - rows, state.rowBudget.remaining());
        return (int) Math.max(1, Math.min(byBytes, byRows));
    }

    /**
     * Sends the full batch, which pauses the slice until the consumer can take more. The slice frees its compute slot meanwhile.
     */
    private void sendBatch() throws IOException {
        final VectorSchemaRoot batch = root;
        root = null;
        state.sending(batch);
        state.rowsSent.addAndGet(rows);
        batchesSent++;
        rows = 0;
        budget = state.spec.batchBudget(batchesSent);
        bytes = emptyBatchBytes();
        releaseComputeSlot();
        try {
            state.sender.send(batch);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        if (acquireComputeSlot()) {
            // The scan may have stopped, or its consumer may have gone away, while the slice waited.
            checkLimits();
        }
    }

    /**
     * Takes a compute slot, waiting while other slices and scans compute, but not past the end of the scan. A cancelled scan
     * fails. A scan that another slice stopped (row limit, compute budget, failure) ends this slice too.
     *
     * @return whether the slice holds a compute slot and may compute; {@code false} once the scan stopped, which ends the slice
     */
    private boolean acquireComputeSlot() {
        if (computeSlot != null) {
            return true;
        }
        try {
            while (true) {
                if (state.consumerGone.getAsBoolean()) {
                    throw new TaskCancelledException("the consumer of the stream went away or the scan was cancelled");
                }
                if (state.stopped()) {
                    if (buffered > 0 && state.rowBudget.exhausted()) {
                        // The buffered docs are matches past the shard's row limit.
                        state.terminatedEarly = true;
                    }
                    done = true;
                    buffered = 0;
                    return false;
                }
                final Releasable slot = state.tryAcquireComputeSlot(SLOT_RECHECK_NANOS);
                if (slot != null) {
                    computeSlot = slot;
                    resumedAt = System.nanoTime();
                    return true;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TaskCancelledException("interrupted while waiting for a compute slot");
        }
    }

    private void releaseComputeSlot() {
        final Releasable slot = computeSlot;
        if (slot != null) {
            computeSlot = null;
            computeNanos += System.nanoTime() - resumedAt;
            state.releaseComputeSlot(slot);
        }
        // Looked-up values may grow the scratch buffers to a few batches, which no memory limit counts. Only the slices that compute keep
        // them, so a slice that waits for its consumer or for a slot holds little heap.
        scratch.trim();
    }

    /**
     * Ends the slice once its compute budget is spent or the shard stopped. The searcher runs this check while the scorer
     * moves through a segment, so docs that do not match count against the budget too. The check acts only after the
     * segment's scorer is set and while no flush runs, since only then does the searcher handle the stop.
     */
    void checkBudgetWhileScoring() {
        if (scoring && flushing == false) {
            checkStopped();
            // A slice that awaits a match past the row limit kept every row it may, so a spent budget ends the wait without a timeout.
            if (done && (awaitsMatchPastLimit() == false || budgetSpent())) {
                throw stopCollecting();
            }
        }
    }

    /** Whether the slice spent its compute budget, which excludes its waits for the consumer and for a compute slot. */
    private boolean budgetSpent() {
        if (state.computeBudgetNanos < 0) {
            return false;
        }
        final long computing = computeSlot != null ? System.nanoTime() - resumedAt : 0;
        return computeNanos + computing >= state.computeBudgetNanos;
    }

    /**
     * Fails the slice if its consumer went away or the scan was cancelled, and ends it once the shard stopped or its compute
     * budget is spent.
     */
    private void checkLimits() {
        if (state.consumerGone.getAsBoolean()) {
            throw new TaskCancelledException("the consumer of the stream went away or the scan was cancelled");
        }
        checkStopped();
    }

    /**
     * Ends the slice once the shard stopped, or once its compute budget is spent, which times the scan out. A scan that
     * another slice already stopped, e.g. at the row limit, does not time out, whatever this slice's budget.
     */
    private void checkStopped() {
        if (done == false) {
            done = state.stopped();
            if (done == false && budgetSpent()) {
                state.timedOut = true;
                done = true;
            }
        }
    }

    /** Returns the most rows, in {@code [offset, total)}, whose message fits the budget, given that {@code offset} rows fit. */
    private int rowsThatFit(int offset, int total) {
        int lo = offset;
        int hi = total;
        while (hi - lo > 1) {
            final int mid = (lo + hi) >>> 1;
            if (BatchBytes.messageBytes(root, mid) <= budget) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return lo == 0 && BatchBytes.messageBytes(root, 0) > budget ? 0 : lo;
    }

    private VectorSchemaRoot newRoot() {
        final long remaining = state.rowBudget.remaining();
        // The rows the budget holds: by the measured row size, or, until rows are measured, by the schema's estimate of the
        // budget left after the batch's metadata.
        final double rowsInBudget = bytesPerRow > 0
            ? budget / bytesPerRow
            : Math.min(MAX_CHUNK, Math.max(0, budget - emptyBatchBytes()) / schemaRowBytes);
        // Measured rows near or above the budget presize one row, not many times the budget, since vectors grow as rows are written.
        final int minRows = bytesPerRow > 0 ? 1 : MIN_INITIAL_ROWS;
        final int initialRows = (int) Math.max(minRows, Math.min(Math.min(remaining, state.spec.maxBatchRows()), rowsInBudget));
        final VectorSchemaRoot created = VectorSchemaRoot.create(state.schema, state.allocator);
        state.building(created);
        final List<FieldVector> vectors = created.getFieldVectors();
        for (int c = 0; c < vectors.size(); c++) {
            final FieldVector vector = vectors.get(c);
            if (vector instanceof VarCharVector varChar) {
                varChar.setInitialCapacity(initialRows, bytesPerValue[c] > 0 ? bytesPerValue[c] : DEFAULT_VALUE_BYTES);
            } else if (vector instanceof ListVector list) {
                final double valuesPerRow = bytesPerValue[c] > 0 ? bytesPerValue[c] : DEFAULT_VALUES_PER_LIST;
                list.setInitialCapacity(initialRows, valuesPerRow);
                if (list.getDataVector() instanceof VarCharVector items) {
                    final double itemBytes = itemBytesPerValue[c] > 0 ? itemBytesPerValue[c] : DEFAULT_VALUE_BYTES;
                    items.setInitialCapacity((int) Math.ceil(initialRows * valuesPerRow), itemBytes);
                }
            } else {
                vector.setInitialCapacity(initialRows);
            }
        }
        try {
            created.allocateNew();
        } catch (OutOfMemoryException e) {
            throw rejection(e);
        }
        return created;
    }

    /** Updates the size estimates that plan later chunks and batches. */
    private void measure() {
        final long body = bytes - BatchBytes.metadataBytes(root);
        bytesPerRow = Math.max(1, (double) body / rows);
        final List<FieldVector> vectors = root.getFieldVectors();
        for (int c = 0; c < vectors.size(); c++) {
            if (vectors.get(c) instanceof VarCharVector varChar) {
                bytesPerValue[c] = Math.max(1, (double) dataBytes(varChar, rows) / rows);
            } else if (vectors.get(c) instanceof ListVector list) {
                final int items = list.getOffsetBuffer().getInt((long) rows * ListVector.OFFSET_WIDTH);
                bytesPerValue[c] = Math.max(0.1, (double) items / rows);
                if (items > 0 && list.getDataVector() instanceof VarCharVector itemVector) {
                    itemBytesPerValue[c] = Math.max(1, (double) dataBytes(itemVector, items) / items);
                }
            }
        }
    }

    private static int dataBytes(VarCharVector vector, int values) {
        return vector.getOffsetBuffer().getInt((long) values * VarCharVector.OFFSET_WIDTH);
    }

    /** Returns the body bytes of a row of {@code columns}, assuming default sizes for variable-size values. */
    private static double schemaRowBytes(List<Column> columns) {
        double bytes = 0;
        for (Column column : columns) {
            final double value = switch (column.type()) {
                case BOOL -> 1.0 / Byte.SIZE;
                case INT8 -> 1;
                case INT16 -> 2;
                case INT32, FLOAT32 -> 4;
                case IP -> 16;
                case UTF8 -> VarCharVector.OFFSET_WIDTH + DEFAULT_VALUE_BYTES;
                default -> 8;
            };
            // The row's validity bit, and its value or its list of values.
            bytes += 1.0 / Byte.SIZE + (column.list() ? ListVector.OFFSET_WIDTH + DEFAULT_VALUES_PER_LIST * value : value);
        }
        return Math.max(1, bytes);
    }

    private long emptyBatchBytes() {
        return BatchBytes.MESSAGE_OVERHEAD + BatchBytes.COLUMN_OVERHEAD * 2L * state.columns.size();
    }

    private int chunkTarget() {
        final long byBytes = bytesPerRow > 0 ? (long) ((budget - bytes) / bytesPerRow) : SEED_CHUNK;
        final long byRows = Math.min(state.spec.maxBatchRows() - rows, state.rowBudget.remaining());
        return (int) Math.max(1, Math.min(Math.min(byBytes, byRows), MAX_CHUNK));
    }

    /**
     * Returns the rejection of a batch allocation that the data node's scan memory limit refused. The coordinator retries a
     * rejected shard elsewhere.
     */
    static OpenSearchRejectedExecutionException rejection(OutOfMemoryException e) {
        final OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException(
            "native memory allocation rejected at [arrow-search-batches]: " + e.getMessage()
        );
        // Suppressed rather than the cause: concurrent segment search replaces a failure that has a cause and is not an
        // OpenSearchException with a query phase failure around that cause, which would turn the rejection into an internal error.
        rejection.addSuppressed(e);
        return rejection;
    }

    /**
     * Ends the slice once the query phase is over, and frees its compute slot. Each segment's rows were read when the segment
     * finished. The rows not sent stay in the state for the last messages of the stream.
     */
    void finish() {
        scoring = false;
        releaseComputeSlot();
    }

    /** Returns the rows of the slice that were not sent yet. */
    int rowsUnsent() {
        return rows;
    }

    /** Returns the heap that the slice's scratch buffers take. */
    long scratchBytes() {
        return scratch.heapBytes();
    }
}

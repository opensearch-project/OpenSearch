/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ExchangeSink;

/**
 * Unions a pre-pass's per-shard Bloom contributions as they arrive, holding exactly one bitset.
 *
 * <p>Replaces buffering the whole capture and merging afterwards, which is what made enabling the feature
 * trip the native memory breaker. The Arrow-IPC capture sink has to be given a byte budget up front, and
 * the only honest budget for "one fixed-size bitset per shard" is {@code bloomBytes × shards} — a
 * <em>reservation</em>, not an average. At an 18-shard table and a 32 MiB filter that is 1.15 GB per filter,
 * and a query planting three to five of them exhausted a ~9 GB pool before it had computed anything.
 * Measured on sf=100: with the feature enabled, 13 of 22 benchmark queries failed with
 * {@code CircuitBreakingException}; one of them (a shape that passes with the feature off) was a clean
 * regression traceable to this.
 *
 * <p>OR-ing incrementally removes the shard multiplier from the reservation entirely: the sink keeps one
 * bitset, copies each contribution's bytes into it, and releases the batch. It also removes an IPC
 * serialise/deserialise round trip that existed only so the coordinator could re-read what it had just
 * written.
 *
 * <p>Every rule the buffered version enforced is enforced here, for the same reasons. Nulls are skipped: a
 * shard whose build fragment matched no rows contributes nothing, which is not the same as contributing an
 * empty filter. A length disagreement is refused outright rather than truncated or padded, because two
 * differently sized SBBFs index blocks differently and combining them would report absent for keys that are
 * present — the one failure mode that loses rows.
 *
 * @opensearch.internal
 */
public final class RuntimeFilterMergeSink implements ExchangeSink {

    private static final Logger LOGGER = LogManager.getLogger(RuntimeFilterMergeSink.class);

    private final int filterId;

    /** Guarded by {@code this}: {@link #feed} may be called concurrently by shard response handlers. */
    private byte[] merged;
    private boolean poisoned;

    public RuntimeFilterMergeSink(int filterId) {
        this.filterId = filterId;
    }

    @Override
    public void feed(VectorSchemaRoot batch) {
        union(batch);
        // Closed only on normal return, which is what ExchangeSink's all-or-nothing ownership contract
        // requires: if this method throws, the caller still owns the batch and closes it. Closing in a
        // `finally` instead would release it here as well and give a double-release on the throwing path.
        // Nothing needs to be kept either way — the bytes are copied into the accumulator, so the batch
        // is released immediately rather than held for a later merge pass.
        batch.close();
    }

    private synchronized void union(VectorSchemaRoot batch) {
        if (poisoned) {
            return;
        }
        if (batch.getFieldVectors().isEmpty()) {
            return;
        }
        FieldVector vector = batch.getVector(0);
        if (!(vector instanceof VarBinaryVector bitsets)) {
            LOGGER.debug("[runtime-filter] pre-pass column 0 is {}, not a bitset; filter {} abandoned", vector.getMinorType(), filterId);
            poisoned = true;
            return;
        }
        for (int row = 0; row < batch.getRowCount(); row++) {
            if (bitsets.isNull(row)) {
                continue;
            }
            byte[] contribution = bitsets.get(row);
            if (merged == null) {
                merged = contribution;
            } else if (merged.length != contribution.length) {
                LOGGER.warn(
                    "[runtime-filter] pre-pass contributions for filter {} disagree on size ({} vs {} bytes); abandoned",
                    filterId,
                    merged.length,
                    contribution.length
                );
                poisoned = true;
                merged = null;
                return;
            } else {
                for (int i = 0; i < merged.length; i++) {
                    merged[i] |= contribution[i];
                }
            }
        }
    }

    /**
     * Abandons this filter: no payload will be produced, and any contribution arriving later is ignored.
     *
     * <p>Called when the pre-pass did not complete. A partial union is the one output of this class that is
     * <em>unsound rather than merely weak</em>: a Bloom missing an entire shard's keys reports absent for
     * keys that are present, so the probe predicate rejects rows that belong in the result. Every other
     * failure here degrades to "no filter", which keeps every row; this one silently loses them.
     *
     * <p>Note that the per-contribution checks cannot catch it. Each shard's contribution is individually
     * well formed and correctly sized — what is missing is a shard, not a byte — so the only place that
     * knows the union is incomplete is whoever observed the stage's terminal state.
     */
    public synchronized void invalidate() {
        poisoned = true;
        merged = null;
    }

    /**
     * The union, or {@code null} when there is nothing usable to install — no shard contributed, the
     * contributions could not be combined soundly, or the pre-pass was abandoned. A null payload leaves the
     * planted predicate unsatisfied, and an unsatisfied predicate keeps every row.
     */
    public synchronized byte[] mergedBitset() {
        return poisoned ? null : merged;
    }

    @Override
    public void close() {
        // Nothing to release: the accumulator is a plain byte array and every batch was closed on feed.
    }
}

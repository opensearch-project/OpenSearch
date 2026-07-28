/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

/**
 * Per-resolver counters for the docId&rarr;Parquet-row translation layer (the
 * {@code RowIdRemappingDocValues} resolver backed by {@code __row_id__}).
 *
 * <p>One instance is created per codec DocValues iterator (each builds its own resolver), so this is
 * single-threaded for the lifetime of one iterator &mdash; counters are plain {@code long}s with no
 * synchronization, mirroring {@link CacheStats}. It is registered once with the query-scoped
 * {@code QueryParquetStats} and summed live at end of query.
 *
 * <h2>Counts only &mdash; no per-call timing</h2>
 * Only <b>counts</b> are tracked here ({@link #lookups()} and the {@link #isIdentity() identity}
 * flag), never per-call time. {@code toRowId(docId)} runs once per document (up to 100M+ times per
 * query); the work per call is on the order of tens of nanoseconds, comparable to
 * {@code System.nanoTime()} itself, so code-timing each call cannot produce a trustworthy figure
 * (it inflates and, when extrapolated, exceeds the whole query time). The honest signal is the
 * exact lookup <b>count</b> here; the actual wall-clock spent in this layer is obtained from a CPU
 * flamegraph (async-profiler), which attributes real per-method time without instrumentation
 * overhead.
 */
public final class RowIdStats {

    /** True when this resolver is the no-op IDENTITY mapping (segment has docId == rowId). */
    private boolean identity;

    /** Number of {@code toRowId} calls that performed a {@code __row_id__} lookup. */
    private long lookups;

    /** Marks this resolver as the IDENTITY (no-op) mapping. */
    public void markIdentity() {
        identity = true;
    }

    /** Records one {@code __row_id__} lookup (called per document on a backed resolver). */
    public void recordLookup() {
        lookups++;
    }

    public boolean isIdentity() {
        return identity;
    }

    public long lookups() {
        return lookups;
    }

    /** True when this resolver did no work (not used / no lookups and not marked identity). */
    public boolean isEmpty() {
        return identity == false && lookups == 0;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Thrown by a backend's broadcast build-side capture sink when the accumulated build payload
 * exceeds the runtime cap the engine passed via
 * {@link ExchangeSinkProvider#createBroadcastCaptureSink(org.apache.arrow.memory.BufferAllocator, org.apache.calcite.rel.type.RelDataType, long)}
 * (sourced from {@code analytics.mpp.broadcast.max_bytes}).
 *
 * <p>This is a <b>cross-boundary contract type</b>, which is why it lives in the SPI rather than a
 * backend module: the engine sets the cap, the SPI carries it into the backend, the backend
 * enforces it, and the engine must recognize the breach to react. The coordinator's
 * {@code DefaultPlanExecutor} catches it (walking the cause chain) and re-plans the query with
 * broadcast made ineligible, so CBO falls back to hash-shuffle / coordinator-centric instead of
 * failing — pre-flight row estimates can under-count filter/semijoin selectivity, so a build CBO
 * judged small enough can still overflow at runtime.
 *
 * <p>Carries the observed and limit byte counts so callers can log an actionable message.
 * Operator remediations: raise {@code analytics.mpp.broadcast.max_bytes}, narrow the query, or set
 * {@code analytics.mpp.enabled=false}.
 *
 * @opensearch.internal
 */
public final class BroadcastSizeExceededException extends RuntimeException {

    private final long observedBytes;
    private final long limitBytes;

    /**
     * Tables scanned by the build side that overflowed, or empty when the thrower does not know them.
     *
     * <p>Carried so the broadcast→shuffle re-plan can suppress only the join whose build actually overflowed.
     * Without it the retry has to make broadcast ineligible for the WHOLE query, which also removes the
     * broadcasts that were fine — including, in a cascade, the bottom-level one that keeps a large fact scan in
     * place. Losing that one turns a modest overflow into a full shuffle of the largest table in the query.
     */
    private final java.util.Set<String> buildTables;

    public BroadcastSizeExceededException(long observed, long limit) {
        this(observed, limit, java.util.Set.of());
    }

    public BroadcastSizeExceededException(long observed, long limit, java.util.Set<String> buildTables) {
        super(
            "Broadcast build-side payload exceeded the configured limit "
                + "(observed="
                + observed
                + " bytes, limit="
                + limit
                + " bytes"
                + (buildTables.isEmpty() ? "" : ", build tables=" + buildTables)
                + "). "
                + "Raise analytics.mpp.broadcast.max_bytes, narrow the query, or set analytics.mpp.enabled=false."
        );
        this.observedBytes = observed;
        this.limitBytes = limit;
        this.buildTables = java.util.Set.copyOf(buildTables);
    }

    /** See {@link #buildTables}. Empty means "unknown", which callers must treat as "disable broadcast wholly". */
    public java.util.Set<String> buildTables() {
        return buildTables;
    }

    /** Bytes the build side actually accumulated before the cap tripped. */
    public long observedBytes() {
        return observedBytes;
    }

    /** The configured cap, in bytes. */
    public long limitBytes() {
        return limitBytes;
    }
}

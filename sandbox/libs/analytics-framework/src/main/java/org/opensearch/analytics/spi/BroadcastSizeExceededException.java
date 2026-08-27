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

    public BroadcastSizeExceededException(long observed, long limit) {
        super(
            "Broadcast build-side payload exceeded the configured limit "
                + "(observed="
                + observed
                + " bytes, limit="
                + limit
                + " bytes). "
                + "Raise analytics.mpp.broadcast.max_bytes, narrow the query, or set analytics.mpp.enabled=false."
        );
        this.observedBytes = observed;
        this.limitBytes = limit;
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

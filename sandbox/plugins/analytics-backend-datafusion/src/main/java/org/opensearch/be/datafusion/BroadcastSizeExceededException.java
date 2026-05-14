/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * Thrown by {@link BroadcastCaptureSink} when the accumulated build-side buffer size exceeds
 * the configured runtime cap (see {@code analytics.mpp.broadcast_max_bytes}). The dispatcher
 * surfaces this through the query's terminal listener so operators see a clear failure mode
 * rather than an out-of-memory cascade.
 *
 * <p>Operators have three remediations: raise the cap, narrow the query (better filters /
 * fewer columns on the build side), or flip {@code analytics.mpp.enabled=false} to revert
 * to coordinator-centric.
 *
 * @opensearch.internal
 */
public final class BroadcastSizeExceededException extends RuntimeException {

    public BroadcastSizeExceededException(long observed, long limit) {
        super(
            "Broadcast build-side payload exceeded the configured limit "
                + "(observed=" + observed + " bytes, limit=" + limit + " bytes). "
                + "Raise analytics.mpp.broadcast_max_bytes, narrow the query, or set analytics.mpp.enabled=false."
        );
    }
}

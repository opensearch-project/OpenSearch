/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import org.opensearch.search.SearchPhaseResult;

/**
 * How the shard stream of one shard ended on the coordinator. It never leaves the node.
 */
final class ArrowShardResult extends SearchPhaseResult {

    private final long rows;
    private final boolean truncated;
    private final boolean timedOut;
    private final boolean terminatedEarly;

    /**
     * Creates the result of one shard.
     *
     * @param rows            the rows relayed to the client
     * @param truncated       whether the search's row limit stopped the shard before it was exhausted
     * @param timedOut        whether the shard hit the search timeout
     * @param terminatedEarly whether the shard's row limit, from {@code size} or {@code terminate_after}, may have left matches out
     */
    ArrowShardResult(long rows, boolean truncated, boolean timedOut, boolean terminatedEarly) {
        this.rows = rows;
        this.truncated = truncated;
        this.timedOut = timedOut;
        this.terminatedEarly = terminatedEarly;
    }

    long rows() {
        return rows;
    }

    boolean truncated() {
        return truncated;
    }

    boolean timedOut() {
        return timedOut;
    }

    boolean terminatedEarly() {
        return terminatedEarly;
    }
}

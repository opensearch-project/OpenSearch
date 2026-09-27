/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.annotation.PublicApi;

/**
 * Coordinator-side events on the {@code latency_breakdown} timeline, distinct from the orchestrated
 * {@link SearchPhaseName} phases. The enum is the producer-side registry; the name string travels on the wire.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.10.0")
public enum CoordinatorLatencyEventName {
    /** Query rewrite (including any terms-lookup sub-queries), via Rewriteable.rewriteAndFetch. */
    QUERY_REWRITE("query_rewrite"),
    /** Index name/alias resolution and cluster-state lookup for the requested indices. */
    INDEX_RESOLUTION("index_resolution"),
    /** Coordinator work from the end of resolution to the start of the first search phase (shard routing + dispatch). */
    COORDINATOR_DISPATCH("coordinator_dispatch"),
    /** Coordinator reduce/coordinate time sitting between two adjacent phases (derived from phase offsets). */
    REDUCE_AND_COORDINATE("reduce_and_coordinate");

    private final String name;

    CoordinatorLatencyEventName(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}

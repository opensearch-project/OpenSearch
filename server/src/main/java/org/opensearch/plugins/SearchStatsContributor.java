/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.search.stats.SearchStats;

/**
 * Extension point for plugins that execute searches outside the standard Lucene path
 * (e.g. analytics engine) to contribute their query metrics into the node-level
 * {@link SearchStats} reported by {@code GET /_nodes/stats}.
 * <p>
 * The returned {@link SearchStats} is merged additively into the node's total search
 * stats before serialization, so existing tooling sees the counters in the standard
 * location without modification.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchStatsContributor {

    /**
     * Returns search stats to be merged into the node's aggregate search stats.
     * Called on each {@code _nodes/stats} request. Implementations should return
     * a snapshot of current counters; the caller handles null gracefully.
     *
     * @return search stats to contribute, or null if nothing to report
     */
    SearchStats contributeSearchStats();
}

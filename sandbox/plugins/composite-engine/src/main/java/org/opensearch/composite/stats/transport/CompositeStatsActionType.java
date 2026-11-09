/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.stats.transport;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.stats.CompositeShardStats;
import org.opensearch.composite.stats.CompositeStatsProvider;
import org.opensearch.plugin.stats.transport.FormatStatsActionType;

/**
 * Action type for the composite per-index stats endpoint
 * ({@code GET /_plugins/composite/{index}/_stats}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CompositeStatsActionType extends FormatStatsActionType<CompositeShardStats> {

    public static final CompositeStatsActionType INSTANCE = new CompositeStatsActionType();

    private CompositeStatsActionType() {
        super(CompositeStatsProvider.FORMAT_NAME, CompositeShardStats::new);
    }
}

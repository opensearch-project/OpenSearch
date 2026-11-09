/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.stats.transport;

import org.opensearch.be.lucene.stats.LuceneShardStats;
import org.opensearch.be.lucene.stats.LuceneStatsProvider;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.plugin.stats.transport.FormatNodeStatsActionType;

/**
 * Action type for the lucene per-node stats endpoint
 * ({@code GET /_plugins/lucene/_nodes/[{nodeId}/]_stats}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneNodeStatsActionType extends FormatNodeStatsActionType<LuceneShardStats> {

    public static final LuceneNodeStatsActionType INSTANCE = new LuceneNodeStatsActionType();

    private LuceneNodeStatsActionType() {
        super(LuceneStatsProvider.FORMAT_NAME, LuceneShardStats::new);
    }
}

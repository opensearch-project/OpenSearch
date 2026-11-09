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
import org.opensearch.plugin.stats.transport.FormatStatsActionType;

/**
 * Action type for the lucene per-index stats endpoint
 * ({@code GET /_plugins/lucene/{index}/_stats}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneStatsActionType extends FormatStatsActionType<LuceneShardStats> {

    public static final LuceneStatsActionType INSTANCE = new LuceneStatsActionType();

    private LuceneStatsActionType() {
        super(LuceneStatsProvider.FORMAT_NAME, LuceneShardStats::new);
    }
}

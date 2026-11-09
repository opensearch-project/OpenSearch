/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats.transport;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.parquet.stats.ParquetShardStats;
import org.opensearch.parquet.stats.ParquetStatsProvider;
import org.opensearch.plugin.stats.transport.FormatNodeStatsActionType;

/**
 * Action type for the parquet per-node stats endpoint
 * ({@code GET /_plugins/parquet/_nodes/[{nodeId}/]_stats}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParquetNodeStatsActionType extends FormatNodeStatsActionType<ParquetShardStats> {

    public static final ParquetNodeStatsActionType INSTANCE = new ParquetNodeStatsActionType();

    private ParquetNodeStatsActionType() {
        super(ParquetStatsProvider.FORMAT_NAME, ParquetShardStats::new);
    }
}

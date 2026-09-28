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
import org.opensearch.plugin.stats.transport.FormatStatsActionType;

/**
 * Action type for the parquet per-index stats endpoint
 * ({@code GET /_plugins/parquet/{index}/_stats}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParquetStatsActionType extends FormatStatsActionType<ParquetShardStats> {

    public static final ParquetStatsActionType INSTANCE = new ParquetStatsActionType();

    private ParquetStatsActionType() {
        super(ParquetStatsProvider.FORMAT_NAME, ParquetShardStats::new);
    }
}

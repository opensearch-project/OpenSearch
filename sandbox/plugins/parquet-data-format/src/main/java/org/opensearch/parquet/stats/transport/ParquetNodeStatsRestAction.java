/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats.transport;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.parquet.stats.ParquetStatsProvider;
import org.opensearch.plugin.stats.transport.BaseFormatNodeStatsRestAction;
import org.opensearch.plugin.stats.transport.FormatNodeStatsResponse;

/**
 * REST handler for {@code GET /_plugins/parquet/_nodes/[{nodeId}/]_stats}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParquetNodeStatsRestAction extends BaseFormatNodeStatsRestAction {

    @Override
    protected String formatName() {
        return ParquetStatsProvider.FORMAT_NAME;
    }

    @Override
    protected ActionType<? extends FormatNodeStatsResponse<?>> actionType() {
        return ParquetNodeStatsActionType.INSTANCE;
    }
}

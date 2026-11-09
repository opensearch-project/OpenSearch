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
import org.opensearch.plugin.stats.transport.BaseFormatStatsRestAction;
import org.opensearch.plugin.stats.transport.FormatStatsResponse;

/**
 * REST handler for {@code GET /_plugins/parquet/{index}/_stats}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParquetStatsRestAction extends BaseFormatStatsRestAction {

    @Override
    protected String formatName() {
        return ParquetStatsProvider.FORMAT_NAME;
    }

    @Override
    protected ActionType<? extends FormatStatsResponse<?>> actionType() {
        return ParquetStatsActionType.INSTANCE;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.stats.transport;

import org.opensearch.action.ActionType;
import org.opensearch.be.lucene.stats.LuceneStatsProvider;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.plugin.stats.transport.BaseFormatStatsRestAction;
import org.opensearch.plugin.stats.transport.FormatStatsResponse;

/**
 * REST handler for {@code GET /_plugins/lucene/{index}/_stats}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneStatsRestAction extends BaseFormatStatsRestAction {

    @Override
    protected String formatName() {
        return LuceneStatsProvider.FORMAT_NAME;
    }

    @Override
    protected ActionType<? extends FormatStatsResponse<?>> actionType() {
        return LuceneStatsActionType.INSTANCE;
    }
}

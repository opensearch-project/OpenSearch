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
import org.opensearch.plugin.stats.transport.BaseFormatNodeStatsRestAction;
import org.opensearch.plugin.stats.transport.FormatNodeStatsResponse;

/**
 * REST handler for {@code GET /_plugins/lucene/_nodes/[{nodeId}/]_stats}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneNodeStatsRestAction extends BaseFormatNodeStatsRestAction {

    @Override
    protected String formatName() {
        return LuceneStatsProvider.FORMAT_NAME;
    }

    @Override
    protected ActionType<? extends FormatNodeStatsResponse<?>> actionType() {
        return LuceneNodeStatsActionType.INSTANCE;
    }
}

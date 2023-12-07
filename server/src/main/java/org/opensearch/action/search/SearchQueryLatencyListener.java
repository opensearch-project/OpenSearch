/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.xcontent.ToXContent;

import java.util.Collections;
import java.util.HashMap;

/**
 * The listener for top N queries by latency
 *
 * @opensearch.internal
 */
public final class SearchQueryLatencyListener extends SearchRequestOperationsListener {
    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));
    private final QueryLatencyInsightService queryLatencyInsightService;

    @Inject
    public SearchQueryLatencyListener(QueryLatencyInsightService queryLatencyInsightService) {
        this.queryLatencyInsightService = queryLatencyInsightService;
    }

    @Override
    public void onPhaseStart(SearchPhaseContext context) {}

    @Override
    public void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

    @Override
    public void onPhaseFailure(SearchPhaseContext context) {}

    @Override
    public void onRequestStart(SearchRequestContext searchRequestContext) {}

    @Override
    public void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        SearchRequest request = context.getRequest();
        queryLatencyInsightService.ingestQueryData(
            request.getOrCreateAbsoluteStartMillis(),
            request.searchType(),
            request.source().toString(FORMAT_PARAMS),
            context.getNumShards(),
            request.indices(),
            new HashMap<>(),
            searchRequestContext.phaseTookMap()
        );
    }
}

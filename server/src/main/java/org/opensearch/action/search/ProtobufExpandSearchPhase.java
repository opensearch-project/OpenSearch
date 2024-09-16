/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.InternalSearchResponse;

/**
 * This search phase is an optional phase that will be executed once all hits are fetched from the shards that executes
 * field-collapsing on the inner hits. This phase only executes if field collapsing is requested in the search request and otherwise
 * forwards to the next phase immediately.
 *
 * @opensearch.internal
 */
final class ProtobufExpandSearchPhase extends SearchPhase {
    private final SearchPhaseContext context;
    private final InternalSearchResponse searchResponse;
    private final AtomicArray<SearchPhaseResult> queryResults;

    ProtobufExpandSearchPhase(SearchPhaseContext context, InternalSearchResponse searchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        super(SearchPhaseName.EXPAND.getName());
        this.context = context;
        this.searchResponse = searchResponse;
        this.queryResults = queryResults;
    }

    @Override
    public void run() {
        context.sendSearchResponse(searchResponse, queryResults);
    }
}

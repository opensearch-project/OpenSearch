/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.labels;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.threadpool.ThreadPool;

/**
 * SearchRequestOperationsListener subscriber for labeling search requests
 *
 * @opensearch.internal
 */
public final class SearchRequestLabelingListener extends SearchRequestOperationsListener {
    final private ThreadPool threadPool;
    final private RuleBasedLabelingService ruleBasedLabelingService;

    public SearchRequestLabelingListener(final ThreadPool threadPool, final RuleBasedLabelingService ruleBasedLabelingService) {
        this.threadPool = threadPool;
        this.ruleBasedLabelingService = ruleBasedLabelingService;
    }

    @Override
    public void onRequestStart(SearchRequestContext searchRequestContext) {
        // add tags to search request
        ruleBasedLabelingService.applyAllRules(threadPool.getThreadContext(), searchRequestContext.getRequest());
    }

    @Override
    public void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}
}

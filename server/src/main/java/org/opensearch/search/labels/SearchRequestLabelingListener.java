/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.labels;

import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;

/**
 * SearchRequestOperationsListener subscriber for labeling search requests
 *
 * @opensearch.internal
 */
public final class SearchRequestLabelingListener extends SearchRequestOperationsListener {
    final private RequestLabelingService requestLabelingService;

    public SearchRequestLabelingListener(final RequestLabelingService requestLabelingService) {
        this.requestLabelingService = requestLabelingService;
    }

    @Override
    public void onRequestStart(SearchRequestContext searchRequestContext) {
        // add tags to search request
        requestLabelingService.applyAllRules(searchRequestContext.getRequest());
        requestLabelingService.parseUserLabels();
    }
}

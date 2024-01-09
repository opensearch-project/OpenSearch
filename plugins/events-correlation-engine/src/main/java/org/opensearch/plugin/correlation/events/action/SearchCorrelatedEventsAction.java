/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.action;

import org.opensearch.action.ActionType;

/**
 * Transport Action for searching correlated events
 *
 * @opensearch.internal
 */
public class SearchCorrelatedEventsAction extends ActionType<SearchCorrelatedEventsResponse> {

    /**
     * Instance of SearchCorrelatedEventsAction
     */
    public static final SearchCorrelatedEventsAction INSTANCE = new SearchCorrelatedEventsAction();
    /**
     * Name of SearchCorrelatedEventsAction
     */
    public static final String NAME = "cluster:admin/search/correlation/events";

    private SearchCorrelatedEventsAction() {
        super(NAME, SearchCorrelatedEventsResponse::new);
    }
}

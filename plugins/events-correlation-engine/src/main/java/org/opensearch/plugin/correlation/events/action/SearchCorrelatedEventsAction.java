/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.action;

import org.opensearch.action.ActionType;

public class SearchCorrelatedEventsAction extends ActionType<SearchCorrelatedEventsResponse> {

    public static final SearchCorrelatedEventsAction INSTANCE = new SearchCorrelatedEventsAction();
    public static final String NAME = "cluster:admin/correlation/events/search";

    private SearchCorrelatedEventsAction() {
        super(NAME, SearchCorrelatedEventsResponse::new);
    }
}

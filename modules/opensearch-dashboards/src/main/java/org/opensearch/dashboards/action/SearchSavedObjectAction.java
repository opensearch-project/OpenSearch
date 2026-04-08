/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchResponse;

/**
 * Action for searching saved objects.
 */
public class SearchSavedObjectAction extends ActionType<SearchResponse> {

    public static final SearchSavedObjectAction INSTANCE = new SearchSavedObjectAction();
    public static final String NAME = "osd:saved_object/search";

    private SearchSavedObjectAction() {
        super(NAME, SearchResponse::new);
    }
}

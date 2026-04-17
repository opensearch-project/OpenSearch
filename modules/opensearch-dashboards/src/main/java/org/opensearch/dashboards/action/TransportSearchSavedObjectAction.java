/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dashboards.DashboardsPluginClient;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportSearchSavedObjectAction extends HandledTransportAction<SearchSavedObjectRequest, SearchResponse> {

    private static final Logger log = LogManager.getLogger(TransportSearchSavedObjectAction.class);

    private final DashboardsPluginClient client;

    @Inject
    public TransportSearchSavedObjectAction(TransportService transportService, ActionFilters actionFilters, DashboardsPluginClient client) {
        super(SearchSavedObjectAction.NAME, transportService, actionFilters, SearchSavedObjectRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, SearchSavedObjectRequest request, ActionListener<SearchResponse> listener) {
        SearchRequest searchRequest = new SearchRequest(new String[] { request.getIndex() }, request.getSource());

        client.search(searchRequest, ActionListener.wrap(listener::onResponse, listener::onFailure));
    }
}

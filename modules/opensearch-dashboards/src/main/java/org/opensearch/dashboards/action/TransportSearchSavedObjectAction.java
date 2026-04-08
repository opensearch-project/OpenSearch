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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action for searching saved objects within a dashboards index.
 */
public class TransportSearchSavedObjectAction extends HandledTransportAction<SearchSavedObjectRequest, SearchResponse> {

    private static final Logger log = LogManager.getLogger(TransportSearchSavedObjectAction.class);

    private final Client client;

    @Inject
    public TransportSearchSavedObjectAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(SearchSavedObjectAction.NAME, transportService, actionFilters, SearchSavedObjectRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, SearchSavedObjectRequest request, ActionListener<SearchResponse> listener) {
        final ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashContext();

        SearchRequest searchRequest = new SearchRequest(new String[] { request.getIndex() }, request.getSource());

        client.search(
            searchRequest,
            ActionListener.runBefore(ActionListener.wrap(listener::onResponse, listener::onFailure), ctx::restore)
        );
    }
}

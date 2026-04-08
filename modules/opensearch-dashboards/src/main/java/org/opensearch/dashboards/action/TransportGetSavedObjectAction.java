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
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.Map;

/**
 * Transport action for getting a single saved object.
 */
public class TransportGetSavedObjectAction extends HandledTransportAction<GetSavedObjectRequest, SavedObjectResponse> {

    private static final Logger log = LogManager.getLogger(TransportGetSavedObjectAction.class);

    private final Client client;

    @Inject
    public TransportGetSavedObjectAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetSavedObjectAction.NAME, transportService, actionFilters, GetSavedObjectRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetSavedObjectRequest request, ActionListener<SavedObjectResponse> listener) {
        final ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashContext();
        GetRequest getRequest = new GetRequest(request.getIndex(), request.getDocumentId());

        client.get(getRequest, ActionListener.runBefore(ActionListener.wrap(getResponse -> {
            if (getResponse.isExists()) {
                Map<String, Object> source = getResponse.getSourceAsMap();
                listener.onResponse(new SavedObjectResponse(source));
            } else {
                listener.onFailure(new OpenSearchStatusException("Saved object not found", RestStatus.NOT_FOUND));
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                listener.onFailure(new OpenSearchStatusException("Saved object not found", RestStatus.NOT_FOUND));
            } else {
                listener.onFailure(e);
            }
        }), ctx::restore));
    }
}

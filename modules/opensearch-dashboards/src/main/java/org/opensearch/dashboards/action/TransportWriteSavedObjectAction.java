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
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action for creating or updating a saved object.
 */
public class TransportWriteSavedObjectAction extends HandledTransportAction<WriteSavedObjectRequest, SavedObjectResponse> {

    private static final Logger log = LogManager.getLogger(TransportWriteSavedObjectAction.class);

    private final Client client;

    @Inject
    public TransportWriteSavedObjectAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(WriteSavedObjectAction.NAME, transportService, actionFilters, WriteSavedObjectRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, WriteSavedObjectRequest request, ActionListener<SavedObjectResponse> listener) {
        final ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashContext();

        IndexRequest indexRequest = new IndexRequest(request.getIndex()).id(request.getDocumentId()).source(request.getDocument());
        if (request.isCreateOperation()) {
            indexRequest.create(true);
        }

        client.index(
            indexRequest,
            ActionListener.runBefore(
                ActionListener.wrap(
                    indexResponse -> listener.onResponse(new SavedObjectResponse(request.getDocument())),
                    listener::onFailure
                ),
                ctx::restore
            )
        );
    }
}

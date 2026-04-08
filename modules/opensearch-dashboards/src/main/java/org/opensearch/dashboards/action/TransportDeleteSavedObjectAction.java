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
import org.opensearch.action.delete.DeleteRequest;
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
 * Transport action for deleting a saved object.
 */
public class TransportDeleteSavedObjectAction extends HandledTransportAction<DeleteSavedObjectRequest, SavedObjectResponse> {

    private static final Logger log = LogManager.getLogger(TransportDeleteSavedObjectAction.class);

    private final Client client;

    @Inject
    public TransportDeleteSavedObjectAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(DeleteSavedObjectAction.NAME, transportService, actionFilters, DeleteSavedObjectRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, DeleteSavedObjectRequest request, ActionListener<SavedObjectResponse> listener) {
        final ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashContext();
        DeleteRequest deleteRequest = new DeleteRequest(request.getIndex(), request.getDocumentId());

        client.delete(deleteRequest, ActionListener.runBefore(ActionListener.wrap(deleteResponse -> {
            if (deleteResponse.getResult() == org.opensearch.action.DocWriteResponse.Result.NOT_FOUND) {
                listener.onFailure(new OpenSearchStatusException("Saved object not found", RestStatus.NOT_FOUND));
            } else {
                listener.onResponse(new SavedObjectResponse(Map.of("deleted", true, "id", request.getDocumentId())));
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

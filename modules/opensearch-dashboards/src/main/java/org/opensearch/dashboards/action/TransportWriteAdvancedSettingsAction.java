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

public class TransportWriteAdvancedSettingsAction extends HandledTransportAction<WriteAdvancedSettingsRequest, AdvancedSettingsResponse> {

    private static final Logger log = LogManager.getLogger(TransportWriteAdvancedSettingsAction.class);

    private final Client client;

    @Inject
    public TransportWriteAdvancedSettingsAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(WriteAdvancedSettingsAction.NAME, transportService, actionFilters, WriteAdvancedSettingsRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, WriteAdvancedSettingsRequest request, ActionListener<AdvancedSettingsResponse> listener) {
        final ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashContext();

        IndexRequest indexRequest = new IndexRequest(request.getIndex()).id(request.getDocumentId()).source(request.getDocument());
        if (request.isCreateOperation()) {
            indexRequest.create(true);
        }

        client.index(
            indexRequest,
            ActionListener.runBefore(
                ActionListener.wrap(
                    indexResponse -> listener.onResponse(new AdvancedSettingsResponse(request.getDocument())),
                    listener::onFailure
                ),
                ctx::restore
            )
        );
    }
}

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
import org.opensearch.core.action.ActionListener;
import org.opensearch.dashboards.DashboardsPluginClient;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportWriteAdvancedSettingsAction extends HandledTransportAction<WriteAdvancedSettingsRequest, AdvancedSettingsResponse> {

    private static final Logger log = LogManager.getLogger(TransportWriteAdvancedSettingsAction.class);

    private final DashboardsPluginClient client;

    @Inject
    public TransportWriteAdvancedSettingsAction(TransportService transportService, ActionFilters actionFilters, DashboardsPluginClient client) {
        super(WriteAdvancedSettingsAction.NAME, transportService, actionFilters, WriteAdvancedSettingsRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, WriteAdvancedSettingsRequest request, ActionListener<AdvancedSettingsResponse> listener) {
        IndexRequest indexRequest = new IndexRequest(request.getIndex()).id(request.getDocumentId()).source(request.getDocument());
        if (request.isCreateOperation()) {
            indexRequest.create(true);
        }

        client.index(indexRequest, ActionListener.wrap(
            indexResponse -> listener.onResponse(new AdvancedSettingsResponse(request.getDocument())),
            listener::onFailure
        ));
    }
}

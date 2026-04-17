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
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dashboards.DashboardsPluginClient;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetSavedObjectAction extends HandledTransportAction<GetSavedObjectRequest, GetResponse> {

    private static final Logger log = LogManager.getLogger(TransportGetSavedObjectAction.class);

    private final DashboardsPluginClient client;

    @Inject
    public TransportGetSavedObjectAction(TransportService transportService, ActionFilters actionFilters, DashboardsPluginClient client) {
        super(GetSavedObjectAction.NAME, transportService, actionFilters, GetSavedObjectRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetSavedObjectRequest request, ActionListener<GetResponse> listener) {
        GetRequest getRequest = new GetRequest(request.getIndex(), request.getDocumentId());
        client.get(getRequest, listener);
    }
}

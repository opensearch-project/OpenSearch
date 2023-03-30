/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsAction;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsRequest;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportSearchCorrelationAction extends HandledTransportAction<
    SearchCorrelatedEventsRequest,
    SearchCorrelatedEventsResponse> {

    public TransportSearchCorrelationAction(TransportService transportService, Client client, ActionFilters actionFilters) {
        super(SearchCorrelatedEventsAction.NAME, transportService, actionFilters, SearchCorrelatedEventsRequest::new);
    }

    @Override
    protected void doExecute(Task task, SearchCorrelatedEventsRequest request, ActionListener<SearchCorrelatedEventsResponse> listener) {

    }
}

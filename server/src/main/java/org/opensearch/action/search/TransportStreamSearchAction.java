/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Provider;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Minimal handler so StreamSearchAction is resolvable.
 * Registers under StreamSearchAction.NAME and reuses core search execution
 * by invoking TransportSearchAction directly (no client delegation).
 */
public class TransportStreamSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    private final Provider<TransportSearchAction> transportSearchActionProvider;

    @Inject
    public TransportStreamSearchAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Provider<TransportSearchAction> transportSearchActionProvider
    ) {
        super(StreamSearchAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.transportSearchActionProvider = transportSearchActionProvider;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        transportSearchActionProvider.get().doExecute(task, request, listener);
    }
}

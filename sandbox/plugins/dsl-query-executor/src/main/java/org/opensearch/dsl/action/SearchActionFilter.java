/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Intercepts all {@code _search} requests and dispatches them to {@link DslExecuteAction}
 * for execution through the Calcite pipeline. Non-search actions pass through unchanged.
 */
public class SearchActionFilter implements ActionFilter {

    private final NodeClient client;

    /**
     * Creates a filter that dispatches intercepted searches via the given client.
     *
     * @param client node client for dispatching to {@link DslExecuteAction}
     */
    public SearchActionFilter(NodeClient client) {
        this.client = client;
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (SearchAction.NAME.equals(action)) {
            SearchRequest searchRequest = (SearchRequest) request;
            client.execute(DslExecuteAction.INSTANCE, searchRequest, (ActionListener<SearchResponse>) listener);
        } else {
            chain.proceed(task, action, request, listener);
        }
    }
}

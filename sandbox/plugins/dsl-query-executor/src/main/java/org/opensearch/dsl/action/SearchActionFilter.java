/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryResponse;
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
 * Intercepts all {@code _search} requests (dispatched to {@link DslExecuteAction}) and
 * {@code _validate/query} requests (dispatched to {@link DslValidateAction}) for handling
 * through the Calcite pipeline. Other actions pass through unchanged.
 */
public class SearchActionFilter implements ActionFilter {

    /** Runs after the Security plugin's authorization filter (order 0). */
    static final int FILTER_ORDER = 1;

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
        return FILTER_ORDER;
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
        // TODO: add support for other search-related APIs (_msearch, _count, _search_shards, etc.).
        // Consider two categories: APIs that execute search vs APIs that only explain/validate.
        if (SearchAction.NAME.equals(action)) {
            SearchRequest searchRequest = (SearchRequest) request;
            client.execute(DslExecuteAction.INSTANCE, searchRequest, (ActionListener<SearchResponse>) listener);
        } else if (ValidateQueryAction.NAME.equals(action)) {
            // Vanilla validate checks Lucene parseability; on this path validity means
            // "the DSL converter accepts it" — route to Calcite-aware validation.
            ValidateQueryRequest validateRequest = (ValidateQueryRequest) request;
            client.execute(DslValidateAction.INSTANCE, validateRequest, (ActionListener<ValidateQueryResponse>) listener);
        } else {
            chain.proceed(task, action, request, listener);
        }
    }
}

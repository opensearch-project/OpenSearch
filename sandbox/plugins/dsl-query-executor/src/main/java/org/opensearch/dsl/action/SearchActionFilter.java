/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Intercepts {@code _search} requests (dispatched to {@link ExecuteAction}) and
 * {@code _validate/query} requests (dispatched to {@link ValidateAction}) when every
 * resolved target uses a pluggable data format. Other requests pass through unchanged.
 */
public class SearchActionFilter implements ActionFilter {

    /** Runs after the Security plugin's authorization filter (order 0). */
    static final int FILTER_ORDER = 1;

    private final NodeClient client;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * Creates a filter that dispatches intercepted searches via the given client.
     *
     * @param client node client for dispatching to {@link ExecuteAction}
     * @param clusterService cluster service used to inspect resolved index settings
     * @param indexNameExpressionResolver resolves aliases and wildcard expressions
     */
    public SearchActionFilter(NodeClient client, ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.client = client;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
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
            if (targetsOnlyPluggableDataFormatIndices(searchRequest)) {
                client.execute(ExecuteAction.INSTANCE, searchRequest, (ActionListener<SearchResponse>) listener);
            } else {
                chain.proceed(task, action, request, listener);
            }
        } else if (ValidateQueryAction.NAME.equals(action)) {
            // Cast is safe: the action name identifies the request/listener types.
            ValidateQueryRequest validateRequest = (ValidateQueryRequest) request;
            if (targetsOnlyPluggableDataFormatIndices(validateRequest)) {
                client.execute(ValidateAction.INSTANCE, validateRequest, (ActionListener<ValidateQueryResponse>) listener);
            } else {
                chain.proceed(task, action, request, listener);
            }
        } else {
            chain.proceed(task, action, request, listener);
        }
    }

    private boolean targetsOnlyPluggableDataFormatIndices(IndicesRequest request) {
        ClusterState state = clusterService.state();
        final Index[] concreteIndices;
        try {
            concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        } catch (RuntimeException e) {
            // Preserve native search error handling for missing or invalid index expressions.
            return false;
        }

        if (concreteIndices.length == 0) {
            return false;
        }
        for (Index index : concreteIndices) {
            IndexMetadata metadata = state.metadata().index(index);
            if (metadata == null || IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.get(metadata.getSettings()) == false) {
                return false;
            }
        }
        return true;
    }
}

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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Intercepts all {@code _search} requests and dispatches them to {@link DslExecuteAction}
 * for execution through the Calcite pipeline. Non-search actions pass through unchanged.
 * Requests targeting indices with {@code index.pluggable.dataformat.enabled} bypass this filter.
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
     * @param client node client for dispatching to {@link DslExecuteAction}
     * @param clusterService cluster service for resolving index metadata
     * @param indexNameExpressionResolver resolves index expressions to concrete indices
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

            // Resolve index expressions (wildcards, aliases) to concrete indices and check
            // how many have pluggable dataformat enabled via index.pluggable.dataformat.enabled.
            Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(clusterService.state(), searchRequest);
            int pluggableCount = countPluggableDataFormatIndices(concreteIndices);

            // Mixed indices (some pluggable, some not) are unsupported — reject the request.
            if (pluggableCount > 0 && pluggableCount < concreteIndices.length) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "Search request targets a mix of indices with and without pluggable dataformat enabled. "
                            + "All target indices must consistently enable or disable the setting."
                    )
                );
                return;
            }

            if (pluggableCount == concreteIndices.length) {
                // All indices use pluggable dataformat — bypass DSL plugin, use normal Lucene search path.
                chain.proceed(task, action, request, listener);
            } else {
                // No pluggable dataformat indices — route through DSL/Calcite analytics engine.
                client.execute(DslExecuteAction.INSTANCE, searchRequest, (ActionListener<SearchResponse>) listener);
            }
        } else {
            // Non-search actions pass through unchanged.
            chain.proceed(task, action, request, listener);
        }
    }

    private int countPluggableDataFormatIndices(Index[] concreteIndices) {
        int count = 0;
        for (Index index : concreteIndices) {
            IndexMetadata indexMetadata = clusterService.state().metadata().index(index);
            if (indexMetadata != null && Mapper.isPluggableDataFormatEnabled(indexMetadata.getSettings())) {
                count++;
            }
        }
        return count;
    }
}

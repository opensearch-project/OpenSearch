/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
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

/**
 * Intercepts {@code _search} requests targeting indices with {@code index.pluggable.dataformat.enabled}
 * and rejects them with an error directing users to PPL instead.
 */
public class SearchActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(SearchActionFilter.class);

    /** Runs after the Security plugin's authorization filter (order 0). */
    static final int FILTER_ORDER = 1;

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public SearchActionFilter(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public int order() {
        return FILTER_ORDER;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        // TODO: Add support for other search-related APIs (_msearch, _count, _search_shards, etc.).
        // Consider two categories: APIs that execute search vs APIs that only explain/validate.
        if (SearchAction.NAME.equals(action)) {
            SearchRequest searchRequest = (SearchRequest) request;
            ClusterState state = clusterService.state();
            Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, searchRequest);

            if (hasPluggableDataFormatIndex(state, concreteIndices)) {
                logger.debug("Rejecting DSL search: at least one target index has pluggable dataformat enabled");
                // TODO: Route to DSL/Calcite engine once supported:
                // client.execute(DslExecuteAction.INSTANCE, searchRequest, (ActionListener<SearchResponse>) listener);
                listener.onFailure(
                    new UnsupportedOperationException("DSL is not supported with Optimized Engine currently. Please use PPL or SQL for search.")
                );
                return;
            }
        }
        chain.proceed(task, action, request, listener);
    }

    private boolean hasPluggableDataFormatIndex(ClusterState state, Index[] concreteIndices) {
        for (Index index : concreteIndices) {
            IndexMetadata indexMetadata = state.metadata().index(index);
            if (indexMetadata != null && IndexSettings.isPluggableDataFormatEnabled(indexMetadata.getSettings())) {
                return true;
            }
        }
        return false;
    }
}

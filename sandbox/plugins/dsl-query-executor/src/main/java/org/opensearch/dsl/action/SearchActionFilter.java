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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.dsl.DslQueryExecutorSettings;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.router.DslCalciteGrammar;
import org.opensearch.dsl.router.RouteDecision;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Intercepts {@code _search} requests and, when Calcite routing is enabled, decides between
 * the Calcite path and the codec path.
 *
 * <p>Layer 1: {@link DslQueryExecutorSettings#CALCITE_ENABLED} — defaults to {@code true};
 * setting it to {@code false} forces every request through the codec path unchanged
 * (operational escape hatch).
 *
 * <p>Layer 2 (only when the setting is on):
 * <ul>
 *   <li>{@link DslCalciteGrammar#validate} classifies the request. Grammar-rejected requests
 *       fall back to the codec path.</li>
 *   <li>Grammar-accepted requests are dispatched to {@link DslExecuteAction}. A
 *       {@link ConversionException} during execution triggers codec fallback (schema-side
 *       checks the grammar can't run may still fail at conversion time). Non-conversion
 *       errors surface to the caller unchanged.</li>
 * </ul>
 */
public class SearchActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(SearchActionFilter.class);

    /** Runs after the Security plugin's authorization filter (order 0). */
    static final int FILTER_ORDER = 1;

    private final NodeClient client;
    private final DslCalciteGrammar grammar;
    /**
     * Kept in sync with {@link DslQueryExecutorSettings#CALCITE_ENABLED} via a
     * cluster-settings update consumer, so a {@code PUT _cluster/settings} change propagates
     * without any per-request settings lookup. {@code volatile} because updates land on the
     * cluster-state-applier thread while reads happen on transport/search threads.
     */
    private volatile boolean calciteEnabled;

    /**
     * @param client node client for dispatching to {@link DslExecuteAction}
     * @param clusterService source of the dynamic {@code CALCITE_ENABLED} setting value
     * @param grammar route-decision oracle
     */
    public SearchActionFilter(NodeClient client, ClusterService clusterService, DslCalciteGrammar grammar) {
        this.client = client;
        this.grammar = grammar;
        this.calciteEnabled = DslQueryExecutorSettings.CALCITE_ENABLED.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DslQueryExecutorSettings.CALCITE_ENABLED, v -> this.calciteEnabled = v);
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
        if (!calciteEnabled || !SearchAction.NAME.equals(action)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        SearchRequest searchRequest = (SearchRequest) request;
        RouteDecision decision = grammar.validate(searchRequest.source());

        if (!decision.supported()) {
            logger.debug("Grammar rejected _search, falling back to codec: {}", decision.rejectionReasons());
            chain.proceed(task, action, request, listener);
            return;
        }

        // Explicit listener rather than ActionListener.wrap: wrap re-routes any exception thrown
        // while delivering the success response into onFailure, which would complete the caller's
        // listener twice (onResponse then onFailure) and could even trigger a codec re-run. Here
        // onResponse forwards directly, so a response-delivery hiccup can never masquerade as a
        // Calcite failure — only a genuine Calcite failure reaches onFailure.
        ActionListener<SearchResponse> calciteListener = new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse response) {
                ((ActionListener<SearchResponse>) listener).onResponse(response);
            }

            @Override
            public void onFailure(Exception error) {
                if (isFallbackable(error) == false) {
                    listener.onFailure(error);
                    return;
                }
                logger.debug("Calcite path threw ConversionException, falling back to codec", error);
                chain.proceed(task, action, request, listener);
            }
        };
        client.execute(DslExecuteAction.INSTANCE, searchRequest, calciteListener);
    }

    /**
     * Only fall back on failures that mean "Calcite can't handle this request" — grammar
     * gaps or schema mismatches the grammar can't see. Runtime engine errors, timeouts,
     * and other operational failures must surface to the caller so they can be diagnosed.
     *
     * <p>Uses an identity-based seen set to defend against pathological self-referential
     * or cyclic exception chains (e.g. legacy code that calls {@code initCause(this)}).
     */
    private static boolean isFallbackable(Throwable e) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = e; t != null && seen.add(t); t = t.getCause()) {
            if (t instanceof ConversionException) return true;
        }
        return false;
    }
}

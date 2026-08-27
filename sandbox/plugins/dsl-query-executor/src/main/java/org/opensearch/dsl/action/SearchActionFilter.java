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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.dsl.DslQueryExecutorSettings;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.router.DslCalciteGrammar;
import org.opensearch.dsl.router.RouteDecision;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Intercepts {@code _search} and routes it to the Calcite path or the codec path.
 *
 * <p>Master switch {@link DslQueryExecutorSettings#CALCITE_ENABLED} (default true) sends everything
 * to codec when off; {@link DslQueryExecutorSettings#CALCITE_QUERY_ENABLED} and
 * {@link DslQueryExecutorSettings#CALCITE_AGGREGATION_ENABLED} independently send just the
 * hits/search or aggregation category to codec.
 *
 * <p>Otherwise {@link DslCalciteGrammar#validate} decides: rejected requests fall back to codec;
 * accepted ones go to {@link DslExecuteAction}, and a {@link ConversionException} there (a
 * schema-dependent check the grammar cannot run) also falls back. Other errors surface to the caller.
 */
public class SearchActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(SearchActionFilter.class);

    /** Runs after the Security plugin's authorization filter (order 0). */
    static final int FILTER_ORDER = 1;

    private final NodeClient client;
    private final DslCalciteGrammar grammar;
    /**
     * Routing switches, kept live via cluster-settings update consumers (no per-request lookup).
     * {@code volatile}: updates land on the cluster-state-applier thread, reads on transport threads.
     */
    private volatile boolean calciteEnabled;
    private volatile boolean queryEnabledOnCalcite;
    private volatile boolean aggregationEnabledOnCalcite;

    /**
     * @param client node client for dispatching to {@link DslExecuteAction}
     * @param clusterService source of the dynamic routing settings
     * @param grammar route-decision oracle
     */
    public SearchActionFilter(NodeClient client, ClusterService clusterService, DslCalciteGrammar grammar) {
        this.client = client;
        this.grammar = grammar;

        Settings settings = clusterService.getSettings();
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        this.calciteEnabled = DslQueryExecutorSettings.CALCITE_ENABLED.get(settings);
        this.queryEnabledOnCalcite = DslQueryExecutorSettings.CALCITE_QUERY_ENABLED.get(settings);
        this.aggregationEnabledOnCalcite = DslQueryExecutorSettings.CALCITE_AGGREGATION_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(DslQueryExecutorSettings.CALCITE_ENABLED, v -> this.calciteEnabled = v);
        clusterSettings.addSettingsUpdateConsumer(DslQueryExecutorSettings.CALCITE_QUERY_ENABLED, v -> this.queryEnabledOnCalcite = v);
        clusterSettings.addSettingsUpdateConsumer(
            DslQueryExecutorSettings.CALCITE_AGGREGATION_ENABLED,
            v -> this.aggregationEnabledOnCalcite = v
        );
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
        SearchSourceBuilder source = searchRequest.source();

        // Per-category operational gate: even when the grammar could handle the request, route it
        // to codec if it exercises a capability whose toggle is off.
        if (isDisabledCapability(source)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        RouteDecision decision = grammar.validate(source);

        if (!decision.supported()) {
            logger.debug("Grammar rejected _search, falling back to codec: {}", decision.rejectionReasons());
            chain.proceed(task, action, request, listener);
            return;
        }

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
     * True if the request exercises a capability whose per-category toggle is off — routed to
     * codec even if the grammar could handle it. {@code usesAggs} = the request carries
     * aggregations; {@code usesQuery} = it returns hits or is a non-aggregation request (plain
     * search / count). A mixed request needs both toggles on.
     */
    private boolean isDisabledCapability(SearchSourceBuilder source) {
        boolean usesAggs = source != null && source.aggregations() != null;
        boolean usesQuery = usesAggs == false || source.size() != 0;
        return (usesAggs && aggregationEnabledOnCalcite == false) || (usesQuery && queryEnabledOnCalcite == false);
    }

    private static boolean isFallbackable(Throwable e) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = e; t != null && seen.add(t); t = t.getCause()) {
            if (t instanceof ConversionException) return true;
        }
        return false;
    }
}

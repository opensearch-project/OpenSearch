/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
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
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Set;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class SearchActionFilterTests extends OpenSearchTestCase {

    private final NodeClient client = mock(NodeClient.class);
    private final Task task = mock(Task.class);
    private final ActionListener<ActionResponse> listener = mock(ActionListener.class);
    private final ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
    private final ActionRequestMetadata<ActionRequest, ActionResponse> metadata = mock(ActionRequestMetadata.class);
    private final DslCalciteGrammar grammar = mock(DslCalciteGrammar.class);

    private SearchActionFilter buildFilter(boolean calciteEnabled) {
        return buildFilter(calciteEnabled, true, true);
    }

    private SearchActionFilter buildFilter(boolean calciteEnabled, boolean queryEnabled, boolean aggregationEnabled) {
        Settings settings = Settings.builder()
            .put(DslQueryExecutorSettings.CALCITE_ENABLED.getKey(), calciteEnabled)
            .put(DslQueryExecutorSettings.CALCITE_QUERY_ENABLED.getKey(), queryEnabled)
            .put(DslQueryExecutorSettings.CALCITE_AGGREGATION_ENABLED.getKey(), aggregationEnabled)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(
                DslQueryExecutorSettings.CALCITE_ENABLED,
                DslQueryExecutorSettings.CALCITE_QUERY_ENABLED,
                DslQueryExecutorSettings.CALCITE_AGGREGATION_ENABLED
            )
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return new SearchActionFilter(client, clusterService, grammar);
    }

    public void testOrderRunsAfterSecurityFilter() {
        SearchActionFilter filter = buildFilter(true);
        assertEquals(SearchActionFilter.FILTER_ORDER, filter.order());
    }

    // ---- Layer 1: master switch ----

    public void testCalciteDisabledSendsSearchToCodec() {
        SearchActionFilter filter = buildFilter(false);
        SearchRequest request = new SearchRequest("test-index");

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
        verify(grammar, never()).validate(any());
    }

    public void testCalciteEnabledButNonSearchActionPassesThrough() {
        SearchActionFilter filter = buildFilter(true);
        BulkRequest request = new BulkRequest();

        filter.apply(task, BulkAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
        verify(grammar, never()).validate(any());
    }

    // ---- Layer 1b: per-category toggles ----

    public void testAggregationDisabledSendsAggRequestToCodec() {
        SearchActionFilter filter = buildFilter(true, true, false); // aggregation off
        SearchRequest request = new SearchRequest("test-index").source(
            new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.avg("avg_price").field("price"))
        );

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(grammar, never()).validate(any());
        verify(client, never()).execute(any(), any(), any());
    }

    public void testQueryDisabledSendsHitsRequestToCodec() {
        SearchActionFilter filter = buildFilter(true, false, true); // query/hits off
        SearchRequest request = new SearchRequest("test-index").source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("brand", "Acme"))
        );

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(grammar, never()).validate(any());
    }

    public void testAggregationDisabledStillRoutesHitsRequest() {
        SearchActionFilter filter = buildFilter(true, true, false); // aggregation off, query on
        SearchRequest request = new SearchRequest("test-index").source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("brand", "Acme"))
        );
        when(grammar.validate(any())).thenReturn(RouteDecision.accepted());

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        // A hits request is unaffected by the aggregation toggle — grammar consulted, dispatched to Calcite.
        verify(grammar).validate(request.source());
        verify(client).execute(eq(DslExecuteAction.INSTANCE), eq(request), any());
    }

    // ---- Layer 2: grammar decision ----

    public void testGrammarRejectFallsBackToCodec() {
        SearchActionFilter filter = buildFilter(true);
        SearchRequest request = new SearchRequest("test-index").source(new SearchSourceBuilder());
        when(grammar.validate(any())).thenReturn(RouteDecision.rejected(List.of("query:match")));

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(grammar).validate(request.source());
        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    public void testGrammarAcceptDispatchesToCalcite() {
        SearchActionFilter filter = buildFilter(true);
        SearchRequest request = new SearchRequest("test-index").source(new SearchSourceBuilder());
        when(grammar.validate(any())).thenReturn(RouteDecision.accepted());

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(client).execute(eq(DslExecuteAction.INSTANCE), eq(request), any());
        verify(chain, never()).proceed(any(), any(), any(), any());
    }

    // ---- Layer 2 fallback: ConversionException triggers codec ----

    public void testConversionExceptionInCalciteFallsBackToCodec() {
        SearchActionFilter filter = buildFilter(true);
        SearchRequest request = new SearchRequest("test-index").source(new SearchSourceBuilder());
        when(grammar.validate(any())).thenReturn(RouteDecision.accepted());

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        // Grab the wrapped listener passed to client.execute and simulate a ConversionException.
        ArgumentCaptor<ActionListener<SearchResponse>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(client).execute(eq(DslExecuteAction.INSTANCE), eq(request), captor.capture());

        captor.getValue().onFailure(new ConversionException("field 'x' not in schema"));

        // Fallback = chain.proceed with the original listener.
        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(listener, never()).onFailure(any());
    }

    public void testWrappedConversionExceptionAlsoTriggersFallback() {
        SearchActionFilter filter = buildFilter(true);
        SearchRequest request = new SearchRequest("test-index").source(new SearchSourceBuilder());
        when(grammar.validate(any())).thenReturn(RouteDecision.accepted());

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        ArgumentCaptor<ActionListener<SearchResponse>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(client).execute(eq(DslExecuteAction.INSTANCE), eq(request), captor.capture());

        // Simulate an OS-side wrapper around the real cause.
        Exception wrapped = new RuntimeException("engine failed", new ConversionException("nested cause"));
        captor.getValue().onFailure(wrapped);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
    }

    public void testNonConversionExceptionSurfacesToCaller() {
        SearchActionFilter filter = buildFilter(true);
        SearchRequest request = new SearchRequest("test-index").source(new SearchSourceBuilder());
        when(grammar.validate(any())).thenReturn(RouteDecision.accepted());

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        ArgumentCaptor<ActionListener<SearchResponse>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(client).execute(eq(DslExecuteAction.INSTANCE), eq(request), captor.capture());

        RuntimeException engineErr = new RuntimeException("engine timeout");
        captor.getValue().onFailure(engineErr);

        // No fallback for runtime errors — the caller sees the failure.
        verify(listener).onFailure(engineErr);
        verify(chain, never()).proceed(any(), any(), any(), any());
    }

    public void testSuccessDeliveryFailureDoesNotDoubleComplete() {
        SearchActionFilter filter = buildFilter(true);
        SearchRequest request = new SearchRequest("test-index").source(new SearchSourceBuilder());
        when(grammar.validate(any())).thenReturn(RouteDecision.accepted());

        // Delivering the successful response to the caller throws (e.g. channel closed).
        RuntimeException deliveryError = new RuntimeException("channel closed");
        doThrow(deliveryError).when(listener).onResponse(any());

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        ArgumentCaptor<ActionListener<SearchResponse>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(client).execute(eq(DslExecuteAction.INSTANCE), eq(request), captor.capture());

        // Calcite succeeded; the delivery hiccup must propagate as-is, NOT be turned into a second
        // completion of the caller's listener (onFailure) or a codec fallback.
        SearchResponse resp = mock(SearchResponse.class);
        expectThrows(RuntimeException.class, () -> captor.getValue().onResponse(resp));

        verify(listener).onResponse(resp);
        verify(listener, never()).onFailure(any());
        verify(chain, never()).proceed(any(), any(), any(), any());
    }

    // ---- dynamic setting update ----

    public void testDynamicDisableStopsInterception() {
        Settings initial = Settings.builder().put(DslQueryExecutorSettings.CALCITE_ENABLED.getKey(), true).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            initial,
            Set.of(
                DslQueryExecutorSettings.CALCITE_ENABLED,
                DslQueryExecutorSettings.CALCITE_QUERY_ENABLED,
                DslQueryExecutorSettings.CALCITE_AGGREGATION_ENABLED
            )
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(initial);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        SearchActionFilter filter = new SearchActionFilter(client, clusterService, grammar);

        // Push a dynamic update: calcite off.
        Settings updated = Settings.builder().put(DslQueryExecutorSettings.CALCITE_ENABLED.getKey(), false).build();
        clusterSettings.applySettings(updated);

        SearchRequest request = new SearchRequest("test-index").source(new SearchSourceBuilder());
        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, SearchAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
        verify(grammar, never()).validate(any());
    }
}

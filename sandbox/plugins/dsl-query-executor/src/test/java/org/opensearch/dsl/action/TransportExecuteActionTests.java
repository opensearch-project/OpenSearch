/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.Version;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportExecuteActionTests extends OpenSearchTestCase {

    public void testDoExecuteReturnsSearchResponse() {
        TransportExecuteAction action = createAction(new Index("test-index", "uuid"));

        TestListener listener = executeWith(action, "test-index");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    public void testDoExecuteReturnsEmptyResponseWhenResolutionIsEmpty() {
        // allow_no_indices=true + a wildcard matching nothing resolves to zero concrete indices.
        // The request must return an empty 200 (vanilla _search parity), not throw or hang while
        // building a mapper for an empty index set (the RequestScopedMapperService ctor guard).
        TransportExecuteAction action = createAction(); // resolver returns no indices

        TestListener listener = executeWith(action, "mi_nomatch_*");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull("Expected an empty 200 response, not a hang", listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
        assertEquals(0L, listener.response.get().getHits().getTotalHits().value());
        assertNull("Empty resolution must carry no aggregations", listener.response.get().getAggregations());
    }

    public void testDoExecuteStillFailsWhenAllowNoIndicesFalseThrows() {
        // allow_no_indices=false + a wildcard matching nothing makes the resolver throw
        // IndexNotFoundException (404) before resolution returns — the empty short-circuit must not
        // broaden to swallow this; the failure must still route to the listener.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenThrow(new IndexNotFoundException("mi_nomatch_*"));

        TransportExecuteAction action = new TransportExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            mock(IndicesService.class),
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "mi_nomatch_*");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(listener.failure.get() instanceof IndexNotFoundException);
    }

    public void testDoExecuteFailsWhenIndexNotInSchema() {
        TransportExecuteAction action = createAction(new Index("nonexistent-index", "uuid"));

        TestListener listener = executeWith(action, "nonexistent-index");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(listener.failure.get() instanceof IllegalArgumentException);
        assertTrue(listener.failure.get().getMessage().contains("nonexistent-index"));
    }

    public void testDoExecuteRejectsDivergentMultiIndexAggregatedField() {
        // "metric" is long in index-a and double in index-b — divergent conversion types on a
        // referenced (and aggregated) field, which the wired schema-equivalence gate must reject
        // with a 400 before any plan executes.
        TransportExecuteAction action = createActionWithMappers(
            Map.of("index-a", NumberFieldMapper.NumberType.LONG, "index-b", NumberFieldMapper.NumberType.DOUBLE),
            new Index("index-a", "uuid-a"),
            new Index("index-b", "uuid-b")
        );

        SearchRequest request = new SearchRequest("multi-alias");
        request.source(new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("by_metric").field("metric")));
        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(listener.failure.get() instanceof IllegalArgumentException);
        String message = listener.failure.get().getMessage();
        assertTrue(message, message.contains("metric"));
        assertTrue(message, message.contains("index-a"));
        assertTrue(message, message.contains("index-b"));
    }

    public void testDoExecuteAcceptsAgreeingMultiIndexReferencedField() {
        // "metric" is long in both indices — a referenced field the indices agree on, so the gate
        // passes and the request flows through the multi-index wiring to execution.
        TransportExecuteAction action = createActionWithMappers(
            Map.of("index-a", NumberFieldMapper.NumberType.LONG, "index-b", NumberFieldMapper.NumberType.LONG),
            new Index("index-a", "uuid-a"),
            new Index("index-b", "uuid-b")
        );

        SearchRequest request = new SearchRequest("multi-alias");
        request.source(new SearchSourceBuilder().query(new TermQueryBuilder("metric", 1)));
        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /**
     * Builds an action over several indices whose "metric" field is typed per-index by
     * {@code metricTypePerIndex}, with a schema exposing the resolved indices' union table (named
     * by the comma-joined concrete names, as the transport resolves it) carrying a single BIGINT
     * "metric" column, and an {@link IndicesService} handing back a MapperService per index.
     */
    private TransportExecuteAction createActionWithMappers(
        Map<String, NumberFieldMapper.NumberType> metricTypePerIndex,
        Index... resolvedIndices
    ) {
        Metadata.Builder metadata = Metadata.builder();
        StringBuilder unionName = new StringBuilder();
        for (Index index : resolvedIndices) {
            metadata.put(
                IndexMetadata.builder(index.getName())
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                false
            );
            if (unionName.length() > 0) {
                unionName.append(',');
            }
            unionName.append(index.getName());
        }
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(resolvedIndices);

        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add(unionName.toString(), new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder().add("metric", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true)).build();
            }
        });
        QueryRequestContext requestContext = new QueryRequestContext(null, schema);
        EngineContextProvider contextProvider = new EngineContextProvider() {
            @Override
            public QueryRequestContext getContext(ClusterState clusterState) {
                return requestContext;
            }

            @Override
            public QueryRequestContext getContext(ClusterState clusterState, IndicesOptions indicesOptions) {
                return requestContext;
            }

            @Override
            public QueryRequestContext getContext() {
                return requestContext;
            }
        };

        IndicesService indicesService = mock(IndicesService.class);
        try {
            when(indicesService.createIndexMapperService(any())).thenAnswer(invocation -> {
                IndexMetadata indexMetadata = invocation.getArgument(0);
                NumberFieldMapper.NumberType metricType = metricTypePerIndex.get(indexMetadata.getIndex().getName());
                MappedFieldType metricFieldType = new NumberFieldMapper.NumberFieldType("metric", metricType);
                MapperService mapperService = mock(MapperService.class);
                when(mapperService.fieldType("metric")).thenReturn(metricFieldType);
                return mapperService;
            });
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        return new TransportExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            contextProvider,
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            indicesService,
            resolver,
            mockThreadPool()
        );
    }

    public void testDoExecuteFailsWhenIndexNotInClusterState() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenThrow(new IndexNotFoundException("bogus-index"));

        TransportExecuteAction action = new TransportExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            mock(IndicesService.class),
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "bogus-index");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(listener.failure.get() instanceof IndexNotFoundException);
    }

    /**
     * Wiring: the transport must thread the request's {@link IndicesOptions} into the engine
     * schema build so schema membership matches the coordinator's index resolution. Captures the
     * options passed to {@code getContext(state, options)} and asserts it is the request's own
     * options object (not the {@code lenientExpandOpen()} default).
     */
    public void testDoExecuteThreadsRequestIndicesOptionsIntoSchemaBuild() {
        Index index = new Index("test-index", "uuid");
        Metadata.Builder metadata = Metadata.builder();
        metadata.put(
            IndexMetadata.builder(index.getName())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
            false
        );
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(new Index[] { index });

        QueryRequestContext ctx = new QueryRequestContext(null, buildSchema());
        AtomicReference<IndicesOptions> captured = new AtomicReference<>();
        EngineContextProvider recording = new EngineContextProvider() {
            @Override
            public QueryRequestContext getContext(ClusterState clusterState) {
                return ctx;
            }

            @Override
            public QueryRequestContext getContext(ClusterState clusterState, IndicesOptions indicesOptions) {
                captured.set(indicesOptions);
                return ctx;
            }

            @Override
            public QueryRequestContext getContext() {
                return ctx;
            }
        };

        TransportExecuteAction action = new TransportExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            recording,
            (plan, c, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            mock(IndicesService.class),
            resolver,
            mockThreadPool()
        );

        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder());
        IndicesOptions custom = IndicesOptions.fromOptions(true, false, false, true);
        request.indicesOptions(custom);

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertSame("Transport must thread request.indicesOptions() into the schema build", custom, captured.get());
    }

    private TestListener executeWith(TransportExecuteAction action, String index) {
        SearchRequest request = new SearchRequest(index);
        request.source(new SearchSourceBuilder());

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);
        return listener;
    }

    private TransportExecuteAction createAction(Index... resolvedIndices) {
        Metadata.Builder metadata = Metadata.builder();
        for (Index index : resolvedIndices) {
            metadata.put(
                IndexMetadata.builder(index.getName())
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                false
            );
        }
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(resolvedIndices);

        return new TransportExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            mock(IndicesService.class),
            resolver,
            mockThreadPool()
        );
    }

    private EngineContextProvider buildEngineContext() {
        QueryRequestContext ctx = new QueryRequestContext(null, buildSchema());
        return new EngineContextProvider() {
            @Override
            public QueryRequestContext getContext(ClusterState clusterState) {
                return ctx;
            }

            @Override
            public QueryRequestContext getContext(ClusterState clusterState, IndicesOptions indicesOptions) {
                return ctx;
            }

            @Override
            public QueryRequestContext getContext() {
                return ctx;
            }
        };
    }

    private SchemaPlus buildSchema() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test-index", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder().add("name", SqlTypeName.VARCHAR).add("price", SqlTypeName.INTEGER).build();
            }
        });
        return schema;
    }

    private static ThreadPool mockThreadPool() {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(any())).thenReturn(executorService);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(executorService).execute(any());
        return threadPool;
    }

    private static class TestListener implements ActionListener<SearchResponse> {
        final AtomicReference<SearchResponse> response = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();

        @Override
        public void onResponse(SearchResponse r) {
            response.set(r);
        }

        @Override
        public void onFailure(Exception e) {
            failure.set(e);
        }
    }
}

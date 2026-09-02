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
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.dsl.settings.DslGateInputs;
import org.opensearch.dsl.settings.DslQuerySettings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportDslExecuteActionTests extends OpenSearchTestCase {

    public void testDoExecuteReturnsSearchResponse() {
        TransportDslExecuteAction action = createAction(new Index("test-index", "uuid"));

        TestListener listener = executeWith(action, "test-index");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    public void testDoExecuteFailsWhenIndexNotInSchema() {
        TransportDslExecuteAction action = createAction(new Index("nonexistent-index", "uuid"));

        TestListener listener = executeWith(action, "nonexistent-index");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(listener.failure.get() instanceof IllegalArgumentException);
        assertTrue(listener.failure.get().getMessage().contains("nonexistent-index"));
    }

    public void testDoExecuteRejectsMultipleConcreteIndices() {
        TransportDslExecuteAction action = createAction(new Index("index-a", "uuid-a"), new Index("index-b", "uuid-b"));

        TestListener listener = executeWith(action, "multi-alias");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(listener.failure.get() instanceof IllegalArgumentException);
        assertTrue(listener.failure.get().getMessage().contains("exactly one concrete index"));
    }

    public void testDoExecuteFailsWhenIndexNotInClusterState() {
        ClusterService clusterService = clusterService();
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenThrow(new IndexNotFoundException("bogus-index"));

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            mock(IndicesService.class),
            resolver,
            mockThreadPool(),
            new DslQuerySettings(clusterService),
            new DslGateInputs(clusterService.getClusterSettings())
        );

        TestListener listener = executeWith(action, "bogus-index");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(listener.failure.get() instanceof IndexNotFoundException);
    }

    private TestListener executeWith(TransportDslExecuteAction action, String index) {
        SearchRequest request = new SearchRequest(index);
        request.source(new SearchSourceBuilder());

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);
        return listener;
    }

    private TransportDslExecuteAction createAction(Index... resolvedIndices) {
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
        // A real state (not a bare mock): doExecute pins the request's mapping off
        // state.metadata().getIndexSafe(...), so the resolved indices must actually exist in it.
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
        // A settings-bearing cluster service, not a bare mock: the action now builds a DslQuerySettings
        // and a DslGateInputs off it.
        ClusterService clusterService = clusterService();
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(resolvedIndices);

        return new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            mock(IndicesService.class),
            resolver,
            mockThreadPool(),
            new DslQuerySettings(clusterService),
            new DslGateInputs(clusterService.getClusterSettings())
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

    /**
     * A mock cluster service with the settings surface both DSL settings holders read at construction:
     * {@code DslQuerySettings} takes its initial value from the node settings and registers an update
     * consumer on the registry, and {@code DslGateInputs} reads the registry per call.
     */
    private static ClusterService clusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.addAll(DslQuerySettings.all());
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, registered));
        return clusterService;
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

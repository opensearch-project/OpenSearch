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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
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

    // Two Index objects sharing the same name but different UUIDs can arise after a reindex or
    // shrink operation. The comma-join must still produce a resolvable schema table name.
    public void testDoExecuteAcceptsMultipleConcreteIndices() {
        // Schema entry is keyed by the comma-joined string that the converter receives.
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "test-index,test-index");

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("test-index", "uuid-a"), new Index("test-index", "uuid-b") }
        );

        QueryRequestContext ctx = new QueryRequestContext(null, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            () -> ctx,
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "multi-alias");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull("Expected a response for multiple resolved indices", listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    // Updated to assert new contract: IndexNotFoundException propagates from resolver
    public void testDoExecuteFailsWhenIndexNotInClusterState() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenThrow(new IndexNotFoundException("bogus-index"));

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "bogus-index");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(listener.failure.get() instanceof IndexNotFoundException);
    }

    // --- New tests for the pre-resolve design ---

    /** Two existing indices resolve and the converter receives both concrete names comma-joined. */
    public void testMultipleConcreteIndicesPassedAsCommaListToConverter() {
        // Schema entry keyed by the comma-joined string proves the transport action
        // passes the resolved names as "index-a,index-b" to the converter.
        // In production the analytics schema's lazy resolution handles comma-split internally.
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "index-a,index-b");

        TransportDslExecuteAction action = createActionWithSchema(schema, new Index("index-a", "uuid-a"), new Index("index-b", "uuid-b"));

        TestListener listener = executeWith(action, "index-a", "index-b");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /** An alias resolves to concrete backing index names, not the alias name itself. */
    public void testAliasResolvesToConcreteBackingIndexNames() {
        // Schema has the concrete backing index, not the alias
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "concrete-backing-index");

        TransportDslExecuteAction action = createActionWithSchema(schema, new Index("concrete-backing-index", "uuid-backing"));

        // Request uses alias name, but resolver returns concrete name
        TestListener listener = executeWith(action, "my-alias");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /** Wildcard matching nothing responds successfully with empty SearchResponse; converter never invoked. */
    public void testWildcardMatchingNothingReturnsEmptyResponse() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        // Zero concrete indices — wildcard matched nothing but allow_no_indices=true (default)
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(new Index[0]);

        // Use a failing executor to prove the converter is never invoked
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> {
                throw new AssertionError("converter should not be invoked");
            },
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "zzz_no_match_*");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull("Expected an empty SearchResponse", listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
        assertEquals(0, listener.response.get().getHits().getTotalHits().value());
        assertEquals(0, listener.response.get().getTotalShards());
    }

    /** allow_no_indices=false with wildcard matching nothing fails with IndexNotFoundException. */
    public void testAllowNoIndicesFalseWithNoMatchFails() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenThrow(new IndexNotFoundException("zzz_no_match_*"));

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        SearchRequest request = new SearchRequest("zzz_no_match_*");
        request.source(new SearchSourceBuilder());
        request.indicesOptions(IndicesOptions.fromOptions(false, false, true, false));

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(
            "Expected IndexNotFoundException but got: " + listener.failure.get().getClass(),
            listener.failure.get() instanceof IndexNotFoundException
        );
    }

    /** A nonexistent concrete index under default options fails with IndexNotFoundException. */
    public void testNonexistentConcreteIndexFailsWithIndexNotFound() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenThrow(new IndexNotFoundException("totally_fake_index"));

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "totally_fake_index");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(
            "Expected IndexNotFoundException but got: " + listener.failure.get().getClass(),
            listener.failure.get() instanceof IndexNotFoundException
        );
    }

    /** ignore_unavailable=true with one existing + one missing resolves to only the existing one. */
    public void testIgnoreUnavailableTrueResolvesOnlyExistingIndex() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "existing-index");

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        // Resolver with ignore_unavailable=true returns only the existing index
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("existing-index", "uuid-exists") }
        );

        QueryRequestContext ctx = new QueryRequestContext(null, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            () -> ctx,
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        SearchRequest request = new SearchRequest("existing-index", "missing-index");
        request.source(new SearchSourceBuilder());
        request.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /** null or empty request.indices() fails with IllegalArgumentException. */
    public void testNullOrEmptyIndicesThrowsIllegalArgument() {
        TransportDslExecuteAction action = createAction(new Index("test-index", "uuid"));

        // Test with empty indices
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder());

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(
            "Expected IllegalArgumentException but got: " + listener.failure.get().getClass(),
            listener.failure.get() instanceof IllegalArgumentException
        );
    }

    // --- Helper methods ---

    private TestListener executeWith(TransportDslExecuteAction action, String... indices) {
        SearchRequest request = new SearchRequest(indices);
        request.source(new SearchSourceBuilder());

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);
        return listener;
    }

    private TransportDslExecuteAction createAction(Index... resolvedIndices) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(resolvedIndices);

        return new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );
    }

    private TransportDslExecuteAction createActionWithSchema(SchemaPlus schema, Index... resolvedIndices) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(resolvedIndices);

        QueryRequestContext ctx = new QueryRequestContext(null, schema);
        return new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            () -> ctx,
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );
    }

    private EngineContextProvider buildEngineContext() {
        QueryRequestContext ctx = new QueryRequestContext(null, buildSchema());
        return () -> ctx;
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

    private static void addTable(SchemaPlus schema, String name) {
        schema.add(name, new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder().add("name", SqlTypeName.VARCHAR).add("price", SqlTypeName.INTEGER).build();
            }
        });
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

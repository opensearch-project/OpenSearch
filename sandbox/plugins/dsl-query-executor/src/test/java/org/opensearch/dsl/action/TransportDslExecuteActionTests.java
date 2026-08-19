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
        // Schema has "test-index" but the converter receives the raw expression "nonexistent-index"
        TransportDslExecuteAction action = createAction(new Index("nonexistent-index", "uuid"));

        TestListener listener = executeWith(action, "nonexistent-index");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(listener.failure.get() instanceof IllegalArgumentException);
        assertTrue(listener.failure.get().getMessage().contains("nonexistent-index"));
    }

    // Two Index objects sharing the same name but different UUIDs can arise after a reindex or
    // shrink operation. The raw expression is passed to the converter (not concrete names).
    public void testDoExecuteAcceptsMultipleConcreteIndices() {
        // Schema entry is keyed by the raw expression that the user sent.
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "multi-alias");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("test-index", "uuid-a"), new Index("test-index", "uuid-b") }
        );

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
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
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

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

    // --- Tests for the raw-expression-to-converter contract ---

    /**
     * When the user sends an alias name, the converter receives the alias name (raw expression),
     * NOT the backing index names. This lets the engine's alias-branch guardrails fire natively.
     */
    public void testAliasRawExpressionReachesConverter() {
        // Schema has the alias name as a table — this is what the engine's schema builder
        // produces when IndicesOptions + alias identity are threaded correctly.
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "my-alias");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        // Resolver returns the backing concrete index, but the converter should NOT see it
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("concrete-backing-index", "uuid-backing") }
        );

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        // Request uses alias name
        TestListener listener = executeWith(action, "my-alias");

        // If the raw expression reaches the converter, it finds "my-alias" in the schema and succeeds.
        // If concrete names were passed, it would look for "concrete-backing-index" and fail.
        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /** Multiple raw index expressions are joined by commas and reach the converter. */
    public void testMultipleRawExpressionsJoinedByComma() {
        // Schema entry keyed by the comma-joined RAW expressions
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "index-a,index-b");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("index-a", "uuid-a"), new Index("index-b", "uuid-b") }
        );

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "index-a", "index-b");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /** Wildcard matching nothing responds successfully with empty SearchResponse; converter never invoked. */
    public void testWildcardMatchingNothingReturnsEmptyResponse() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

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
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

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
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

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
        // Schema has the raw expression that will be passed to the converter
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "existing-index,missing-index");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        // Resolver with ignore_unavailable=true returns only the existing index
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("existing-index", "uuid-exists") }
        );

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
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

    // --- Filtered-alias rejection tests (engine-side path) ---

    /**
     * A filtered alias name reaches the converter as the raw expression.
     * Engine-side rejection (via IndexResolution.resolveAlias) cannot be asserted from this
     * unit test module because it requires a real OpenSearchSchemaBuilder + IndexResolution
     * wiring. This test verifies that the raw alias name is what gets passed to the converter
     * (not concrete backing indices), which is the prerequisite for engine guardrails to fire.
     *
     * Integration test DslQueryExecutorIT should cover end-to-end filtered-alias rejection.
     */
    public void testFilteredAliasNameReachesConverterAsRawExpression() {
        // Schema has the alias name — converter will find it and succeed.
        // In production the engine's IndexResolution.resolveAlias would reject it,
        // but that happens inside the engine, not at the coordinator level.
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "filtered-alias");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("backing-1", "uuid-1"), new Index("backing-2", "uuid-2") }
        );

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "filtered-alias");

        // The alias name reaches the converter (schema has "filtered-alias" entry), so it succeeds
        // at the coordinator. Engine-side rejection happens deeper in IndexResolution.resolveAlias.
        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /**
     * A single-backing filtered alias also passes the raw alias name to the converter.
     * Same principle: engine-side rejection is tested at integration level.
     */
    public void testSingleBackingFilteredAliasNameReachesConverter() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "single-filtered-alias");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("single-backing", "uuid-single") }
        );

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "single-filtered-alias");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /** A non-filtered alias passes the raw alias name to the converter and succeeds. */
    public void testNonFilteredAliasPassesRawNameToConverter() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "non-filtered-alias");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("nf-backing-1", "uuid-nf1"), new Index("nf-backing-2", "uuid-nf2") }
        );

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "non-filtered-alias");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /** A plain concrete index (no alias involvement) still succeeds. */
    public void testPlainConcreteIndexStillSucceeds() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "plain-concrete");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("plain-concrete", "uuid-plain") }
        );

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "plain-concrete");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /** A wildcard expression is passed as the raw expression to the converter. */
    public void testWildcardExpressionPassedAsRawToConverter() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "filtered-*");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(new Index[] { new Index("wc-backing", "uuid-wc") });

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        // User specifies wildcard "filtered-*"
        TestListener listener = executeWith(action, "filtered-*");

        // The raw wildcard expression reaches the converter — schema has "filtered-*" entry
        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    // --- D5: QueryRequestContext carries IndicesOptions and ClusterState ---

    /** The QueryRequestContext handed to the engine carries the request's IndicesOptions and the captured ClusterState. */
    public void testQueryRequestContextCarriesIndicesOptionsAndClusterState() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "ctx-test-index");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(new Index[] { new Index("ctx-test-index", "uuid-ctx") });

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        // Capture what the engine executor actually receives
        AtomicReference<QueryRequestContext> capturedContext = new AtomicReference<>();
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> {
                capturedContext.set(execCtx);
                l.onResponse(Collections.emptyList());
            },
            clusterService,
            resolver,
            mockThreadPool()
        );

        IndicesOptions customOptions = IndicesOptions.fromOptions(true, false, true, true);
        SearchRequest request = new SearchRequest("ctx-test-index");
        request.source(new SearchSourceBuilder());
        request.indicesOptions(customOptions);

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());

        // Verify the context passed to the engine
        assertNotNull("QueryRequestContext should be passed to engine executor", capturedContext.get());
        assertSame("ClusterState must be the same snapshot captured at coordinator", state, capturedContext.get().clusterState());
        assertSame("IndicesOptions must match the request's options", customOptions, capturedContext.get().indicesOptions());
    }

    /** Default IndicesOptions are threaded when not explicitly set on the request. */
    public void testDefaultIndicesOptionsAreThreadedToEngine() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "default-opts-index");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("default-opts-index", "uuid-default") }
        );

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        AtomicReference<QueryRequestContext> capturedContext = new AtomicReference<>();
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> {
                capturedContext.set(execCtx);
                l.onResponse(Collections.emptyList());
            },
            clusterService,
            resolver,
            mockThreadPool()
        );

        // SearchRequest default IndicesOptions
        SearchRequest request = new SearchRequest("default-opts-index");
        request.source(new SearchSourceBuilder());

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(capturedContext.get());
        assertSame(
            "IndicesOptions should be the request's default options",
            request.indicesOptions(),
            capturedContext.get().indicesOptions()
        );
        assertSame("ClusterState should be the captured state", state, capturedContext.get().clusterState());
    }

    // --- D6: Schema built with request's IndicesOptions ---

    /**
     * The schema handed to the converter must be built with the request's IndicesOptions, not
     * the default lenientExpandOpen(). This verifies that
     * {@link EngineContextProvider#getContext(ClusterState, IndicesOptions)} is called with the
     * request's options and that the resulting schema drives conversion.
     */
    public void testSchemaBuiltWithRequestIndicesOptions() {
        // Two schemas: one "default" and one "custom". The custom schema has a table
        // that only appears when the right options are used.
        SchemaPlus defaultSchema = CalciteSchema.createRootSchema(true).plus();
        addTable(defaultSchema, "default-table");

        SchemaPlus customSchema = CalciteSchema.createRootSchema(true).plus();
        addTable(customSchema, "custom-options-table");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("custom-options-table", "uuid-custom") }
        );

        IndicesOptions customOptions = IndicesOptions.fromOptions(true, true, true, true);

        // Set up the context provider to return different schemas depending on whether
        // the options-aware overload is called
        QueryRequestContext defaultCtx = new QueryRequestContext(state, defaultSchema);
        QueryRequestContext customCtx = new QueryRequestContext(state, customSchema, null, null, customOptions);

        EngineContextProvider provider = mock(EngineContextProvider.class);
        when(provider.getContext(any(ClusterState.class))).thenReturn(defaultCtx);
        when(provider.getContext(any(ClusterState.class), any(IndicesOptions.class))).thenReturn(customCtx);

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            provider,
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        SearchRequest request = new SearchRequest("custom-options-table");
        request.source(new SearchSourceBuilder());
        request.indicesOptions(customOptions);

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);

        // If the action uses the options-aware overload, the converter gets the custom schema
        // which has "custom-options-table" — and succeeds. If it uses the default overload,
        // the converter gets the default schema which doesn't have "custom-options-table" and fails.
        assertNull(
            "Expected no failure — schema must be built with request's IndicesOptions, but got: " + listener.failure.get(),
            listener.failure.get()
        );
        assertNotNull(listener.response.get());
    }

    // --- Coordinator-side filtered-alias rejection tests (rejectFilteringAliases guard) ---

    /** Literal filtered alias name is rejected at coordinator (E1 case). */
    public void testFilteredAliasLiteralIsRejected() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(new Index[] { new Index("mi_open_a", "uuid-a") });

        Set<String> resolvedExprs = new HashSet<>();
        resolvedExprs.add("al_filtered");
        when(resolver.resolveExpressions(any(), any(IndicesOptions.class), any(String[].class))).thenReturn(resolvedExprs);
        when(resolver.filteringAliases(state, "mi_open_a", resolvedExprs)).thenReturn(new String[] { "al_filtered" });

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "al_filtered");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(
            "Expected IllegalArgumentException but got: " + listener.failure.get().getClass(),
            listener.failure.get() instanceof IllegalArgumentException
        );
        assertTrue(
            "Expected message about filter alias but got: " + listener.failure.get().getMessage(),
            listener.failure.get()
                .getMessage()
                .contains(
                    "Alias [al_filtered] declares a filter on index [mi_open_a]; filter aliases are not yet supported by analytics queries"
                )
        );
    }

    /** Wildcard 'al_*' matching a filtered alias is rejected at coordinator (E2 case). */
    public void testWildcardMatchingFilteredAliasIsRejected() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("mi_open_a", "uuid-a"), new Index("mi_open_b", "uuid-b") }
        );

        Set<String> resolvedExprs = new HashSet<>();
        resolvedExprs.add("al_filtered");
        resolvedExprs.add("al_open");
        when(resolver.resolveExpressions(any(), any(IndicesOptions.class), any(String[].class))).thenReturn(resolvedExprs);
        when(resolver.filteringAliases(state, "mi_open_a", resolvedExprs)).thenReturn(new String[] { "al_filtered" });
        when(resolver.filteringAliases(state, "mi_open_b", resolvedExprs)).thenReturn(null);

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "al_*");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(
            "Expected IllegalArgumentException but got: " + listener.failure.get().getClass(),
            listener.failure.get() instanceof IllegalArgumentException
        );
        assertTrue(
            "Expected message about filter alias but got: " + listener.failure.get().getMessage(),
            listener.failure.get()
                .getMessage()
                .contains(
                    "Alias [al_filtered] declares a filter on index [mi_open_a]; filter aliases are not yet supported by analytics queries"
                )
        );
    }

    /** Comma-list 'mi_open_b,al_filtered' is rejected at coordinator (E3 case). */
    public void testCommaListWithFilteredAliasSecondIsRejected() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("mi_open_b", "uuid-b"), new Index("mi_open_a", "uuid-a") }
        );

        Set<String> resolvedExprs = new HashSet<>();
        resolvedExprs.add("mi_open_b");
        resolvedExprs.add("al_filtered");
        when(resolver.resolveExpressions(any(), any(IndicesOptions.class), any(String[].class))).thenReturn(resolvedExprs);
        when(resolver.filteringAliases(state, "mi_open_b", resolvedExprs)).thenReturn(null);
        when(resolver.filteringAliases(state, "mi_open_a", resolvedExprs)).thenReturn(new String[] { "al_filtered" });

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "mi_open_b", "al_filtered");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(
            "Expected IllegalArgumentException but got: " + listener.failure.get().getClass(),
            listener.failure.get() instanceof IllegalArgumentException
        );
        assertTrue(
            "Expected message about filter alias but got: " + listener.failure.get().getMessage(),
            listener.failure.get()
                .getMessage()
                .contains(
                    "Alias [al_filtered] declares a filter on index [mi_open_a]; filter aliases are not yet supported by analytics queries"
                )
        );
    }

    /** Reversed comma-list 'al_filtered,mi_open_b' is rejected at coordinator (E3b case). */
    public void testReversedCommaListWithFilteredAliasFirstIsRejected() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("mi_open_a", "uuid-a"), new Index("mi_open_b", "uuid-b") }
        );

        Set<String> resolvedExprs = new HashSet<>();
        resolvedExprs.add("al_filtered");
        resolvedExprs.add("mi_open_b");
        when(resolver.resolveExpressions(any(), any(IndicesOptions.class), any(String[].class))).thenReturn(resolvedExprs);
        when(resolver.filteringAliases(state, "mi_open_a", resolvedExprs)).thenReturn(new String[] { "al_filtered" });
        when(resolver.filteringAliases(state, "mi_open_b", resolvedExprs)).thenReturn(null);

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "al_filtered", "mi_open_b");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(
            "Expected IllegalArgumentException but got: " + listener.failure.get().getClass(),
            listener.failure.get() instanceof IllegalArgumentException
        );
        assertTrue(
            "Expected message about filter alias but got: " + listener.failure.get().getMessage(),
            listener.failure.get()
                .getMessage()
                .contains(
                    "Alias [al_filtered] declares a filter on index [mi_open_a]; filter aliases are not yet supported by analytics queries"
                )
        );
    }

    /** Prefix wildcard 'al_filt*' matching filtered alias is rejected at coordinator (E5 case). */
    public void testPrefixWildcardMatchingFilteredAliasIsRejected() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(new Index[] { new Index("mi_open_a", "uuid-a") });

        Set<String> resolvedExprs = new HashSet<>();
        resolvedExprs.add("al_filtered");
        when(resolver.resolveExpressions(any(), any(IndicesOptions.class), any(String[].class))).thenReturn(resolvedExprs);
        when(resolver.filteringAliases(state, "mi_open_a", resolvedExprs)).thenReturn(new String[] { "al_filtered" });

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "al_filt*");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(
            "Expected IllegalArgumentException but got: " + listener.failure.get().getClass(),
            listener.failure.get() instanceof IllegalArgumentException
        );
        assertTrue(
            "Expected message about filter alias but got: " + listener.failure.get().getMessage(),
            listener.failure.get()
                .getMessage()
                .contains(
                    "Alias [al_filtered] declares a filter on index [mi_open_a]; filter aliases are not yet supported by analytics queries"
                )
        );
    }

    /** Duplicated alias 'al_filtered,al_filtered' is rejected at coordinator (E6 case). */
    public void testDuplicatedFilteredAliasIsRejected() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(new Index[] { new Index("mi_open_a", "uuid-a") });

        Set<String> resolvedExprs = new HashSet<>();
        resolvedExprs.add("al_filtered");
        when(resolver.resolveExpressions(any(), any(IndicesOptions.class), any(String[].class))).thenReturn(resolvedExprs);
        when(resolver.filteringAliases(state, "mi_open_a", resolvedExprs)).thenReturn(new String[] { "al_filtered" });

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "al_filtered", "al_filtered");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(
            "Expected IllegalArgumentException but got: " + listener.failure.get().getClass(),
            listener.failure.get() instanceof IllegalArgumentException
        );
        assertTrue(
            "Expected message about filter alias but got: " + listener.failure.get().getMessage(),
            listener.failure.get()
                .getMessage()
                .contains(
                    "Alias [al_filtered] declares a filter on index [mi_open_a]; filter aliases are not yet supported by analytics queries"
                )
        );
    }

    /** A non-filtered alias (al_open) passes through and succeeds — the guard must not block it. */
    public void testNonFilteredAliasIsNotRejectedByGuard() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "al_open");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(new Index[] { new Index("mi_open_b", "uuid-b") });

        Set<String> resolvedExprs = new HashSet<>();
        resolvedExprs.add("al_open");
        when(resolver.resolveExpressions(any(), any(IndicesOptions.class), any(String[].class))).thenReturn(resolvedExprs);
        // No filtering aliases for this alias
        when(resolver.filteringAliases(state, "mi_open_b", resolvedExprs)).thenReturn(null);

        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        TestListener listener = executeWith(action, "al_open");

        assertNull("Expected no failure but got: " + listener.failure.get(), listener.failure.get());
        assertNotNull(listener.response.get());
        assertEquals(200, listener.response.get().status().getStatus());
    }

    /**
     * A hidden filtered alias must be rejected by the coordinator guard when
     * {@code expand_wildcards=open,hidden} is set. Before the fix, the guard called
     * {@code resolveExpressions(state, requestIndices)} which hardcodes
     * {@code IndicesOptions.lenientExpandOpen()} — excluding hidden abstractions from
     * the resolved set. This let the alias slip through undetected.
     */
    public void testHiddenFilteredAliasIsRejectedWhenExpandWildcardsIncludesHidden() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        // The wildcard "hid_al*" expands (with hidden=true) to the hidden alias "hid_al_filtered",
        // which backs "concrete_data_index". The index name does NOT match the wildcard.
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(
            new Index[] { new Index("concrete_data_index", "uuid-data") }
        );

        // The options-aware overload (3-arg) resolves the hidden alias correctly.
        Set<String> hiddenResolved = new HashSet<>();
        hiddenResolved.add("hid_al_filtered");
        when(resolver.resolveExpressions(any(ClusterState.class), any(IndicesOptions.class), any(String[].class))).thenReturn(
            hiddenResolved
        );

        // filteringAliases detects the alias when it appears in the resolved set.
        when(resolver.filteringAliases(state, "concrete_data_index", hiddenResolved)).thenReturn(new String[] { "hid_al_filtered" });

        // Schema includes the wildcard expression so execution succeeds if guard doesn't throw
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        addTable(schema, "hid_al*");
        QueryRequestContext ctx = new QueryRequestContext(state, schema);

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mockContextProvider(ctx),
            (plan, execCtx, l) -> l.onResponse(Collections.emptyList()),
            clusterService,
            resolver,
            mockThreadPool()
        );

        // Request with expand_wildcards=open,hidden
        SearchRequest request = new SearchRequest("hid_al*");
        request.source(new SearchSourceBuilder());
        request.indicesOptions(IndicesOptions.fromOptions(true, true, true, false, true));

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);

        assertNull("Guard should have rejected the request but it succeeded: response=" + listener.response.get(), listener.response.get());
        assertNotNull("Expected IllegalArgumentException for hidden filtered alias but guard passed", listener.failure.get());
        assertTrue(
            "Expected IllegalArgumentException but got: " + listener.failure.get().getClass(),
            listener.failure.get() instanceof IllegalArgumentException
        );
        assertTrue(
            "Expected message about filter alias but got: " + listener.failure.get().getMessage(),
            listener.failure.get()
                .getMessage()
                .contains(
                    "Alias [hid_al_filtered] declares a filter on index [concrete_data_index];"
                        + " filter aliases are not yet supported by analytics queries"
                )
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
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(resolvedIndices);
        // Stub the filtering-alias guard: no filtering aliases by default
        when(resolver.resolveExpressions(any(), any(IndicesOptions.class), any(String[].class))).thenReturn(Collections.emptySet());
        when(resolver.filteringAliases(any(), any(String.class), any())).thenReturn(null);

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

    private EngineContextProvider buildEngineContext() {
        SchemaPlus schema = buildSchema();
        ClusterState state = mock(ClusterState.class);
        QueryRequestContext ctx = new QueryRequestContext(state, schema);
        return mockContextProvider(ctx);
    }

    private EngineContextProvider mockContextProvider(QueryRequestContext ctx) {
        EngineContextProvider provider = mock(EngineContextProvider.class);
        when(provider.getContext()).thenReturn(ctx);
        when(provider.getContext(any(ClusterState.class))).thenReturn(ctx);
        when(provider.getContext(any(ClusterState.class), any(IndicesOptions.class))).thenReturn(ctx);
        return provider;
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

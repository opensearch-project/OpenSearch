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
import org.opensearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.opensearch.action.support.ActionFilters;
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
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.indices.IndicesService;
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

public class TransportValidateActionTests extends OpenSearchTestCase {

    public void testValidQuery() {
        TestListener listener = validate(new TermQueryBuilder("name", "laptop"), false);

        assertNull(listener.failure.get());
        assertTrue(listener.response.get().isValid());
        // Like vanilla: no explanations unless explain is requested.
        assertTrue(listener.response.get().getQueryExplanation().isEmpty());
    }

    public void testValidQueryWithExplainReturnsPlan() {
        TestListener listener = validate(new TermQueryBuilder("name", "laptop"), true);

        assertTrue(listener.response.get().isValid());
        assertEquals(1, listener.response.get().getQueryExplanation().size());
        String explanation = listener.response.get().getQueryExplanation().get(0).getExplanation();
        assertNotNull(explanation);
        assertTrue("expected a RelNode plan, got: " + explanation, explanation.contains("LogicalTableScan"));
    }

    public void testUnknownFieldIsInvalid() {
        TestListener listener = validate(new TermQueryBuilder("no_such_field", "x"), true);

        assertNull(listener.failure.get());
        ValidateQueryResponse response = listener.response.get();
        assertFalse(response.isValid());
        assertEquals(1, response.getQueryExplanation().size());
        assertFalse(response.getQueryExplanation().get(0).isValid());
        assertTrue(response.getQueryExplanation().get(0).getError().contains("no_such_field"));
    }

    public void testUnknownFieldWithoutExplainOmitsDetail() {
        TestListener listener = validate(new TermQueryBuilder("no_such_field", "x"), false);

        assertFalse(listener.response.get().isValid());
        assertTrue(listener.response.get().getQueryExplanation().isEmpty());
    }

    /**
     * Query types without a registered translator become UnresolvedQueryCall for the engine
     * to resolve or reject at execution time — conversion-level validation reports them valid.
     */
    public void testUnregisteredQueryTypeIsValid() {
        TestListener listener = validate(new MatchQueryBuilder("name", "laptop"), false);

        assertNull(listener.failure.get());
        assertTrue(listener.response.get().isValid());
    }

    public void testNullQueryIsValid() {
        TestListener listener = validate(null, false);

        assertNull(listener.failure.get());
        assertTrue(listener.response.get().isValid());
    }

    public void testMultipleConcreteIndicesFails() {
        TransportValidateAction action = createAction(new Index("index-a", "uuid-a"), new Index("index-b", "uuid-b"));

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), new ValidateQueryRequest("multi-alias"), listener);

        assertNull(listener.response.get());
        assertTrue(listener.failure.get() instanceof IllegalArgumentException);
        assertTrue(listener.failure.get().getMessage().contains("exactly one concrete index"));
    }

    public void testIndexNotInSchemaFails() {
        TransportValidateAction action = createAction(new Index("nonexistent-index", "uuid"));

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), new ValidateQueryRequest("nonexistent-index"), listener);

        assertNull(listener.response.get());
        assertTrue(listener.failure.get() instanceof IllegalArgumentException);
        assertTrue(listener.failure.get().getMessage().contains("nonexistent-index"));
    }

    // ---- Helpers ----

    private TestListener validate(QueryBuilder query, boolean explain) {
        TransportValidateAction action = createAction(new Index("test-index", "uuid"));

        ValidateQueryRequest request = new ValidateQueryRequest("test-index");
        if (query != null) {
            request.query(query);
        }
        request.explain(explain);

        TestListener listener = new TestListener();
        action.doExecute(mock(Task.class), request, listener);
        return listener;
    }

    private TransportValidateAction createAction(Index... resolvedIndices) {
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
        when(resolver.concreteIndices(any(), any(ValidateQueryRequest.class))).thenReturn(resolvedIndices);

        return new TransportValidateAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
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

    private static class TestListener implements ActionListener<ValidateQueryResponse> {
        final AtomicReference<ValidateQueryResponse> response = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();

        @Override
        public void onResponse(ValidateQueryResponse r) {
            response.set(r);
        }

        @Override
        public void onFailure(Exception e) {
            failure.set(e);
        }
    }
}

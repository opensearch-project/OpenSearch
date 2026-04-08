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
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.analytics.EngineContext;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.any;
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
        TransportDslExecuteAction action = createAction(
            new Index("index-a", "uuid-a"),
            new Index("index-b", "uuid-b")
        );

        TestListener listener = executeWith(action, "multi-alias");

        assertNull(listener.response.get());
        assertNotNull(listener.failure.get());
        assertTrue(listener.failure.get() instanceof IllegalArgumentException);
        assertTrue(listener.failure.get().getMessage().contains("exactly one concrete index"));
    }

    public void testDoExecuteFailsWhenIndexNotInClusterState() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class)))
            .thenThrow(new IndexNotFoundException("bogus-index"));

        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx) -> Collections.emptyList(),
            clusterService,
            resolver
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
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(), any(SearchRequest.class))).thenReturn(resolvedIndices);

        return new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            buildEngineContext(),
            (plan, ctx) -> Collections.emptyList(),
            clusterService,
            resolver
        );
    }

    private EngineContext buildEngineContext() {
        SchemaPlus schema = buildSchema();
        return new EngineContext() {
            @Override
            public SchemaPlus getSchema() {
                return schema;
            }

            @Override
            public SqlOperatorTable operatorTable() {
                return SqlOperatorTables.of();
            }
        };
    }

    private SchemaPlus buildSchema() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test-index", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                    .add("name", SqlTypeName.VARCHAR)
                    .add("price", SqlTypeName.INTEGER)
                    .build();
            }
        });
        return schema;
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

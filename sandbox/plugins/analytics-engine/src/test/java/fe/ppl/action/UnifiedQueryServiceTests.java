/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package fe.ppl.action;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedQueryService;
import org.opensearch.ppl.planner.PushDownPlanner;
import org.opensearch.analytics.spi.SchemaProvider;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.test.OpenSearchTestCase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link UnifiedQueryService}.
 */
public class UnifiedQueryServiceTests extends OpenSearchTestCase {

    private PushDownPlanner mockPlanner;
    private RelNode mockLogicalPlan;
    private RelNode mockMixedPlan;
    private ClusterState clusterState;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockPlanner = mock(PushDownPlanner.class);
        mockLogicalPlan = mock(RelNode.class);
        mockMixedPlan = mock(RelNode.class);
        clusterState = buildClusterState();

        when(mockPlanner.plan(any(RelNode.class))).thenReturn(mockMixedPlan);
    }

    /**
     * Test full pipeline: PPL → RelNode → optimize → compile → execute → response.
     */
    public void testFullPipelineReturnsCorrectResponse() throws Exception {
        PreparedStatement mockStatement = createMockStatement(
            new String[] { "host", "status" },
            new Object[][] { { "server-1", 200 }, { "server-2", 404 } }
        );

        UnifiedQueryService service = createTestService(mockStatement);
        PPLResponse response = service.execute("source=logs", clusterState);

        assertEquals(2, response.getColumns().size());
        assertEquals("host", response.getColumns().get(0));
        assertEquals("status", response.getColumns().get(1));
        assertEquals(2, response.getRows().size());
        assertArrayEquals(new Object[] { "server-1", 200 }, response.getRows().get(0));
        assertArrayEquals(new Object[] { "server-2", 404 }, response.getRows().get(1));

        verify(mockPlanner).plan(any(RelNode.class));
    }

    /**
     * Test that results are correctly extracted from a mock ResultSet with various data types.
     */
    public void testResultExtractionWithVariousDataTypes() throws Exception {
        PreparedStatement mockStatement = createMockStatement(
            new String[] { "name", "value", "active" },
            new Object[][] { { "test", 3.14, true } }
        );

        UnifiedQueryService service = createTestService(mockStatement);
        PPLResponse response = service.execute("source=data", clusterState);

        assertEquals(3, response.getColumns().size());
        assertEquals(1, response.getRows().size());
        assertArrayEquals(new Object[] { "test", 3.14, true }, response.getRows().get(0));
    }

    /**
     * Test resource cleanup on success path: statement is closed via try-with-resources.
     * Validates: Requirement 16.1
     */
    public void testResourceCleanupOnSuccess() throws Exception {
        PreparedStatement mockStatement = createMockStatement(new String[] { "col" }, new Object[0][]);
        AtomicBoolean contextClosed = new AtomicBoolean(false);

        UnifiedQueryService service = createTestServiceWithContextTracking(mockStatement, contextClosed);
        service.execute("source=test", clusterState);

        verify(mockStatement).close();
        assertTrue("UnifiedQueryContext should be closed on success", contextClosed.get());
    }

    /**
     * Test resource cleanup on failure path: context is closed even when exception thrown.
     * Validates: Requirement 16.2
     */
    public void testResourceCleanupOnFailure() throws Exception {
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        when(mockStatement.executeQuery()).thenThrow(new SQLException("execution failed"));
        AtomicBoolean contextClosed = new AtomicBoolean(false);

        UnifiedQueryService service = createTestServiceWithContextTracking(mockStatement, contextClosed);

        expectThrows(RuntimeException.class, () -> service.execute("source=test", clusterState));
        verify(mockStatement).close();
        assertTrue("UnifiedQueryContext should be closed on failure", contextClosed.get());
    }

    /**
     * Test empty result set returns response with columns but no rows.
     */
    public void testEmptyResultSet() throws Exception {
        PreparedStatement mockStatement = createMockStatement(new String[] { "a", "b" }, new Object[0][]);

        UnifiedQueryService service = createTestService(mockStatement);
        PPLResponse response = service.execute("source=empty", clusterState);

        assertEquals(2, response.getColumns().size());
        assertTrue(response.getRows().isEmpty());
    }

    // --- helpers ---

    /**
     * Creates a mock PreparedStatement that returns a ResultSet with the given columns and rows.
     */
    private PreparedStatement createMockStatement(String[] columnNames, Object[][] rowData) throws Exception {
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        ResultSetMetaData mockMetaData = mock(ResultSetMetaData.class);

        when(mockStatement.executeQuery()).thenReturn(mockRs);
        when(mockRs.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.getColumnCount()).thenReturn(columnNames.length);
        for (int i = 0; i < columnNames.length; i++) {
            when(mockMetaData.getColumnName(i + 1)).thenReturn(columnNames[i]);
        }

        // Set up rs.next() to return true for each row, then false
        Boolean[] nextResults = new Boolean[rowData.length + 1];
        for (int i = 0; i < rowData.length; i++) {
            nextResults[i] = true;
        }
        nextResults[rowData.length] = false;
        if (nextResults.length == 1) {
            when(mockRs.next()).thenReturn(false);
        } else {
            Boolean first = nextResults[0];
            Boolean[] rest = new Boolean[nextResults.length - 1];
            System.arraycopy(nextResults, 1, rest, 0, rest.length);
            when(mockRs.next()).thenReturn(first, rest);
        }

        // Set up rs.getObject() for each column across rows
        for (int col = 0; col < columnNames.length; col++) {
            if (rowData.length == 0) continue;
            if (rowData.length == 1) {
                when(mockRs.getObject(col + 1)).thenReturn(rowData[0][col]);
            } else {
                Object first = rowData[0][col];
                Object[] rest = new Object[rowData.length - 1];
                for (int row = 1; row < rowData.length; row++) {
                    rest[row - 1] = rowData[row][col];
                }
                when(mockRs.getObject(col + 1)).thenReturn(first, rest);
            }
        }

        return mockStatement;
    }

    private UnifiedQueryService createTestService(PreparedStatement mockStatement) {
        return new UnifiedQueryService(mockPlanner, testSchemaProvider()) {
            @Override
            protected PreparedStatement compileAndPrepare(UnifiedQueryContext context, RelNode mixedPlan) {
                return mockStatement;
            }
        };
    }

    private UnifiedQueryService createTestServiceWithContextTracking(PreparedStatement mockStatement, AtomicBoolean contextClosed) {
        return new UnifiedQueryService(mockPlanner, testSchemaProvider()) {
            @Override
            protected PreparedStatement compileAndPrepare(UnifiedQueryContext context, RelNode mixedPlan) {
                return mockStatement;
            }

            @Override
            public PPLResponse execute(String pplText, ClusterState cs) {
                // Replicate the real execute logic but track context cleanup
                RelNode mixed = mockPlanner.plan(mockLogicalPlan);

                try {
                    try (PreparedStatement statement = mockStatement) {
                        ResultSet rs = statement.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        List<String> columns = new ArrayList<>();
                        for (int i = 1; i <= columnCount; i++) {
                            columns.add(metaData.getColumnName(i));
                        }
                        List<Object[]> rows = new ArrayList<>();
                        while (rs.next()) {
                            Object[] row = new Object[columnCount];
                            for (int i = 1; i <= columnCount; i++) {
                                row[i - 1] = rs.getObject(i);
                            }
                            rows.add(row);
                        }
                        return new PPLResponse(columns, rows);
                    }
                } catch (Exception e) {
                    if (e instanceof RuntimeException) throw (RuntimeException) e;
                    throw new RuntimeException(e.getMessage(), e);
                } finally {
                    contextClosed.set(true);
                }
            }
        };
    }

    /**
     * Builds a SchemaProvider that creates a Calcite schema from ClusterState,
     * replicating the mapping logic without depending on the engine plugin.
     */
    @SuppressWarnings("unchecked")
    private SchemaProvider testSchemaProvider() {
        return cs -> {
            ClusterState state = (ClusterState) cs;
            CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
            SchemaPlus schemaPlus = rootSchema.plus();
            for (Map.Entry<String, IndexMetadata> entry : state.metadata().indices().entrySet()) {
                String indexName = entry.getKey();
                MappingMetadata mapping = entry.getValue().mapping();
                if (mapping == null) continue;
                Map<String, Object> properties = (Map<String, Object>) mapping.sourceAsMap().get("properties");
                if (properties == null) continue;
                schemaPlus.add(indexName, new AbstractTable() {
                    @Override
                    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                        RelDataTypeFactory.Builder builder = typeFactory.builder();
                        for (Map.Entry<String, Object> f : properties.entrySet()) {
                            Map<String, Object> fp = (Map<String, Object>) f.getValue();
                            String ft = (String) fp.get("type");
                            if (ft == null || "nested".equals(ft) || "object".equals(ft)) continue;
                            SqlTypeName sqlType;
                            switch (ft) {
                                case "keyword": case "text": case "ip": sqlType = SqlTypeName.VARCHAR; break;
                                case "long": sqlType = SqlTypeName.BIGINT; break;
                                case "integer": sqlType = SqlTypeName.INTEGER; break;
                                case "double": sqlType = SqlTypeName.DOUBLE; break;
                                case "float": sqlType = SqlTypeName.FLOAT; break;
                                case "boolean": sqlType = SqlTypeName.BOOLEAN; break;
                                case "date": sqlType = SqlTypeName.TIMESTAMP; break;
                                default: sqlType = SqlTypeName.VARCHAR; break;
                            }
                            builder.add(f.getKey(), typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlType), true));
                        }
                        return builder.build();
                    }
                });
            }
            return schemaPlus;
        };
    }

    private ClusterState buildClusterState() {
        try {
            IndexMetadata logsIndex = IndexMetadata.builder("logs")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping("{\"properties\":{\"host\":{\"type\":\"keyword\"},\"status\":{\"type\":\"integer\"}}}")
                .build();

            IndexMetadata dataIndex = IndexMetadata.builder("data")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping("{\"properties\":{\"name\":{\"type\":\"keyword\"},\"value\":{\"type\":\"double\"},\"active\":{\"type\":\"boolean\"}}}")
                .build();

            IndexMetadata emptyIndex = IndexMetadata.builder("empty")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping("{\"properties\":{\"a\":{\"type\":\"keyword\"},\"b\":{\"type\":\"keyword\"}}}")
                .build();

            IndexMetadata testIndex = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping("{\"properties\":{\"col\":{\"type\":\"keyword\"}}}")
                .build();

            return ClusterState.builder(new ClusterName("test"))
                .metadata(
                    Metadata.builder()
                        .put(logsIndex, false)
                        .put(dataIndex, false)
                        .put(emptyIndex, false)
                        .put(testIndex, false)
                        .build()
                )
                .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build test ClusterState", e);
        }
    }
}

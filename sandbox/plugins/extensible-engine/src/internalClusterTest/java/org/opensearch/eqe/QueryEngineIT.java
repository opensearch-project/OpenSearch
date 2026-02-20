/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.eqe.action.QueryAction;
import org.opensearch.eqe.action.QueryRequest;
import org.opensearch.eqe.action.QueryResponse;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Integration tests for the query engine's streaming transport wiring
 * and end-to-end query execution via TransportQueryAction.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class QueryEngineIT extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "test_table";
    private static final int NUM_SHARDS = 2;

    @BeforeClass
    public static void setupNettyProperties() {
        System.setProperty("io.netty.allocator.numDirectArenas", "1");
        System.setProperty("io.netty.noUnsafe", "false");
        System.setProperty("io.netty.tryUnsafe", "true");
        System.setProperty("io.netty.tryReflectionSetAccessible", "true");
    }

    /**
     * Test plugin that extends ExtensibleEnginePlugin and directly sets the executor.
     * Needed because the test framework's MockNode doesn't pass extendedPlugins metadata,
     * so SPI discovery doesn't work.
     */
    public static class TestPluginQuery extends ExtensibleQueryEnginePlugin {
        @Override
        public void loadExtensions(ExtensionLoader loader) {
            this.executor = new MockExecutionEngine();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPluginQuery.class, FlightStreamPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createTestIndex();
        ensureGreen();
    }

    private void createTestIndex() {
        if (!indexExists(TEST_INDEX)) {
            createIndex(TEST_INDEX,
                Settings.builder()
                    .put("index.number_of_shards", NUM_SHARDS)
                    .put("index.number_of_replicas", 0)
                    .build(),
                "\"properties\": {"
                    + "\"name\": {\"type\": \"keyword\"},"
                    + "\"age\": {\"type\": \"long\"},"
                    + "\"score\": {\"type\": \"double\"}"
                    + "}");
        }
    }

    /**
     * Build a LogicalTableScan RelNode for test_table and serialize it as JSON.
     */
    private String buildTableScanJson() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();

        // Build schema matching the test index
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus schemaPlus = rootSchema.plus();
        schemaPlus.add(TEST_INDEX, new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                    .add("name", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("age", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), true))
                    .add("score", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.DOUBLE), true))
                    .build();
            }
        });

        // Create catalog reader and cluster
        Properties props = new Properties();
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema, Collections.singletonList(""), typeFactory, config);

        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(
            JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE));
        cluster.setMetadataQuerySupplier(RelMetadataQuery::instance);

        // Build LogicalTableScan
        RelOptTable table = catalogReader.getTable(List.of(TEST_INDEX));
        assertNotNull("Table " + TEST_INDEX + " should be found in catalog", table);
        RelNode scan = LogicalTableScan.create(cluster, table, List.of());

        // Serialize to JSON
        RelJsonWriter writer = new RelJsonWriter();
        scan.explain(writer);
        return writer.asString();
    }

    /**
     * Test end-to-end query execution via TransportQueryAction:
     * build a LogicalTableScan, serialize to JSON, invoke via client().execute(),
     * verify aggregated results from all data nodes.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testTransportQueryAction() throws Exception {
        String jsonPlan = buildTableScanJson();
        QueryRequest request = new QueryRequest(jsonPlan);
        QueryResponse response = client().execute(QueryAction.INSTANCE, request)
            .actionGet(10, TimeUnit.SECONDS);

        assertFalse("Should not have error", response.hasError());

        assertEquals("Should have 3 rows per primary shard", 3 * NUM_SHARDS, response.getRows().size());
        assertEquals(3, response.getColumns().size());
        assertEquals("name", response.getColumns().get(0));
        assertEquals("age", response.getColumns().get(1));
        assertEquals("score", response.getColumns().get(2));

        // Verify first row
        assertEquals("Alice", response.getRows().get(0)[0]);
        assertEquals(30L, response.getRows().get(0)[1]);
        assertEquals(95.5, response.getRows().get(0)[2]);
    }
}

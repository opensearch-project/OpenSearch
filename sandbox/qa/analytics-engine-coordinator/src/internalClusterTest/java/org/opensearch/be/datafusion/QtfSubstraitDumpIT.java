/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.Plan;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Diagnostic dump: drives a QTF SQL query through the full planner stack
 * (parse → CBO → QTF rewrite → DAG cut → fragment conversion) and prints both
 * the per-stage RelNode trees and the per-stage Substrait Plan bytes (parsed back
 * into proto for human-readable output).
 *
 * <p>Not a regression test — it has no asserts beyond "things didn't crash."
 * Run with {@code --info} to see the dumped plans.
 *
 * <p>Lives in the QA module because the analytics-engine plugin's test classpath
 * doesn't have substrait, but this module does.
 *
 * @opensearch.internal
 */
public class QtfSubstraitDumpIT extends OpenSearchTestCase {

    private static final Logger LOGGER = LogManager.getLogger(QtfSubstraitDumpIT.class);

    private static final String INDEX = "hits";

    public void testDumpQtfPipeline() throws Exception {
        String sql = "SELECT URL, EventDate FROM hits WHERE CounterID = 5 ORDER BY EventDate LIMIT 10";

        Map<String, Map<String, Object>> fields = new LinkedHashMap<>();
        fields.put("CounterID", Map.of("type", "integer"));
        fields.put("UserID", Map.of("type", "long"));
        fields.put("URL", Map.of("type", "keyword"));
        fields.put("Title", Map.of("type", "keyword"));
        fields.put("EventDate", Map.of("type", "date"));

        ClusterState clusterState = clusterStateWith(INDEX, fields, "parquet", 2);

        // Real Substrait extensions, loaded via the DataFusionPlugin's exact path.
        SimpleExtension.ExtensionCollection extensions = loadExtensions();

        // Stub DataFusionPlugin: only the bits getCapabilityProvider() and
        // getFragmentConvertor() touch (no native runtime, no plugin lifecycle).
        DataFusionPlugin dfPlugin = mock(DataFusionPlugin.class);
        when(dfPlugin.name()).thenReturn("datafusion");
        when(dfPlugin.getSupportedFormats()).thenReturn(List.of("parquet"));
        when(dfPlugin.getSubstraitExtensions()).thenReturn(extensions);

        DataFusionAnalyticsBackendPlugin dfBackend = new DataFusionAnalyticsBackendPlugin(dfPlugin);

        PlannerContext context = new PlannerContext(
            new CapabilityRegistry(List.of(dfBackend), FieldStorageResolver::new),
            clusterState,
            false
        );

        // Parse → CBO → DAG → forker → fragment conversion.
        RelNode parsed = parseSql(sql, clusterState);
        LOGGER.info("[QTF-DUMP] sql:\n{}", sql);
        LOGGER.info("[QTF-DUMP] parsed RelNode:\n{}", org.apache.calcite.plan.RelOptUtil.toString(parsed));

        RelNode cbo = PlannerImpl.runAllOptimizations(parsed, context);
        LOGGER.info("[QTF-DUMP] post-CBO+QTF RelNode:\n{}", org.apache.calcite.plan.RelOptUtil.toString(cbo));

        QueryDAG dag = DAGBuilder.build(cbo, context.getCapabilityRegistry(), mockClusterService());
        LOGGER.info("[QTF-DUMP] QueryDAG (pre-conversion):\n{}", dag);

        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        LOGGER.info("[QTF-DUMP] QueryDAG (post-conversion, with backend-resolved fragments):\n{}", dag);

        // Walk every stage and dump its substrait Plan(s).
        dumpSubstraitPerStage(dag.rootStage());
    }

    /**
     * Stage 0's converted Substrait base_schema must include {@code __row_id__}. The rewriter
     * adds the helper to the narrowed Scan rowType; if the override is dropped during DAG
     * cuts or fragment conversion, Stage 1's reduce plan references {@code input-0.__row_id__}
     * but the partition exposes only the physical cols and DataFusion fails registration with
     * "No field named __row_id__".
     */
    public void testStage0ScanCarriesRowIdInConvertedSubstrait() throws Exception {
        String sql = "SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10";
        QueryDAG dag = buildAndConvertQtfDag(sql);

        // Stage 0 sits two levels below the LM stage (root post-LM reduce → LM → reduce → scan).
        Stage scan = dag.rootStage().getChildStages().getFirst().getChildStages().getFirst().getChildStages().getFirst();
        byte[] bytes = scan.getPlanAlternatives().getFirst().convertedBytes();
        Plan plan = Plan.parseFrom(bytes);

        List<String> baseSchemaNames = plan.getRelations(0).getRoot().getInput().getRead().getBaseSchema().getNamesList();
        assertTrue(
            "Stage 0 base_schema must include __row_id__; got " + baseSchemaNames,
            baseSchemaNames.contains("__row_id__")
        );
    }

    /**
     * Reusable harness: parses SQL, runs the planner, builds the DAG, forks plans, and runs
     * fragment conversion. Returns the fully-converted DAG ready for per-stage assertions.
     */
    private QueryDAG buildAndConvertQtfDag(String sql) {
        Map<String, Map<String, Object>> fields = new LinkedHashMap<>();
        fields.put("CounterID", Map.of("type", "integer"));
        fields.put("UserID", Map.of("type", "long"));
        fields.put("URL", Map.of("type", "keyword"));
        fields.put("Title", Map.of("type", "keyword"));
        fields.put("EventDate", Map.of("type", "date"));
        ClusterState clusterState = clusterStateWith(INDEX, fields, "parquet", 2);

        SimpleExtension.ExtensionCollection extensions = loadExtensions();
        DataFusionPlugin dfPlugin = mock(DataFusionPlugin.class);
        when(dfPlugin.name()).thenReturn("datafusion");
        when(dfPlugin.getSupportedFormats()).thenReturn(List.of("parquet"));
        when(dfPlugin.getSubstraitExtensions()).thenReturn(extensions);
        DataFusionAnalyticsBackendPlugin dfBackend = new DataFusionAnalyticsBackendPlugin(dfPlugin);
        PlannerContext context = new PlannerContext(
            new CapabilityRegistry(List.of(dfBackend), FieldStorageResolver::new),
            clusterState,
            false
        );

        RelNode parsed = parseSql(sql, clusterState);
        RelNode cbo = PlannerImpl.runAllOptimizations(parsed, context);
        QueryDAG dag = DAGBuilder.build(cbo, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        return dag;
    }

    private void dumpSubstraitPerStage(Stage stage) throws Exception {
        LOGGER.info("[QTF-DUMP] === Stage {} ({}) — alternatives: {} ===",
            stage.getStageId(),
            stage.getExecutionType(),
            stage.getPlanAlternatives().size()
        );
        for (int i = 0; i < stage.getPlanAlternatives().size(); i++) {
            StagePlan alt = stage.getPlanAlternatives().get(i);
            byte[] bytes = alt.convertedBytes();
            if (bytes == null || bytes.length == 0) {
                LOGGER.info("[QTF-DUMP] stage {} alt[{}] backend={} — no Substrait bytes (stage type doesn't convert)",
                    stage.getStageId(), i, alt.backendId()
                );
                continue;
            }
            Plan plan = Plan.parseFrom(bytes);
            LOGGER.info("[QTF-DUMP] stage {} alt[{}] backend={} — Substrait Plan ({} bytes):\n{}",
                stage.getStageId(), i, alt.backendId(), bytes.length, plan
            );
        }
        for (Stage child : stage.getChildStages()) {
            dumpSubstraitPerStage(child);
        }
    }

    // ── parse helpers (inlined from analytics-engine's SqlPlannerTestFixture) ─────

    private static RelNode parseSql(String sql, ClusterState clusterState) {
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
            CalciteSchema.from(schema),
            Collections.singletonList(""),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties())
        );
        SqlValidator validator = SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(),
            catalogReader,
            typeFactory,
            SqlValidator.Config.DEFAULT
        );
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, new RexBuilder(typeFactory));

        SqlParser.Config parserConfig = SqlParser.config().withUnquotedCasing(Casing.UNCHANGED);
        SqlNode parsedNode;
        try {
            parsedNode = SqlParser.create(sql, parserConfig).parseQuery();
        } catch (SqlParseException e) {
            throw new AssertionError("Failed to parse SQL: " + sql, e);
        }
        SqlToRelConverter converter = new SqlToRelConverter(
            (rowType, queryString, schemaPath, viewPath) -> { throw new UnsupportedOperationException("View expansion not used"); },
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.config()
        );
        return converter.convertQuery(parsedNode, true, true).project();
    }

    private static ClusterState clusterStateWith(String indexName, Map<String, Map<String, Object>> fields, String primaryDataFormat, int shardCount) {
        try (XContentBuilder mapping = XContentBuilder.builder(MediaTypeRegistry.JSON.xContent())) {
            mapping.startObject().field("properties", fields).endObject();
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
                        .put("index.composite.primary_data_format", primaryDataFormat)
                        .putList("index.composite.secondary_data_formats", "lucene")
                )
                .numberOfShards(shardCount)
                .numberOfReplicas(0)
                .putMapping(mapping.toString())
                .build();
            Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
            return ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
        } catch (Exception e) {
            throw new AssertionError("Failed to build ClusterState", e);
        }
    }

    private static SimpleExtension.ExtensionCollection loadExtensions() {
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(QtfSubstraitDumpIT.class.getClassLoader());
            SimpleExtension.ExtensionCollection delegationExtensions = SimpleExtension.load(List.of("/delegation_functions.yaml"));
            SimpleExtension.ExtensionCollection scalarExtensions = SimpleExtension.load(List.of("/opensearch_scalar_functions.yaml"));
            SimpleExtension.ExtensionCollection arrayExtensions = SimpleExtension.load(List.of("/opensearch_array_functions.yaml"));
            SimpleExtension.ExtensionCollection aggregateExtensions = SimpleExtension.load(List.of("/opensearch_aggregate_functions.yaml"));
            SimpleExtension.ExtensionCollection roundingOverloads = SimpleExtension.load(List.of("/opensearch_rounding_overloads.yaml"));
            return DefaultExtensionCatalog.DEFAULT_COLLECTION
                .merge(delegationExtensions)
                .merge(scalarExtensions)
                .merge(arrayExtensions)
                .merge(aggregateExtensions)
                .merge(roundingOverloads);
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    @SuppressWarnings("unchecked")
    private static ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        OperationRouting routing = mock(OperationRouting.class);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.operationRouting()).thenReturn(routing);
        when(routing.searchShards(any(), any(), any(), any())).thenReturn(new GroupShardsIterator<ShardIterator>(List.of()));
        return clusterService;
    }
}

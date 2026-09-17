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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
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
import org.opensearch.action.search.TransportSearchAction;
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
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Planner-level regression for sorting by a multi-value (LIST) key across shards with a limit.
 * Drives SQL through parse → CBO → QTF → DAG cut → fragment conversion (no cluster), then for
 * every stage whose resolved fragment carries a collated {@link Sort} asserts the converted
 * Substrait still contains a SORT rel, a FETCH rel and the {@code array_min}/{@code array_max}
 * reduction on the sort key.
 *
 * <p>The unprojected {@code sort tags | head N} shape is the important one: QTF leaves the
 * anchor Sort above a coordinator {@code Project}, so {@code FragmentConversionDriver} attaches
 * it on top of the reduce stage through {@code attachFragmentOnTop} rather than converting it
 * inside a fragment. A Calcite-level rewrite that expanded the Sort into a hidden-key Project
 * sandwich did not survive that stitch (only the outer Project was spliced; Fetch, Sort and the
 * key were dropped, returning the whole table unsorted and unlimited). Live execution of the
 * same shapes is covered by {@link MultiValueSortDistributedIT}.
 */
public class MultiValueSortQtfPlanIT extends OpenSearchTestCase {
    private static final Logger LOGGER = LogManager.getLogger(MultiValueSortQtfPlanIT.class);
    private static final String INDEX = "mv";
    private static final IndexNameExpressionResolver TEST_RESOLVER = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

    /** Unprojected sort|head (QTF fires: id/region fetch-only) — the shape that lost the Sort. */
    public void testSortTagsLimit_unprojected() throws Exception {
        assertSortSurvivesEveryStage("SELECT id, tags, region FROM mv ORDER BY tags LIMIT 10", true);
    }

    /** Scalar-key control of the identical shape: same QTF split, no LIST reduction involved. */
    public void testSortScalarLimit_unprojected_control() throws Exception {
        assertSortSurvivesEveryStage("SELECT id, tags, region FROM mv ORDER BY id LIMIT 10", false);
    }

    /** Sort with a WHERE — a below-Project sits directly above the exchange. */
    public void testSortTagsLimit_withWhere() throws Exception {
        assertSortSurvivesEveryStage("SELECT id, tags FROM mv WHERE region = 'a' ORDER BY tags LIMIT 10", true);
    }

    /** Explicit projection of only the sort key + DESC — exercises the non-QTF reduce path. */
    public void testSortTagsDescLimit_projectedKeyOnly() throws Exception {
        assertSortSurvivesEveryStage("SELECT tags FROM mv ORDER BY tags DESC LIMIT 10", true);
    }

    /** Sort key not projected at all, filter on a scalar. */
    public void testSortTagsLimit_whereOnScalar_projectScalar() throws Exception {
        assertSortSurvivesEveryStage("SELECT id FROM mv WHERE id > 0 ORDER BY tags LIMIT 5", true);
    }

    private void assertSortSurvivesEveryStage(String sql, boolean expectReduction) throws Exception {
        QueryDAG dag = buildAndConvert(sql);
        List<String> failures = new ArrayList<>();
        walk(dag.rootStage(), failures, expectReduction);
        assertTrue("Sort rewrite lost in stages:\n" + String.join("\n", failures), failures.isEmpty());
    }

    private void walk(Stage stage, List<String> failures, boolean expectReduction) throws Exception {
        for (StagePlan alt : stage.getPlanAlternatives()) {
            RelNode fragment = alt.resolvedFragment();
            boolean hasCollatedSort = containsCollatedSort(fragment);
            byte[] bytes = alt.convertedBytes();
            LOGGER.debug(
                "=== Stage {} ({}) backend={} collatedSort={} ===\nRelNode:\n{}",
                stage.getStageId(),
                stage.getExecutionType(),
                alt.backendId(),
                hasCollatedSort,
                RelOptUtil.toString(fragment)
            );
            if (bytes == null || bytes.length == 0) {
                LOGGER.debug("stage {} — no Substrait bytes", stage.getStageId());
                continue;
            }
            Plan plan = Plan.parseFrom(bytes);
            String text = plan.toString();
            LOGGER.debug("stage {} Substrait:\n{}", stage.getStageId(), text);
            if (hasCollatedSort) {
                Rel root = plan.getRelations(0).getRoot().getInput();
                boolean hasSort = contains(root, Rel.RelTypeCase.SORT);
                boolean hasFetch = contains(root, Rel.RelTypeCase.FETCH);
                boolean hasReduction = !expectReduction || text.contains("array_min") || text.contains("array_max");
                LOGGER.debug("stage {} hasSort={} hasFetch={} hasReduction={}", stage.getStageId(), hasSort, hasFetch, hasReduction);
                if (!hasSort || !hasFetch || !hasReduction) {
                    failures.add(
                        "stage "
                            + stage.getStageId()
                            + " ("
                            + stage.getExecutionType()
                            + "): hasSort="
                            + hasSort
                            + " hasFetch="
                            + hasFetch
                            + " hasReduction="
                            + hasReduction
                            + "\nRelNode:\n"
                            + RelOptUtil.toString(fragment)
                            + "\nSubstrait:\n"
                            + text
                    );
                }
            }
        }
        for (Stage child : stage.getChildStages()) {
            walk(child, failures, expectReduction);
        }
    }

    private static boolean containsCollatedSort(RelNode node) {
        if (node instanceof Sort sort && !sort.getCollation().getFieldCollations().isEmpty()) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (containsCollatedSort(input)) return true;
        }
        return false;
    }

    private static boolean contains(Rel rel, Rel.RelTypeCase type) {
        if (rel.getRelTypeCase() == type) return true;
        return switch (rel.getRelTypeCase()) {
            case FETCH -> contains(rel.getFetch().getInput(), type);
            case SORT -> contains(rel.getSort().getInput(), type);
            case PROJECT -> contains(rel.getProject().getInput(), type);
            case FILTER -> contains(rel.getFilter().getInput(), type);
            case AGGREGATE -> contains(rel.getAggregate().getInput(), type);
            default -> false;
        };
    }

    private QueryDAG buildAndConvert(String sql) {
        Map<String, Map<String, Object>> fields = new LinkedHashMap<>();
        fields.put("id", Map.of("type", "integer"));
        fields.put("tags", Map.of("type", "keyword", "index", "false", "multi_value", true));
        fields.put("region", Map.of("type", "keyword", "index", "false"));
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
        LOGGER.debug("sql: {}\nparsed:\n{}", sql, RelOptUtil.toString(parsed));
        RelNode cbo = PlannerImpl.runAllOptimizations(parsed, context);
        LOGGER.debug("post-CBO+QTF+pushdown:\n{}", RelOptUtil.toString(cbo));
        QueryDAG dag = DAGBuilder.build(cbo, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        return dag;
    }

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
                        .putList("index.composite.secondary_data_formats")
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
            t.setContextClassLoader(MultiValueSortQtfPlanIT.class.getClassLoader());
            SimpleExtension.ExtensionCollection delegationExtensions = SimpleExtension.load(List.of("/delegation_functions.yaml"));
            SimpleExtension.ExtensionCollection scalarExtensions = SimpleExtension.load(List.of("/opensearch_scalar_functions.yaml"));
            SimpleExtension.ExtensionCollection arrayExtensions = SimpleExtension.load(List.of("/opensearch_array_functions.yaml"));
            SimpleExtension.ExtensionCollection aggregateExtensions = SimpleExtension.load(List.of("/opensearch_aggregate_functions.yaml"));
            return DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(delegationExtensions)
                .merge(scalarExtensions)
                .merge(arrayExtensions)
                .merge(aggregateExtensions);
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
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }
}

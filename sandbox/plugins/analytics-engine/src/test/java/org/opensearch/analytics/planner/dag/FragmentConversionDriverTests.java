/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.MockDataFusionBackend;
import org.opensearch.analytics.planner.MockLuceneBackend;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateCallAnnotation;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.InstructionType;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link FragmentConversionDriver} — verifies annotations are stripped
 * and the correct {@link FragmentConvertor} method is called per leaf type,
 * across a range of query shapes.
 */
public class FragmentConversionDriverTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(FragmentConversionDriverTests.class);

    private static final Set<String> OPENSEARCH_OPERATORS = Set.of(
        OpenSearchFilter.class.getSimpleName(),
        OpenSearchAggregate.class.getSimpleName(),
        OpenSearchTableScan.class.getSimpleName(),
        OpenSearchSort.class.getSimpleName(),
        OpenSearchProject.class.getSimpleName()
    );

    private static final Set<String> ANNOTATION_MARKERS = Set.of(
        AnnotatedPredicate.class.getSimpleName(),
        AggregateCallAnnotation.class.getSimpleName(),
        AnnotatedProjectExpression.class.getSimpleName()
    );

    // ---- Helpers ----

    private MockDataFusionBackend dfWithConvertor(RecordingConvertor convertor) {
        return new MockDataFusionBackend() {
            @Override
            public FragmentConvertor getFragmentConvertor() {
                return convertor;
            }
        };
    }

    private QueryDAG buildAndConvert(int shardCount, RelNode logicalPlan, RecordingConvertor convertor) {
        var df = dfWithConvertor(convertor);
        var context = buildContext("parquet", shardCount, intFields(), List.of(df));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        LOGGER.info("QueryDAG after conversion:\n{}", dag);
        return dag;
    }

    private void assertDoesntContainOperators(RelNode fragment, Set<String> forbidden) {
        assertNotNull(fragment);
        String planStr = RelOptUtil.toString(fragment);
        LOGGER.info("Stripped fragment:\n{}", planStr);
        for (String op : forbidden)
            assertFalse("Stripped fragment must not contain [" + op + "]", planStr.contains(op));
    }

    private void assertShardScanConverted(RecordingConvertor convertor, Stage stage) {
        assertEquals("expected exactly one alternative", 1, stage.getPlanAlternatives().size());
        assertNotNull("convertedBytes must be set", stage.getPlanAlternatives().getFirst().convertedBytes());
        assertTrue("convertShardScanFragment must be called", convertor.shardScanCalled);
        assertEquals("test_index", convertor.shardScanTableName);
        assertDoesntContainOperators(convertor.shardScanFragment, OPENSEARCH_OPERATORS);
        assertDoesntContainOperators(convertor.shardScanFragment, ANNOTATION_MARKERS);
        // Instruction assertions
        StagePlan plan = stage.getPlanAlternatives().getFirst();
        assertFalse("instructions must not be empty", plan.instructions().isEmpty());
        assertEquals("first instruction must be SHARD_SCAN", InstructionType.SETUP_SHARD_SCAN, plan.instructions().getFirst().type());
    }

    private void assertReduceStageConverted(RecordingConvertor convertor, Stage stage) {
        assertEquals("expected exactly one alternative", 1, stage.getPlanAlternatives().size());
        assertNotNull("convertedBytes must be set", stage.getPlanAlternatives().getFirst().convertedBytes());
        assertTrue("convertFinalAggFragment must be called", convertor.finalAggCalled);
        assertDoesntContainOperators(convertor.reduceFragment, OPENSEARCH_OPERATORS);
        assertDoesntContainOperators(convertor.reduceFragment, ANNOTATION_MARKERS);
        // Coord-side reduce stages no longer register FinalAggregateInstructionHandler.
        // DataFusion plans the substrait Aggregate's Partial+Final pair itself via the legacy
        // executeLocalPlan path; the previous SETUP_FINAL_AGGREGATE instruction routed through
        // Rust's apply_aggregate_mode strip, which corrupted column refs (cnt[sum]/cnt[count]).
        StagePlan plan = stage.getPlanAlternatives().getFirst();
        assertTrue("coord-side reduce instructions must be empty", plan.instructions().isEmpty());
    }

    // ---- Single-stage query shapes ----

    /**
     * Scan, Filter(Scan), Aggregate(Scan), Sort(Filter(Scan)) — single-shard plans now
     * have a coord stage above the data-node stage (since scans declare RANDOM, the
     * coord must gather). The test verifies convertShardScanFragment is called on the
     * data-node child stage and the fragment is fully stripped.
     */
    public void testSingleStageQueryShapes() {
        RecordingConvertor scanConvertor = new RecordingConvertor();
        QueryDAG scanDag = buildAndConvert(1, stubScan(mockTable("test_index", "status", "size")), scanConvertor);
        assertShardScanConverted(scanConvertor, dataNodeStage(scanDag));

        RecordingConvertor filterConvertor = new RecordingConvertor();
        QueryDAG filterDag = buildAndConvert(
            1,
            LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
            filterConvertor
        );
        assertShardScanConverted(filterConvertor, dataNodeStage(filterDag));

        RecordingConvertor aggConvertor = new RecordingConvertor();
        QueryDAG aggDag = buildAndConvert(1, makeAggregate(sumCall()), aggConvertor);
        assertShardScanConverted(aggConvertor, dataNodeStage(aggDag));

        RecordingConvertor sortConvertor = new RecordingConvertor();
        QueryDAG sortDag = buildAndConvert(
            1,
            makeSort(makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)), 10),
            sortConvertor
        );
        assertShardScanConverted(sortConvertor, dataNodeStage(sortDag));
    }

    /** Walks to the deepest leaf stage (the data-node fragment) — used by tests that
     *  assert SHARD_SCAN behavior, which lives on the data-node side regardless of
     *  whether a coord stage sits above. */
    private static Stage dataNodeStage(QueryDAG dag) {
        Stage current = dag.rootStage();
        while (!current.getChildStages().isEmpty()) {
            current = current.getChildStages().get(0);
        }
        return current;
    }

    // ---- Composed pipeline shapes ----

    /** Aggregate(Filter(Scan)) — most common OLAP shape. */
    public void testAggregateOnFilteredScan() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(
            1,
            makeAggregate(
                makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                sumCall()
            ),
            convertor
        );
        assertShardScanConverted(convertor, dataNodeStage(dag));
    }

    /** Sort(Aggregate(Filter(Scan))) with limit — full OLAP pipeline. */
    public void testSortOnAggregateOnFilteredScan() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(
            1,
            makeSort(
                makeAggregate(
                    makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                    sumCall()
                ),
                10
            ),
            convertor
        );
        assertShardScanConverted(convertor, dataNodeStage(dag));
    }

    // ---- Two-stage shapes ----

    /**
     * Multi-shard Aggregate(Scan) — child calls convertShardScanFragment,
     * root calls convertFinalAggFragment.
     */
    public void testTwoStageAggregateConversion() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(2, makeAggregate(sumCall()), convertor);

        assertEquals(1, dag.rootStage().getChildStages().size());
        assertReduceStageConverted(convertor, dag.rootStage());
        assertShardScanConverted(convertor, dag.rootStage().getChildStages().getFirst());
    }

    /**
     * Multi-shard Sort(Aggregate(Filter(Scan))) with limit — full OLAP pipeline, two stages.
     */
    public void testTwoStageSortOnAggregateOnFilteredScan() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(
            2,
            makeSort(
                makeAggregate(
                    makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                    sumCall()
                ),
                10
            ),
            convertor
        );

        assertEquals(1, dag.rootStage().getChildStages().size());
        assertReduceStageConverted(convertor, dag.rootStage());
        assertShardScanConverted(convertor, dag.rootStage().getChildStages().getFirst());
    }

    // ---- Multi-input (join) coord fragment shapes ----

    /**
     * Coord-side fragment: Aggregate ← Join ← (ER ← ...) | (ER ← ...).
     * Both branches are gathered subtrees. convertReduceNode must convert the whole Join +
     * branches + ERs + StageInputScans subtree in a single {@code convertFinalAggFragment}
     * pass — same path as Union / Intersect / Minus. No substrait-level join stitching.
     */
    public void testJoinDirectlyOverTwoExchanges() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(2, buildJoinOverTwoScans("test_index", "test_index"), convertor);

        // Find the coord-side join stage — the stage whose fragment contains the Join with
        // two exchange-gathered branches. The whole subtree converts in one pass.
        Stage joinStage = findStageWithTwoChildren(dag.rootStage());
        assertNotNull("expected a stage with 2 child stages (the coord-side Join stage)", joinStage);
        assertNotNull("join stage alternative must have convertedBytes", joinStage.getPlanAlternatives().getFirst().convertedBytes());
        assertTrue("convertFinalAggFragment must be called for the Join subtree", convertor.finalAggCalled);
    }

    private static Stage findStageWithTwoChildren(Stage stage) {
        if (stage.getChildStages().size() == 2) return stage;
        for (Stage child : stage.getChildStages()) {
            Stage found = findStageWithTwoChildren(child);
            if (found != null) return found;
        }
        return null;
    }

    /**
     * Coord-side Union with pass-through operators (Sort/Project) between each arm and its
     * ER. Isthmus's SubstraitRelVisitor handles Union natively; convertReduceNode converts
     * the whole Union subtree as one convertFinalAggFragment call — same path as Join.
     */
    public void testUnionOverPassthroughThenExchange() {
        RecordingConvertor convertor = new RecordingConvertor();
        MockDataFusionBackend dfWithUnion = new MockDataFusionBackend() {
            @Override
            protected Set<EngineCapability> supportedEngineCapabilities() {
                Set<EngineCapability> caps = new java.util.HashSet<>(super.supportedEngineCapabilities());
                caps.add(EngineCapability.UNION);
                return caps;
            }

            @Override
            public org.opensearch.analytics.spi.FragmentConvertor getFragmentConvertor() {
                return convertor;
            }
        };
        PlannerContext context = buildContext("parquet", 2, intFields(), List.of(dfWithUnion));
        RelNode logical = buildUnionOverSortedAggArms();
        RelNode cboOutput = runPlanner(logical, context);
        LOGGER.info("Marked+CBO:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());

        Stage root = dag.rootStage();
        assertNotNull("root alternative must have convertedBytes", root.getPlanAlternatives().getFirst().convertedBytes());
        assertTrue("convertFinalAggFragment must be called for the Union subtree", convertor.finalAggCalled);
    }

    /**
     * Builds a minimal shape that reproduces the AppendPipeCommandIT plan: a LogicalUnion
     * over two arms, each arm being {@code Sort ← Project ← Aggregate ← Scan} of the same
     * multi-shard table. After CBO each arm carries an ER above its aggregate (PARTIAL/FINAL
     * split or SINGLE-over-RANDOM gather), and the Union sits at the coord with two
     * pass-through operators above each ER.
     */
    private RelNode buildUnionOverSortedAggArms() {
        RelNode arm1 = buildSortedAggArm();
        RelNode arm2 = buildSortedAggArm();
        return LogicalUnion.create(List.of(arm1, arm2), true);
    }

    private RelNode buildSortedAggArm() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode project = LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1)),
            List.of("status", "size")
        );
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            project,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), false),
            "s"
        );
        RelNode agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall));
        return LogicalSort.create(
            agg,
            org.apache.calcite.rel.RelCollations.of(new org.apache.calcite.rel.RelFieldCollation(0)),
            null,
            null
        );
    }

    /**
     * Builds an After-HEP shape close to JoinCommandIT#testInnerJoin: a top-level
     * count Aggregate over a Join of two separately-aggregated, separately-projected
     * scans. After CBO each join side carries an ER above its partial-agg subtree.
     */
    private RelNode buildJoinOverTwoScans(String leftTable, String rightTable) {
        RelOptTable left = mockTable(leftTable, "status", "size");
        RelOptTable right = mockTable(rightTable, "status", "size");

        RelNode leftScan = stubScan(left);
        RelNode leftProject = LogicalProject.create(
            leftScan,
            List.of(),
            List.of(rexBuilder.makeInputRef(leftScan, 0), rexBuilder.makeInputRef(leftScan, 1)),
            List.of("status", "size")
        );
        RelNode rightScan = stubScan(right);
        RelNode rightProject = LogicalProject.create(
            rightScan,
            List.of(),
            List.of(rexBuilder.makeInputRef(rightScan, 0), rexBuilder.makeInputRef(rightScan, 1)),
            List.of("status", "size")
        );
        RelNode rightSorted = LogicalSort.create(
            rightProject,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(50000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode join = LogicalJoin.create(leftProject, rightSorted, List.of(), cond, Set.of(), JoinRelType.INNER);

        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            join,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return LogicalAggregate.create(join, List.of(), ImmutableBitSet.of(), null, List.of(countCall));
    }

    // ---- Join conversion tests (Gap #1 — M0 coordinator-centric) ----

    /**
     * INNER equi-join on column 0 of both sides. Two single-column {@code LogicalTableScan}s →
     * {@code LogicalJoin} → planner marks as {@code OpenSearchJoin} over two
     * {@code OpenSearchExchangeReducer} legs, each reducing a separate scan subtree.
     */
    private LogicalJoin makeInnerEquiJoin() {
        RelNode leftScan = stubScan(mockTable("test_index", "status", "size"));
        RelNode rightScan = stubScan(mockTable("test_index", "status", "size"));
        int leftCols = leftScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftCols)
        );
        return LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), JoinRelType.INNER);
    }

    /**
     * Drives a coordinator-centric equi-join all the way through {@link FragmentConversionDriver}
     * and asserts the recording convertor sees:
     * <ul>
     *   <li>{@code convertShardScanFragment("test_index", LogicalTableScan)} called once per side
     *       (two child stages).</li>
     *   <li>{@code convertFinalAggFragment(LogicalJoin(StageInputScan, StageInputScan))} called
     *       once on the root stage — the OpenSearchJoin wrapper and the two ExchangeReducers
     *       were stripped by {@code FragmentConversionDriver.convertReduceNode}, leaving a plain
     *       {@code LogicalJoin} Isthmus can walk natively.</li>
     * </ul>
     *
     * <p>This is Gap #1's end-to-end coverage anchor: if something in the planner/DAG stack
     * emits a shape that doesn't reduce cleanly to {@code LogicalJoin(SIS, SIS)} here, the test
     * catches it before it reaches the DataFusion runtime.
     */
    public void testCoordinatorCentricJoinConversion() {
        RecordingConvertor convertor = new RecordingConvertor();
        // Multi-shard so PR #21639's split rule demands COORDINATOR+SINGLETON on each input,
        // producing OpenSearchJoin(ER(scan), ER(scan)) with two child stages — the coord-centric
        // shape this test was designed to verify. Single-shard same-table joins go SHARD-local
        // under PR's design (one stage, no ERs) and don't exercise the coord-centric path.
        QueryDAG dag = buildAndConvert(3, makeInnerEquiJoin(), convertor);

        assertEquals("Binary join → exactly two child stages", 2, dag.rootStage().getChildStages().size());

        // Both child stages converted as shard scans.
        for (Stage child : dag.rootStage().getChildStages()) {
            assertNotNull(child.getPlanAlternatives().getFirst().convertedBytes());
        }
        assertTrue("convertShardScanFragment must be called for the scan side(s)", convertor.shardScanCalled);
        assertEquals("test_index", convertor.shardScanTableName);

        // Root stage: convertFinalAggFragment called on the stripped join fragment.
        assertTrue("convertFinalAggFragment must be called on the coordinator join fragment", convertor.finalAggCalled);
        assertNotNull(convertor.reduceFragment);

        // The recorded fragment must be a plain LogicalJoin (OpenSearch* wrappers stripped) whose
        // two inputs are StageInputScan leaves — that's the shape Isthmus consumes natively.
        String recorded = RelOptUtil.toString(convertor.reduceFragment);
        LOGGER.info("Recorded reduce fragment:\n{}", recorded);
        assertDoesntContainOperators(convertor.reduceFragment, OPENSEARCH_OPERATORS);
        assertDoesntContainOperators(convertor.reduceFragment, ANNOTATION_MARKERS);
        assertTrue(
            "Recorded reduce fragment must be rooted at a LogicalJoin (got:\n" + recorded + ")",
            recorded.contains("LogicalJoin")
        );
        assertTrue(
            "Recorded reduce fragment must carry two OpenSearchStageInputScan leaves (got:\n" + recorded + ")",
            recorded.split("OpenSearchStageInputScan", -1).length - 1 == 2
        );
    }

    /**
     * Multi-shard variant: each join side scans a 5-shard index. Conversion shape is identical;
     * the number of shards only affects target resolution at dispatch time, not the conversion
     * pipeline. This pins the invariant so future shard-count-dependent changes don't silently
     * alter the coordinator-side conversion path.
     */
    public void testCoordinatorCentricJoinConversionMultiShard() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(5, makeInnerEquiJoin(), convertor);

        assertEquals(2, dag.rootStage().getChildStages().size());
        assertTrue(convertor.shardScanCalled);
        assertTrue(convertor.finalAggCalled);
        String recorded = RelOptUtil.toString(convertor.reduceFragment);
        assertTrue(recorded.contains("LogicalJoin"));
        assertEquals(
            "Recorded reduce fragment must carry two StageInputScan leaves (one per reducer side)",
            2,
            recorded.split("OpenSearchStageInputScan", -1).length - 1
        );
    }

    // ---- Left-build broadcast conversion ----

    /**
     * Regression: probe-side fragment with shape {@code Join(OpenSearchBroadcastScan, OpenSearchTableScan)}
     * (build = LEFT) must classify as a shard-scan stage, not throw "Unknown leaf type" on
     * conversion. Earlier {@code findLeaf} returned the broadcast placeholder for the left
     * input, leaving {@code convert()} unable to route the fragment.
     */
    public void testLeftBuildProbeFragmentRoutesToShardScanConversion() {
        // Construct probe-stage fragment manually: Join(BroadcastScan, TableScan).
        // We use a minimal cluster + scan to avoid driving the full advisor/rewriter chain —
        // the goal is to exercise FragmentConversionDriver.convert against the new shape.
        // The harness only knows test_index; reuse it for both probe and a placeholder
        // build-side row type (any OpenSearchTableScan would do).
        org.opensearch.analytics.planner.rel.OpenSearchTableScan probeScan =
            (org.opensearch.analytics.planner.rel.OpenSearchTableScan) markScan(stubScan(mockTable("test_index", "status", "size")));
        org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan broadcastScan =
            new org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan(
                probeScan.getCluster(),
                probeScan.getTraitSet(),
                /* buildStageId */ 0,
                probeScan.getRowType(),
                probeScan.getViableBackends()
            );
        int leftCols = broadcastScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftCols)
        );
        org.opensearch.analytics.planner.rel.OpenSearchJoin probeJoin =
            new org.opensearch.analytics.planner.rel.OpenSearchJoin(
                probeScan.getCluster(),
                probeScan.getTraitSet(),
                broadcastScan,                  // build = LEFT input
                probeScan,                      // probe scan = RIGHT input
                condition,
                JoinRelType.INNER,
                probeScan.getViableBackends()
            );

        RecordingConvertor convertor = new RecordingConvertor();
        FragmentConversionDriver.IntraOperatorDelegationBytes delegationBytes =
            new FragmentConversionDriver.IntraOperatorDelegationBytes(
                buildContext("parquet", 1, intFields(), List.of(dfWithConvertor(convertor))).getCapabilityRegistry()
            );

        // Must not throw "Unknown leaf type" — findLeaf must skip the broadcast placeholder
        // and recurse into the right input (the probe scan).
        byte[] bytes = FragmentConversionDriver.convert(probeJoin, convertor, delegationBytes);
        assertNotNull(bytes);
        assertTrue(convertor.shardScanCalled);
        assertEquals("table name from probe scan, not broadcast placeholder", "test_index", convertor.shardScanTableName);
    }

    /** Mark a logical scan via the planner so it becomes OpenSearchTableScan. */
    private RelNode markScan(RelNode logicalScan) {
        org.opensearch.analytics.planner.PlannerContext ctx = buildContext("parquet", 1, intFields());
        return runPlanner(logicalScan, ctx);
    }

    // ---- Delegation tagging tests ----

    private static final SqlFunction MATCH_PHRASE_FUNCTION = new SqlFunction(
        "MATCH_PHRASE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final SqlFunction FUZZY_FUNCTION = new SqlFunction(
        "FUZZY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /** Records serialization calls for delegation tests. */
    private static class RecordingSerializer implements DelegatedPredicateSerializer {
        int callCount;
        final List<String> serializedFunctions = new ArrayList<>();

        @Override
        public byte[] serialize(RexCall call, List<FieldStorageInfo> fieldStorage) {
            callCount++;
            serializedFunctions.add(call.getOperator().getName());
            return ("delegated:" + call.getOperator().getName()).getBytes(StandardCharsets.UTF_8);
        }
    }

    private List<AnalyticsSearchBackendPlugin> delegationBackends(RecordingConvertor dfConvertor, RecordingSerializer serializer) {
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            protected Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                map.put(ScalarFunction.MATCH, serializer);
                map.put(ScalarFunction.WILDCARD, serializer);
                map.put(ScalarFunction.REGEXP, serializer);
                return map;
            }
        };
        return List.of(df, lucene);
    }

    private QueryDAG buildDelegationDag(
        RexNode condition,
        RecordingConvertor dfConvertor,
        RecordingSerializer serializer,
        String[] fieldNames,
        SqlTypeName[] fieldTypes,
        Map<String, Map<String, Object>> fields
    ) {
        var backends = delegationBackends(dfConvertor, serializer);
        var context = buildContext("parquet", fields, backends);
        LogicalFilter filter = LogicalFilter.create(stubScan(mockTable("test_index", fieldNames, fieldTypes)), condition);
        RelNode cboOutput = runPlanner(filter, context);
        LOGGER.info("Marked+CBO:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        return dag;
    }

    /** Single-field delegation helper. */
    private QueryDAG buildSingleFieldDelegationDag(RexNode condition, RecordingConvertor dfConvertor, RecordingSerializer serializer) {
        return buildDelegationDag(
            condition,
            dfConvertor,
            serializer,
            new String[] { "message" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            Map.of("message", Map.of("type", "keyword", "index", true))
        );
    }

    /** Two-field delegation helper (integer status + keyword message). */
    private QueryDAG buildTwoFieldDelegationDag(RexNode condition, RecordingConvertor dfConvertor, RecordingSerializer serializer) {
        return buildDelegationDag(
            condition,
            dfConvertor,
            serializer,
            new String[] { "status", "message" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR },
            Map.of("status", Map.of("type", "integer", "index", true), "message", Map.of("type", "keyword", "index", true))
        );
    }

    // ---- Shared delegation assertions ----

    private static Stage leafStage(QueryDAG dag) {
        Stage stage = dag.rootStage();
        while (!stage.getChildStages().isEmpty()) {
            stage = stage.getChildStages().getFirst();
        }
        return stage;
    }

    private void assertDelegationResult(
        StagePlan plan,
        RecordingConvertor dfConvertor,
        RecordingSerializer serializer,
        int expectedDelegatedCount,
        boolean expectPlaceholder,
        boolean expectNativeEquals,
        List<String> expectedFunctions,
        FilterTreeShape expectedTreeShape
    ) {
        assertEquals("delegatedQueries count", expectedDelegatedCount, plan.delegatedExpressions().size());
        assertEquals("serializer call count", expectedDelegatedCount, serializer.callCount);
        assertEquals("serialized functions", expectedFunctions, serializer.serializedFunctions);

        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        LOGGER.info("Stripped plan:\n{}", strippedPlan);

        if (expectPlaceholder) {
            assertTrue(
                "Stripped plan should contain " + DelegatedPredicateFunction.NAME,
                strippedPlan.contains(DelegatedPredicateFunction.NAME)
            );
            assertFalse("Stripped plan should not contain MATCH_PHRASE", strippedPlan.contains("MATCH_PHRASE"));
            assertFalse("Stripped plan should not contain FUZZY", strippedPlan.contains("FUZZY"));
        } else {
            assertFalse(
                "Stripped plan should not contain " + DelegatedPredicateFunction.NAME,
                strippedPlan.contains(DelegatedPredicateFunction.NAME)
            );
        }

        if (expectNativeEquals) {
            assertTrue("Stripped plan should contain native equals", strippedPlan.contains("="));
        }

        // No annotation markers should survive stripping
        assertDoesntContainOperators(dfConvertor.shardScanFragment, ANNOTATION_MARKERS);

        // Instruction assertions: delegation plans must have SHARD_SCAN + FILTER_DELEGATION_FOR_INDEX
        if (expectedDelegatedCount > 0) {
            assertTrue(
                "delegation plan must have SHARD_SCAN_WITH_DELEGATION instruction",
                plan.instructions().stream().anyMatch(node -> node.type() == InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION)
            );
            ShardScanWithDelegationInstructionNode filterInstruction = (ShardScanWithDelegationInstructionNode) plan.instructions()
                .stream()
                .filter(node -> node.type() == InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION)
                .findFirst()
                .orElseThrow();
            assertEquals("delegatedPredicateCount in instruction", expectedDelegatedCount, filterInstruction.getDelegatedPredicateCount());
            assertEquals(
                "delegatedPredicateCount matches delegatedExpressions size",
                plan.delegatedExpressions().size(),
                filterInstruction.getDelegatedPredicateCount()
            );
            assertEquals("treeShape in instruction", expectedTreeShape, filterInstruction.getTreeShape());
        }
    }

    // ---- Single predicate ----

    /** Single delegated MATCH_PHRASE — replaced with placeholder, one entry in delegatedQueries. */
    public void testSingleDelegatedPredicate() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildSingleFieldDelegationDag(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello world"), dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(plan, dfConvertor, serializer, 1, true, false, List.of("MATCH_PHRASE"), FilterTreeShape.CONJUNCTIVE);
    }

    /** Single native equals — no delegation, empty delegatedQueries. */
    public void testSingleNativePredicate() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildTwoFieldDelegationDag(makeEquals(0, SqlTypeName.INTEGER, 200), dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(plan, dfConvertor, serializer, 0, false, true, List.of(), FilterTreeShape.NO_DELEGATION);
    }

    // ---- AND conditions ----

    /** AND(native, delegated) — equals unwrapped, MATCH_PHRASE replaced. */
    public void testAndNativeAndDelegated() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildTwoFieldDelegationDag(
            makeAnd(makeEquals(0, SqlTypeName.INTEGER, 200), makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "timeout error")),
            dfConvertor,
            serializer
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(plan, dfConvertor, serializer, 1, true, true, List.of("MATCH_PHRASE"), FilterTreeShape.CONJUNCTIVE);
    }

    /** AND(delegated, delegated) — both replaced, two entries in delegatedQueries. */
    public void testAndTwoDelegated() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildSingleFieldDelegationDag(
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")),
            dfConvertor,
            serializer
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(
            plan,
            dfConvertor,
            serializer,
            2,
            true,
            false,
            List.of("MATCH_PHRASE", "FUZZY"),
            FilterTreeShape.CONJUNCTIVE
        );
    }

    // ---- OR conditions ----

    /** OR(native, delegated) — structure preserved, delegated replaced. */
    public void testOrNativeAndDelegated() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildTwoFieldDelegationDag(
            rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
                makeEquals(0, SqlTypeName.INTEGER, 200),
                makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "timeout error")
            ),
            dfConvertor,
            serializer
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(
            plan,
            dfConvertor,
            serializer,
            1,
            true,
            true,
            List.of("MATCH_PHRASE"),
            FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION
        );
        assertTrue("OR structure should be preserved", RelOptUtil.toString(dfConvertor.shardScanFragment).contains("OR"));
    }

    // ---- Interleaved AND/OR/NOT ----

    /** AND(native, OR(delegated, NOT(delegated))) — nested boolean structure with delegation. */
    public void testInterleavedAndOrNot() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        RexNode notFuzzy = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT,
            makeFullTextCall(FUZZY_FUNCTION, 1, "wrld")
        );
        RexNode orClause = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "timeout error"),
            notFuzzy
        );
        RexNode condition = makeAnd(makeEquals(0, SqlTypeName.INTEGER, 200), orClause);
        QueryDAG dag = buildTwoFieldDelegationDag(condition, dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(plan, dfConvertor, serializer, 2, true, true, List.of("MATCH_PHRASE", "FUZZY"), FilterTreeShape.CONJUNCTIVE);
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        assertTrue("AND structure should be preserved", strippedPlan.contains("AND"));
        assertTrue("OR structure should be preserved", strippedPlan.contains("OR"));
        assertTrue("NOT structure should be preserved", strippedPlan.contains("NOT"));
    }

    // ---- Error paths ----

    /** Delegated annotation with no serializer registered → IllegalStateException. */
    public void testMissingSerializerThrows() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        // Lucene mock accepts delegation but has NO serializers at all
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }
        };
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        PlannerContext context = buildContext("parquet", fields, List.of(df, lucene));
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello world")
        );
        RelNode marked = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry())
        );
        assertTrue(exception.getMessage().contains("No DelegatedPredicateSerializer"));
        assertTrue(exception.getMessage().contains("MATCH_PHRASE"));
    }

    // ---- RecordingConvertor ----

    /** Records which convertor method was called and what was passed. */
    private static class RecordingConvertor implements FragmentConvertor {
        boolean shardScanCalled;
        boolean finalAggCalled;
        String shardScanTableName;
        RelNode shardScanFragment;
        RelNode reduceFragment;

        @Override
        public byte[] convertShardScanFragment(String tableName, RelNode fragment) {
            this.shardScanCalled = true;
            this.shardScanTableName = tableName;
            this.shardScanFragment = fragment;
            return ("shard:" + tableName).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] convertFinalAggFragment(RelNode fragment) {
            this.finalAggCalled = true;
            this.reduceFragment = fragment;
            return "reduce".getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
            return ("attach:" + new String(innerBytes, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
            return ("partialAgg:" + new String(innerBytes, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        }
    }
}

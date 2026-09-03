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
import org.apache.calcite.rex.RexUtil;
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
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegatedSubtreeConvertor;
import org.opensearch.analytics.spi.DelegationPossibleFunction;
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

    // Operator markers that carry annotations and must be stripped before isthmus conversion.
    // OpenSearchTableScan is intentionally NOT in this set: stripAnnotations leaves it in place
    // because it carries no annotations and isthmus only reads its rowType + qualified name.
    // Stripping it to LogicalTableScan would silently drop QTF's overrideRowType (helper cols
    // like __row_id__) — see OpenSearchTableScan#stripAnnotations.
    private static final Set<String> OPENSEARCH_OPERATORS = Set.of(
        OpenSearchFilter.class.getSimpleName(),
        OpenSearchAggregate.class.getSimpleName(),
        OpenSearchSort.class.getSimpleName(),
        OpenSearchProject.class.getSimpleName()
    );

    // AggregateCallAnnotation is stored on OpenSearchAggregate as a side-map (not in
    // aggCall.rexList), so it never appears in the plan's textual representation —
    // hence it's not in this set. AnnotatedPredicate and AnnotatedProjectExpression
    // still ride in their parent operators' RexNodes and DO appear textually.
    private static final Set<String> ANNOTATION_MARKERS = Set.of(
        AnnotatedPredicate.class.getSimpleName(),
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
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
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
        assertTrue("convertFragment (shard-scan shape) must be called", convertor.shardScanCalled);
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
        assertTrue("convertFragment (final-agg shape) must be called", convertor.finalAggCalled);
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
     * coord must gather). The test verifies convertFragment (shard-scan shape) is called on the
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
     * Multi-shard Aggregate(Scan) — child calls convertFragment (shard-scan shape),
     * root calls convertFragment (final-agg shape).
     */
    public void testTwoStageAggregateConversion() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(2, makeAggregate(sumCall()), convertor);

        assertEquals(1, dag.rootStage().getChildStages().size());
        assertReduceStageConverted(convertor, dag.rootStage());
        assertShardScanConverted(convertor, dag.rootStage().getChildStages().getFirst());
    }

    /**
     * PARTIAL aggregate stages must emit SETUP_PARTIAL_AGGREGATE so the shard executor
     * strips the DF Final layer. Without this, slice_count > 1 causes FinalPartitioned
     * to hash-repartition groups within the shard, producing incorrect cross-shard merges.
     */
    public void testPartialAggregateInstruction_emittedForCountGroupBy() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(2, makeAggregate(countStarCall()), convertor);
        assertShardHasPartialAggInstruction(dag, "COUNT");
    }

    public void testPartialAggregateInstruction_emittedForSumGroupBy() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(2, makeAggregate(sumCall()), convertor);
        assertShardHasPartialAggInstruction(dag, "SUM");
    }

    public void testPartialAggregateInstruction_emittedForFilteredAggregate() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(
            2,
            makeAggregate(
                makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                countStarCall()
            ),
            convertor
        );
        assertShardHasPartialAggInstruction(dag, "COUNT with filter (delegation path)");
    }

    public void testPartialAggregateInstruction_emittedForApproxCountDistinct() {
        RecordingConvertor convertor = new RecordingConvertor();
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        QueryDAG dag = buildAndConvert(2, makeAggregate(approxCountDistinctCall(scan)), convertor);
        assertShardHasPartialAggInstruction(dag, "APPROX_COUNT_DISTINCT (engine-native)");
    }

    private void assertShardHasPartialAggInstruction(QueryDAG dag, String label) {
        Stage shardStage = dag.rootStage().getChildStages().getFirst();
        StagePlan plan = shardStage.getPlanAlternatives().getFirst();
        assertTrue(
            label + ": PARTIAL aggregate shard stage must emit SETUP_PARTIAL_AGGREGATE",
            plan.instructions().stream().anyMatch(node -> node.type() == InstructionType.SETUP_PARTIAL_AGGREGATE)
        );
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
     * Both branches are gathered subtrees. Reduce-stage conversion must serialize the whole
     * Join + branches + ERs + StageInputScans subtree in a single
     * {@code convertFragment (final-agg shape)} pass — same path as Union / Intersect /
     * Minus. No substrait-level join stitching.
     */
    public void testJoinDirectlyOverTwoExchanges() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(2, buildJoinOverTwoScans("test_index", "test_index"), convertor);

        // Find the coord-side join stage — the stage whose fragment contains the Join with
        // two exchange-gathered branches. The whole subtree converts in one pass.
        Stage joinStage = findStageWithTwoChildren(dag.rootStage());
        assertNotNull("expected a stage with 2 child stages (the coord-side Join stage)", joinStage);
        assertNotNull("join stage alternative must have convertedBytes", joinStage.getPlanAlternatives().getFirst().convertedBytes());
        assertTrue("convertFragment (final-agg shape) must be called for the Join subtree", convertor.finalAggCalled);
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
     * ER. Isthmus's SubstraitRelVisitor handles Union natively; reduce-stage conversion serializes
     * the whole Union subtree as one convertFragment (final-agg shape) call — same path as Join.
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
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());

        Stage root = dag.rootStage();
        assertNotNull("root alternative must have convertedBytes", root.getPlanAlternatives().getFirst().convertedBytes());
        assertTrue("convertFragment (final-agg shape) must be called for the Union subtree", convertor.finalAggCalled);
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
        assertTrue("Recorded reduce fragment must be rooted at a LogicalJoin (got:\n" + recorded + ")", recorded.contains("LogicalJoin"));
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
        org.opensearch.analytics.planner.rel.OpenSearchJoin probeJoin = new org.opensearch.analytics.planner.rel.OpenSearchJoin(
            probeScan.getCluster(),
            probeScan.getTraitSet(),
            broadcastScan,                  // build = LEFT input
            probeScan,                      // probe scan = RIGHT input
            condition,
            JoinRelType.INNER,
            probeScan.getViableBackends()
        );

        RecordingConvertor convertor = new RecordingConvertor();
        FragmentConversionDriver.IntraOperatorDelegationBytes delegationBytes = new FragmentConversionDriver.IntraOperatorDelegationBytes(
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

    /** Walks a RexNode subtree and produces a descriptive string for test assertions. */
    private static String describeSubtree(RexNode node) {
        if (node instanceof AnnotatedPredicate ap) {
            node = ap.unwrap();
        }
        if (node instanceof RexCall call) {
            switch (call.getKind()) {
                case AND:
                case OR:
                case NOT: {
                    List<String> children = new ArrayList<>();
                    for (RexNode child : call.getOperands()) {
                        children.add(describeSubtree(child));
                    }
                    return call.getKind() + "(" + String.join(",", children) + ")";
                }
                default:
                    return call.getOperator().getName();
            }
        }
        return node.toString();
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
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                map.put(ScalarFunction.MATCH, serializer);
                map.put(ScalarFunction.WILDCARD, serializer);
                map.put(ScalarFunction.REGEXP, serializer);
                // EQUALS registered for performance-delegation tests (dual-viable predicate that
                // stays on the operator backend but is also serializable by the Lucene peer).
                map.put(ScalarFunction.EQUALS, serializer);
                return map;
            }

            @Override
            public DelegatedSubtreeConvertor getDelegatedSubtreeConvertor() {
                return (subtree, fieldStorage) -> {
                    // Walk the subtree and produce a descriptive string for test assertions
                    return describeSubtree(subtree).getBytes(StandardCharsets.UTF_8);
                };
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
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
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

    /**
     * Three-field delegation helper:
     *   - field 0 = status   (integer, indexed). NOTE: prod Lucene only filters keyword/text/
     *     match_only_text; {@link MockLuceneBackend} declares EQUALS over numerics for test
     *     purposes only. Pre-existing tests use this to exercise the dual-viable performance
     *     path against a mock Lucene; do not interpret as a prod use case.
     *   - field 1 = message  (keyword, indexed) — DUAL-VIABLE for full-text + DataFusion.
     *   - field 2 = amount   (integer, NOT indexed) — SINGLE-VIABLE to DataFusion only.
     */
    private QueryDAG buildTwoFieldDelegationDag(RexNode condition, RecordingConvertor dfConvertor, RecordingSerializer serializer) {
        return buildDelegationDag(
            condition,
            dfConvertor,
            serializer,
            new String[] { "status", "message", "amount" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR, SqlTypeName.INTEGER },
            Map.of(
                "status",
                Map.of("type", "integer", "index", true),
                "message",
                Map.of("type", "keyword", "index", true),
                "amount",
                Map.of("type", "integer", "index", false)
            )
        );
    }

    /**
     * Realistic prod-Lucene-shape DAG for the combiner shape matrix: two keyword fields
     * (both perf-delegatable for EQUALS) plus a non-indexed integer for the native sibling.
     * Mirrors prod Lucene's {@code STANDARD_TYPES = {KEYWORD, TEXT, MATCH_ONLY_TEXT}} —
     * EQUALS-on-int is NOT a Lucene cap in prod even when the field has BKD points.
     */
    private QueryDAG buildKeywordPlusNativeDag(RexNode condition, RecordingConvertor dfConvertor, RecordingSerializer serializer) {
        return buildDelegationDag(
            condition,
            dfConvertor,
            serializer,
            new String[] { "tag", "message", "amount" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.INTEGER },
            Map.of(
                "tag",
                Map.of("type", "keyword", "index", true),
                "message",
                Map.of("type", "keyword", "index", true),
                "amount",
                Map.of("type", "integer", "index", false)
            )
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

    /** Single native equals on a non-indexed field — single-viable to DataFusion, no delegation. */
    public void testSingleNativePredicate() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // amount is index=false → only DataFusion can evaluate, Lucene is not viable.
        QueryDAG dag = buildTwoFieldDelegationDag(makeEquals(2, SqlTypeName.INTEGER, 200), dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(plan, dfConvertor, serializer, 0, false, true, List.of(), FilterTreeShape.NO_DELEGATION);
    }

    /**
     * Single dual-viable equals on an indexed integer field — DataFusion narrows the operator
     * but Lucene was also viable, so FragmentConversion wraps the predicate with the
     * {@code delegation_possible(original, annotationId)} marker for opportunistic per-RG
     * consultation. Asserts one delegated entry, CONJUNCTIVE shape, and that the original
     * predicate survives in the plan alongside the wrapper (unlike correctness delegation,
     * which replaces the original with a {@code delegated_predicate} placeholder).
     */
    public void testSinglePerformanceDelegatedPredicate() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // status is index=true → both DataFusion and Lucene viable → now fully delegated to Lucene.
        QueryDAG dag = buildTwoFieldDelegationDag(makeEquals(0, SqlTypeName.INTEGER, 200), dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        // Wire-shape: one delegated expression via DelegatedSubtreeConvertor.
        assertEquals("delegatedQueries count", 1, plan.delegatedExpressions().size());

        // Tree shape carries CONJUNCTIVE so the driving backend knows about the delegation.
        ShardScanWithDelegationInstructionNode delegationInstruction = (ShardScanWithDelegationInstructionNode) plan.instructions()
            .stream()
            .filter(node -> node.type() == InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION)
            .findFirst()
            .orElseThrow(() -> new AssertionError("delegation plan must have SHARD_SCAN_WITH_DELEGATION"));
        assertEquals("FilterTreeShape", FilterTreeShape.CONJUNCTIVE, delegationInstruction.getTreeShape());
        assertEquals("delegatedPredicateCount", 1, delegationInstruction.getDelegatedPredicateCount());

        // Plan-shape: delegated_predicate placeholder replaces the original.
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        assertTrue(
            "Stripped plan should contain " + DelegationPossibleFunction.NAME,
            strippedPlan.contains(DelegationPossibleFunction.NAME)
        );
        assertDoesntContainOperators(dfConvertor.shardScanFragment, ANNOTATION_MARKERS);
    }

    // ---- AND conditions ----

    /** AND(native, delegated) — equals on non-indexed amount stays native; MATCH_PHRASE replaced. */
    public void testAndNativeAndDelegated() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildTwoFieldDelegationDag(
            makeAnd(makeEquals(2, SqlTypeName.INTEGER, 200), makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "timeout error")),
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
        // With DelegatedSubtreeConvertor, same-backend predicates under AND are combined into 1
        assertDelegationResult(
            plan,
            dfConvertor,
            serializer,
            1,
            true,
            false,
            List.of("MATCH_PHRASE", "FUZZY"),
            FilterTreeShape.CONJUNCTIVE
        );
    }

    /** AND(delegated, delegated) with combining support — combined into single DelegatedExpression. */
    public void testAndTwoDelegatedWithCombining() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // Override the mock Lucene backend to support combining
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
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        var context = buildContext("parquet", fields, backends);
        RexNode condition = makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld"));
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        // Should produce 1 combined DelegatedExpression (not 2 individual ones)
        assertEquals("Should have 1 combined delegated expression", 1, plan.delegatedExpressions().size());
        // The combined bytes should contain both predicates (via DelegatedSubtreeConvertor)
        String combinedStr = new String(
            plan.delegatedExpressions().getFirst().getExpressionBytes(),
            java.nio.charset.StandardCharsets.UTF_8
        );
        assertTrue("Combined should contain MATCH_PHRASE", combinedStr.contains("MATCH_PHRASE"));
        assertTrue("Combined should contain FUZZY", combinedStr.contains("FUZZY"));
    }

    /** OR(delegated, delegated) with combining support — combined into single DelegatedExpression with OR structure. */
    public void testOrTwoDelegatedWithCombining() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // Override the mock Lucene backend to support combining
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
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        var context = buildContext("parquet", fields, backends);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"),
            makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")
        );
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        // OR siblings targeting same backend should be combined into 1 DelegatedExpression
        assertEquals("Should have 1 combined delegated expression", 1, plan.delegatedExpressions().size());
        String combinedStr = new String(
            plan.delegatedExpressions().getFirst().getExpressionBytes(),
            java.nio.charset.StandardCharsets.UTF_8
        );
        assertTrue("Combined should contain MATCH_PHRASE", combinedStr.contains("MATCH_PHRASE"));
        assertTrue("Combined should contain FUZZY", combinedStr.contains("FUZZY"));
    }

    // ---- Complex combining cases ----

    /** OR(AND(delegated, delegated), delegated) — pure Lucene nested structure → 1 combined. */
    public void testOrOfAndDelegated_PureLucene() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
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
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        var context = buildContext("parquet", fields, backends);
        // OR(AND(match_phrase, fuzzy), match_phrase)
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")),
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru")
        );
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        assertEquals("Pure Lucene nested OR(AND,leaf) should produce 1 expression", 1, plan.delegatedExpressions().size());
    }

    /** OR(AND(a,b), AND(c,d)) — pure Lucene two AND groups under OR → 1 combined. */
    public void testOrOfTwoAndGroups_PureLucene() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
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
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        var context = buildContext("parquet", fields, backends);
        // OR(AND(match_phrase, fuzzy), AND(match_phrase, fuzzy))
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")),
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru"), makeFullTextCall(FUZZY_FUNCTION, 0, "typo"))
        );
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        assertEquals("OR(AND,AND) pure Lucene should produce 1 expression", 1, plan.delegatedExpressions().size());
    }

    /** AND(OR(AND(a,b), c), d) — pure Lucene deep nesting → 1 combined. */
    public void testAndOrAndNested_PureLucene() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
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
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        var context = buildContext("parquet", fields, backends);
        // AND(OR(AND(match_phrase, fuzzy), match_phrase), fuzzy)
        RexNode orClause = rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")),
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru")
        );
        RexNode condition = makeAnd(orClause, makeFullTextCall(FUZZY_FUNCTION, 0, "http"));
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        assertEquals("AND(OR(AND,leaf),leaf) pure Lucene should produce 1 expression", 1, plan.delegatedExpressions().size());
    }

    /** AND(OR(AND(a,b), c), d, native) — mixed: Lucene subtree combined + native preserved. */
    public void testMixedAndOrAndWithNative() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
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
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        // message=keyword(indexed), amount=integer(NOT indexed → native only)
        Map<String, Map<String, Object>> fields = Map.of(
            "message",
            Map.of("type", "keyword", "index", true),
            "amount",
            Map.of("type", "integer", "index", false)
        );
        var context = buildContext("parquet", fields, backends);
        // AND(OR(AND(match_phrase, fuzzy), match_phrase), fuzzy, amount=200)
        RexNode orClause = rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")),
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru")
        );
        RexNode condition = makeAnd(orClause, makeFullTextCall(FUZZY_FUNCTION, 0, "http"), makeEquals(1, SqlTypeName.INTEGER, 200));
        LogicalFilter filter = LogicalFilter.create(
            stubScan(
                mockTable(
                    "test_index",
                    new String[] { "message", "amount" },
                    new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
                )
            ),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        // All 4 Lucene predicates combined into 1, native amount=200 stays separate
        assertEquals("Mixed: Lucene combined into 1 expression", 1, plan.delegatedExpressions().size());
        // Verify the stripped plan still has AND (placeholder + native)
        assertTrue("AND should be preserved", RelOptUtil.toString(dfConvertor.shardScanFragment).contains("AND"));
    }

    // ---- Combining: shared helper ----

    /** Result holder for combining tests. */
    private record CombiningResult(StagePlan plan, RecordingConvertor convertor, RecordingSerializer serializer) {
    }

    private static final SqlFunction WILDCARD_FN = new SqlFunction(
        "WILDCARD",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
    private static final SqlFunction REGEXP_FN = new SqlFunction(
        "REGEXP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /**
     * Builds and runs the combining pipeline for a filter condition.
     * Uses 3 fields: message(keyword,indexed), amount(int,NOT indexed), count(int,NOT indexed).
     * Lucene accepts delegation for MATCH_PHRASE, FUZZY, WILDCARD, REGEXP.
     */
    private CombiningResult runCombining(RexNode condition) {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
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
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                map.put(ScalarFunction.WILDCARD, serializer);
                map.put(ScalarFunction.REGEXP, serializer);
                return map;
            }

        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of(
            "message",
            Map.of("type", "keyword", "index", true),
            "amount",
            Map.of("type", "integer", "index", false),
            "count",
            Map.of("type", "integer", "index", false)
        );
        var context = buildContext("parquet", fields, backends);
        LogicalFilter filter = LogicalFilter.create(
            stubScan(
                mockTable(
                    "test_index",
                    new String[] { "message", "amount", "count" },
                    new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER, SqlTypeName.INTEGER }
                )
            ),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        return new CombiningResult(plan, dfConvertor, serializer);
    }

    private RexNode matchPhrase(String query) {
        return makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, query);
    }

    private RexNode fuzzy(String query) {
        return makeFullTextCall(FUZZY_FUNCTION, 0, query);
    }

    private RexNode wildcard(String pattern) {
        return makeFullTextCall(WILDCARD_FN, 0, pattern);
    }

    private RexNode regexp(String pattern) {
        return makeFullTextCall(REGEXP_FN, 0, pattern);
    }

    private RexNode amountEquals(int val) {
        return makeEquals(1, SqlTypeName.INTEGER, val);
    }

    private RexNode countEquals(int val) {
        return makeEquals(2, SqlTypeName.INTEGER, val);
    }

    private RexNode or(RexNode... ops) {
        return rexBuilder.makeCall(SqlStdOperatorTable.OR, ops);
    }

    private RexNode not(RexNode op) {
        return rexBuilder.makeCall(SqlStdOperatorTable.NOT, op);
    }

    private FilterTreeShape treeShapeOf(StagePlan plan) {
        return ((ShardScanWithDelegationInstructionNode) plan.instructions()
            .stream()
            .filter(n -> n.type() == InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION)
            .findFirst()
            .orElseThrow()).getTreeShape();
    }

    // ---- HAVING regression: delegated WHERE under an Aggregate with a HAVING filter above ----

    /**
     * HAVING produces two stacked filters that don't merge across the Aggregate: a native HAVING
     * (count=1) above and the delegated WHERE (match_phrase) below. The derived FilterTreeShape
     * (which the data node reads as its classification) must reflect the WHERE's delegation
     * (CONJUNCTIVE), not the topmost HAVING (NO_DELEGATION). Regression: picking the topmost filter
     * yielded NO_DELEGATION → data node skipped the Lucene collector → full scan / over-count.
     */
    public void testHavingFilterAboveDelegatedWhere_derivesConjunctive() {
        StagePlan plan = runHaving(matchPhrase("hello"));
        assertEquals("delegated WHERE under HAVING must still ship one expression", 1, plan.delegatedExpressions().size());
        assertEquals(
            "treeShape must come from the WHERE (CONJUNCTIVE), not the HAVING (NO_DELEGATION)",
            FilterTreeShape.CONJUNCTIVE,
            treeShapeOf(plan)
        );
    }

    /** Builds Filter(count=1)[HAVING] over Aggregate(group=message, count(*)) over Filter(where)[scan]. */
    private StagePlan runHaving(RexNode whereCondition) {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
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
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                return map;
            }
        };
        List<AnalyticsSearchBackendPlugin> backends = List.of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of(
            "message",
            Map.of("type", "keyword", "index", true),
            "amount",
            Map.of("type", "integer", "index", false),
            "count",
            Map.of("type", "integer", "index", false)
        );
        // Single shard: no exchange split, so HAVING + Aggregate + WHERE stay in ONE fragment — the
        // shape the bug needs (multi-shard forks HAVING into a separate reduce stage, hiding it).
        PlannerContext context = buildContext("parquet", 1, fields, backends);
        RelNode scan = stubScan(
            mockTable(
                "test_index",
                new String[] { "message", "amount", "count" },
                new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER, SqlTypeName.INTEGER }
            )
        );
        LogicalFilter where = LogicalFilter.create(scan, whereCondition);
        LogicalAggregate aggregate = makeAggregate(where, ImmutableBitSet.of(0), countStarCall(where));
        LogicalFilter having = LogicalFilter.create(aggregate, makeEquals(1, SqlTypeName.BIGINT, 1));
        RelNode cboOutput = runPlanner(having, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        return leafStage(dag).getPlanAlternatives().getFirst();
    }

    // ---- Combining tests (OR/NOT/mixed) ----

    /** match_phrase AND fuzzy OR amount=200 → OR(AND(lucene,lucene), native) — combined, INTERLEAVED. */
    public void testCombine_OrOfAndLuceneWithNative() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld")), amountEquals(200)));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(AND(match_phrase, fuzzy), AND(amount=100, count=200)) — one Lucene arm combined, one native arm. */
    public void testCombine_OrOfAndLuceneWithAndNative() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld")), makeAnd(amountEquals(100), countEquals(200))));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** AND(OR(match_phrase, amount=200), fuzzy) — match_phrase can't merge with fuzzy across OR → 2 expressions. */
    public void testCombine_AndWithOrContainingMixed() {
        var r = runCombining(makeAnd(or(matchPhrase("hello"), amountEquals(200)), fuzzy("wrld")));
        assertEquals(2, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(AND(match_phrase, fuzzy), AND(wildcard, amount=200)) — left arm combined, right has wildcard separate. */
    public void testCombine_OrOfPureLuceneAndMixedAnd() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld")), makeAnd(wildcard("h*llo"), amountEquals(200))));
        assertEquals(2, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(AND(match_phrase, fuzzy, wildcard), amount=200) — all 3 Lucene AND-combined, then OR with native. */
    public void testCombine_OrOfThreeLuceneAndNative() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld"), wildcard("h*llo")), amountEquals(200)));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(AND(match_phrase, fuzzy), AND(wildcard, regexp)) — pure Lucene both sides, fully combined to 1. */
    public void testCombine_OrOfTwoAndGroupsPureLucene() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld")), makeAnd(wildcard("h*llo"), regexp("h.llo"))));
        assertEquals(1, r.plan.delegatedExpressions().size());
    }

    /** OR(match_phrase, amount=200, fuzzy) — flat interleaved OR. */
    public void testCombine_OrFlatInterleaved() {
        var r = runCombining(or(matchPhrase("hello"), amountEquals(200), fuzzy("wrld")));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(match_phrase, amount=100, fuzzy, count=200, wildcard) — 3 Lucene combined, 2 native preserved. */
    public void testCombine_OrFlatFivePredicates() {
        var r = runCombining(or(matchPhrase("hello"), amountEquals(100), fuzzy("wrld"), countEquals(200), wildcard("h*")));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** AND(match_phrase, OR(fuzzy, amount=100), wildcard) — fuzzy materialized inside OR, match_phrase+wildcard may combine at AND. */
    public void testCombine_AndLuceneOrMixedLucene() {
        var r = runCombining(makeAnd(matchPhrase("hello"), or(fuzzy("wrld"), amountEquals(100)), wildcard("h*")));
        assertTrue("at least 1 expression", r.plan.delegatedExpressions().size() >= 1);
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** NOT(match_phrase) OR amount=200 — NOT wraps delegated, OR with native, INTERLEAVED. */
    public void testCombine_OrNotLuceneWithNative() {
        var r = runCombining(or(not(matchPhrase("hello")), amountEquals(200)));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** (match_phrase AND fuzzy AND NOT(wildcard)) OR amount=200 — AND combines all 3, then OR with native. */
    public void testCombine_OrOfAndWithNotAndNative() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld"), not(wildcard("h*"))), amountEquals(200)));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(NOT(match_phrase), NOT(fuzzy), amount=200) — both NOTs combine under OR, native stays. */
    public void testCombine_OrTwoNotsWithNative() {
        var r = runCombining(or(not(matchPhrase("hello")), not(fuzzy("wrld")), amountEquals(200)));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** AND(OR(match_phrase, fuzzy), OR(wildcard, amount=200)) — first OR bubbles up, second is mixed. */
    public void testCombine_AndOfTwoOrs_OnePureOneMixed() {
        var r = runCombining(makeAnd(or(matchPhrase("hello"), fuzzy("wrld")), or(wildcard("h*"), amountEquals(200))));
        assertEquals(2, r.plan.delegatedExpressions().size());
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /**
     * Regression for the RexUtil.isFlat AssertionError. After predicate delegation strips the
     * annotation wrappers in FragmentConversionDriver.strip(), an {@code IN (1,2,3)} expands to
     * a bare OR(=,=,=) nested directly inside another OR — e.g.
     * {@code AND(OR(delegated_predicate(0), >), OR(delegated_predicate(2), OR(=,=,=)), =)}.
     * Calcite's shallow RexUtil.flatten does not descend into the inner OR, so
     * LogicalFilter.create's recursive isFlat assertion fires. RelNodeUtils.deepFlatten walks the
     * tree bottom-up and removes the nesting. This builds that exact tree and asserts it.
     */
    public void testDeepFlatten_removesNestedSameOpOr() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode ref = rexBuilder.makeInputRef(intType, 0);
        RexNode eq1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, rexBuilder.makeLiteral(1, intType, true));
        RexNode eq2 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, rexBuilder.makeLiteral(2, intType, true));
        RexNode eq3 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, rexBuilder.makeLiteral(3, intType, true));
        RexNode innerOr = or(eq1, eq2, eq3);
        // Mirror production: the OR(=,=,=) is nested one level DOWN inside an AND, not at the
        // root — so a shallow root-only flatten never visits it. AND(OR(eq2, OR(=,=,=)), eq3).
        RexNode nested = makeAnd(or(eq2, innerOr), eq3);
        assertFalse("precondition: tree is not flat", RexUtil.isFlat(nested));

        RexNode flat = RelNodeUtils.deepFlatten(rexBuilder, nested);
        assertTrue("deepFlatten must produce a flat tree", RexUtil.isFlat(flat));
        // Pure flatten: the inner OR's 3 operands splice into the middle OR (eq2 + 3 = 4), none deduped.
        RexCall andCall = (RexCall) flat;
        assertEquals(SqlKind.AND, andCall.getKind());
        RexNode middleOr = andCall.getOperands().get(0);
        assertEquals(SqlKind.OR, middleOr.getKind());
        assertEquals(4, ((RexCall) middleOr).getOperands().size());
    }

    /** Same-op nesting hidden under a NOT — NOT(OR(eqA, OR(eqB, eqC))) AND eqD. */
    public void testDeepFlatten_removesNestedOrUnderNot() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode ref = rexBuilder.makeInputRef(intType, 0);
        RexNode eqA = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, rexBuilder.makeLiteral(1, intType, true));
        RexNode eqB = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, rexBuilder.makeLiteral(2, intType, true));
        RexNode eqC = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, rexBuilder.makeLiteral(3, intType, true));
        RexNode eqD = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, rexBuilder.makeLiteral(4, intType, true));
        RexNode nested = makeAnd(not(or(eqA, or(eqB, eqC))), eqD);
        assertFalse("precondition: tree is not flat", RexUtil.isFlat(nested));

        RexNode flat = RelNodeUtils.deepFlatten(rexBuilder, nested);
        // The OR nested under NOT must also be flattened (shuttle recurses through NOT).
        assertTrue("deepFlatten must produce a flat tree", RexUtil.isFlat(flat));
    }

    /** An already-flat tree is returned unchanged (and stays flat). */
    public void testDeepFlatten_alreadyFlatUnchanged() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode ref = rexBuilder.makeInputRef(intType, 0);
        RexNode eqA = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, rexBuilder.makeLiteral(1, intType, true));
        RexNode eqB = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, rexBuilder.makeLiteral(2, intType, true));
        // AND(eqA, OR(eqA, eqB)) — mixed operators, no same-op nesting → already flat.
        RexNode flatInput = makeAnd(eqA, or(eqA, eqB));
        assertTrue("precondition: input already flat", RexUtil.isFlat(flatInput));

        RexNode flat = RelNodeUtils.deepFlatten(rexBuilder, flatInput);
        assertTrue(RexUtil.isFlat(flat));
        assertEquals(2, ((RexCall) flat).getOperands().size());
    }

    // ---- OR conditions ----

    /** OR(native, delegated) — equals on non-indexed amount stays native; MATCH_PHRASE replaced. */
    public void testOrNativeAndDelegated() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildTwoFieldDelegationDag(
            rexBuilder.makeCall(
                SqlStdOperatorTable.OR,
                makeEquals(2, SqlTypeName.INTEGER, 200),
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
        RexNode notFuzzy = rexBuilder.makeCall(SqlStdOperatorTable.NOT, makeFullTextCall(FUZZY_FUNCTION, 1, "wrld"));
        RexNode orClause = rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "timeout error"),
            notFuzzy
        );
        // amount is index=false → equals stays native; only MATCH_PHRASE + FUZZY get delegated.
        RexNode condition = makeAnd(makeEquals(2, SqlTypeName.INTEGER, 200), orClause);
        QueryDAG dag = buildTwoFieldDelegationDag(condition, dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(plan, dfConvertor, serializer, 1, true, true, List.of("MATCH_PHRASE", "FUZZY"), FilterTreeShape.CONJUNCTIVE);
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        assertTrue("AND structure should be preserved", strippedPlan.contains("AND"));
    }

    // ---- Dual-viable leaves stay performance-based by default ----

    /**
     * Dual AND Dual: both leaves stay performance-based. The two same-backend perf leaves fuse into
     * one shipped expression; the plan carries delegation_possible (DataFusion prunes natively +
     * opportunistic peer consult) and NO delegated_predicate (nothing shipped wholesale to the peer).
     *
     * <p>Regression guard for the perf-demotion bug: previously every dual-viable leaf was demoted to
     * correctness-based delegation and shipped wholesale to Lucene — even under a plain AND where
     * nothing forces it — so DataFusion never evaluated it. A demoted result here would show a bare
     * delegated_predicate and zero delegation_possible.
     */
    public void testAndTwoDual_defaultsToPerformanceDelegation() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // tag, message both keyword(indexed) → dual-viable for EQUALS; amount int(not indexed) = native.
        RexNode tagEq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeLiteral("alpha")
        );
        RexNode msgEq = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1),
            rexBuilder.makeLiteral("hello")
        );
        QueryDAG dag = buildKeywordPlusNativeDag(makeAnd(tagEq, msgEq), dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        String stripped = RelOptUtil.toString(dfConvertor.shardScanFragment);

        assertTrue(
            "Dual AND Dual must stay performance-based (delegation_possible present). Plan: " + stripped,
            stripped.contains(DelegationPossibleFunction.NAME)
        );
        assertFalse(
            "Dual AND Dual must NOT be demoted to correctness (no delegated_predicate). Plan: " + stripped,
            stripped.contains(DelegatedPredicateFunction.NAME)
        );
    }

    // ---- OR with mixed correctness + performance delegation ----

    /**
     * {@code OR(perf-tag, perf-message, FUZZY-correctness)} — two dual-viable leaves plus a
     * correctness-delegated leaf, all same-backend, under OR. Performance delegation can't survive
     * a disjunction, and a correctness sibling is present, so the combiner fuses the entire OR into
     * ONE {@code delegated_predicate} shipped to the peer: 1 delegated expression, 1
     * delegated_predicate marker, 0 delegation_possible markers.
     */
    public void testOrPerformanceLeavesAndCorrectness_fusesToOneDelegated() {
        var ctx = buildFuseTestSetup();
        QueryDAG dag = buildKeywordFuseDag(ctx.condition, ctx.dfConvertor, ctx.serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        String strippedPlan = RelOptUtil.toString(ctx.dfConvertor.shardScanFragment);
        assertEquals("1 delegated expression (whole OR ships as one peer subtree)", 1, plan.delegatedExpressions().size());
        assertEquals(
            "0 delegation_possible markers — performance delegation doesn't compose under OR",
            0,
            countOccurrences(strippedPlan, "delegation_possible")
        );
        assertEquals("exactly 1 delegated_predicate marker at top", 1, countOccurrences(strippedPlan, "delegated_predicate"));
    }

    /**
     * Regression for the prod "all-docs" disjunction bug — 2-leaf {@code OR(MATCH-correctness,
     * EQUALS-perf)}. Previously the combiner emitted {@code OR(delegated_predicate(matchId),
     * delegation_possible(EQUALS, eqId))} while the tree-shape deriver labelled it CONJUNCTIVE; the
     * data node trusted the label, forced the single-collector path onto a tree containing an OR,
     * failed to apply the filter, and returned every row (ClickBench: 99,997,497 vs. the correct
     * 28,313,573).
     *
     * <p>Now the combiner collapses an OR/NOT with a same-backend correctness sibling into one
     * {@code delegated_predicate} — a bare delegated leaf the data node routes wholesale to Lucene.
     * Pins: exactly 1 delegated_predicate marker, zero delegation_possible markers, 1 expression.
     */
    public void testOrCorrectnessPlusPerf_twoLeaves_collapsesToOneDelegated() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // OR(MATCH(message,'hello') correctness-delegated, EQUALS(tag='alpha') perf-delegated)
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "hello"),
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
                rexBuilder.makeLiteral("alpha")
            )
        );
        QueryDAG dag = buildKeywordFuseDag(condition, dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        assertEquals(
            "2-leaf OR(correctness, perf): 1 delegated expression (whole OR ships as one peer subtree)",
            1,
            plan.delegatedExpressions().size()
        );
        assertEquals(
            "2-leaf OR(correctness, perf): 0 delegation_possible markers — doesn't compose under disjunction",
            0,
            countOccurrences(strippedPlan, "delegation_possible")
        );
        assertEquals(
            "2-leaf OR(correctness, perf): exactly 1 delegated_predicate marker at top",
            1,
            countOccurrences(strippedPlan, "delegated_predicate")
        );
    }

    /**
     * Combiner shape matrix across AND/OR/NOT × native-sibling presence. Each row pins the
     * post-combiner shape via three counters:
     * <ol>
     *   <li>{@code DelegatedExpression} entries the data node receives,</li>
     *   <li>{@code delegated_predicate(id)} markers (peer evaluates these end-to-end),</li>
     *   <li>{@code delegation_possible(original, id)} markers (driver evaluates natively, peer
     *       consulted opportunistically).</li>
     * </ol>
     *
     * <p>Rule: a dual-viable leaf is performance-delegated under AND (one delegation_possible per
     * leaf); under OR/NOT it's reclassified to correctness, fusing with any same-backend correctness
     * siblings into one delegated_predicate. Native-only siblings stay native at the top.
     *
     * <p>Field setup (from {@code buildKeywordPlusNativeDag}, prod-Lucene-shaped):
     * tag/message:keyword(index=true) → dual-viable for EQUALS; message also handles MATCH/FUZZY
     * (Lucene-correctness); amount:int(index=false) → native-only.
     */
    public void testCombinerShapeMatrix_andOrNot_withAndWithoutNativeSibling() {
        // ── shapes ──────────────────────────────────────────────────────────
        RexNode match = makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "hello");
        RexNode notMatch = rexBuilder.makeCall(SqlStdOperatorTable.NOT, match);
        RexNode fuzzy = makeFullTextCall(FUZZY_FUNCTION, 1, "wrld");
        // EQUALS on tag (keyword, indexed) — dual-viable, perf-delegation candidate.
        RexNode eqPerf = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeLiteral("alpha")
        );
        // Distinct field for the second perf EQUALS so Calcite's SEARCH/Sarg rewrite
        // doesn't fold them. message is also keyword(indexed) → dual-viable.
        RexNode eqPerf2 = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1),
            rexBuilder.makeLiteral("hello")
        );
        // amount has index=false → no Lucene format → native-only.
        RexNode nativeArm = makeEquals(2, SqlTypeName.INTEGER, 42);

        // (label, condition, expectation) — expectation = {delegatedExpressions,
        // delegated_predicate markers, delegation_possible markers}. Single expectation per shape:
        // delegation is now decided purely by tree position.
        //
        // Rules: a dual-viable (perf) leaf stays performance-delegated (delegation_possible) under
        // AND. Under OR/NOT it cannot stay perf (perf delegation is AND-only in the data node), so
        // it's reclassified to correctness and ships to the peer, fusing with same-backend siblings.
        Object[][] cases = {
            // ── AND: perf stays perf (delegation_possible per perf leaf) ──
            { "AND(MATCH, EQUALS-perf)", makeAnd(match, eqPerf), new int[] { 2, 1, 1 } },
            { "AND(MATCH, EQUALS-perf, native)", makeAnd(match, eqPerf, nativeArm), new int[] { 2, 1, 1 } },
            { "AND(MATCH, native)", makeAnd(match, nativeArm), new int[] { 1, 1, 0 } },

            // ── OR perf-only: perf reclassified to correctness, fused into one peer shipment ──
            { "OR(EQUALS-perf, EQUALS-perf2)", or(eqPerf, eqPerf2), new int[] { 1, 1, 0 } },
            { "OR(EQUALS-perf, EQUALS-perf2, native)", or(eqPerf, eqPerf2, nativeArm), new int[] { 1, 1, 0 } },

            // ── OR/NOT with a same-backend correctness sibling: perf fuses into it → 1 delegated_predicate ──
            { "OR(MATCH, FUZZY)", or(match, fuzzy), new int[] { 1, 1, 0 } },
            { "OR(MATCH, EQUALS-perf)", or(match, eqPerf), new int[] { 1, 1, 0 } },
            { "OR(MATCH, EQUALS-perf, native)", or(match, eqPerf, nativeArm), new int[] { 1, 1, 0 } },
            { "OR(MATCH, FUZZY, native)", or(match, fuzzy, nativeArm), new int[] { 1, 1, 0 } },
            { "OR(MATCH, native)", or(match, nativeArm), new int[] { 1, 1, 0 } },

            // ── NOT shapes (Calcite folds NOT(=) → ≠ pre-combiner, so no perf survives there) ──
            { "NOT(MATCH)", rexBuilder.makeCall(SqlStdOperatorTable.NOT, match), new int[] { 1, 1, 0 } },
            { "NOT(EQUALS-perf)", rexBuilder.makeCall(SqlStdOperatorTable.NOT, eqPerf), new int[] { 0, 0, 0 } },

            // ── NOT inside boolean — the prod-bug shape from the OR-disjunction fix ───
            { "OR(NOT(MATCH), EQUALS-perf)", or(notMatch, eqPerf), new int[] { 1, 1, 0 } },
            { "OR(NOT(MATCH), EQUALS-perf, native)", or(notMatch, eqPerf, nativeArm), new int[] { 1, 1, 0 } },
            { "AND(NOT(MATCH), EQUALS-perf, native)", makeAnd(notMatch, eqPerf, nativeArm), new int[] { 2, 1, 1 } },
            { "OR(NOT(MATCH), native)", or(notMatch, nativeArm), new int[] { 1, 1, 0 } },
            { "AND(NOT(MATCH), native)", makeAnd(notMatch, nativeArm), new int[] { 1, 1, 0 } }, };

        for (Object[] testCase : cases) {
            String label = (String) testCase[0];
            RexNode condition = (RexNode) testCase[1];
            int[] expected = (int[]) testCase[2];
            assertCombinerShape(label, condition, expected[0], expected[1], expected[2]);
        }
    }

    private void assertCombinerShape(
        String label,
        RexNode condition,
        int expDelegatedExprs,
        int expDelegatedPredicate,
        int expDelegationPossible
    ) {
        RecordingConvertor c = new RecordingConvertor();
        QueryDAG d = buildKeywordPlusNativeDag(condition, c, new RecordingSerializer());
        StagePlan p = leafStage(d).getPlanAlternatives().getFirst();
        String stripped = RelOptUtil.toString(c.shardScanFragment);
        assertEquals(label + " — delegatedExpressions (plan: " + stripped + ")", expDelegatedExprs, p.delegatedExpressions().size());
        assertEquals(
            label + " — delegated_predicate markers (plan: " + stripped + ")",
            expDelegatedPredicate,
            countOccurrences(stripped, "delegated_predicate")
        );
        assertEquals(
            label + " — delegation_possible markers (plan: " + stripped + ")",
            expDelegationPossible,
            countOccurrences(stripped, "delegation_possible")
        );
    }

    /** Shared (recorders, condition) for the three fuse-mode tests. Uses two keyword fields
     *  for the perf-delegation candidates — matches prod Lucene's filterable types
     *  ({@code STANDARD_TYPES = {KEYWORD, TEXT, MATCH_ONLY_TEXT}}). Distinct fields prevent
     *  Calcite's SEARCH/Sarg rewrite from folding the two EQUALS into a sargable predicate.
     */
    private FuseTestSetup buildFuseTestSetup() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            // EQUALS on tag (keyword, indexed) — dual-viable, performance-delegation candidate
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
                rexBuilder.makeLiteral("alpha")
            ),
            // EQUALS on message (keyword, indexed) — dual-viable
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1),
                rexBuilder.makeLiteral("hello")
            ),
            // FUZZY on message — full-text only, correctness-delegated to Lucene
            makeFullTextCall(FUZZY_FUNCTION, 1, "wrld")
        );
        return new FuseTestSetup(dfConvertor, serializer, condition);
    }

    private record FuseTestSetup(RecordingConvertor dfConvertor, RecordingSerializer serializer, RexNode condition) {
    }

    /** Two-keyword-field delegation helper. Both fields are dual-viable; no integer field is
     *  involved, so this stays consistent with prod Lucene caps. */
    private QueryDAG buildKeywordFuseDag(RexNode condition, RecordingConvertor dfConvertor, RecordingSerializer serializer) {
        return buildDelegationDag(
            condition,
            dfConvertor,
            serializer,
            new String[] { "tag", "message" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.VARCHAR },
            Map.of("tag", Map.of("type", "keyword", "index", true), "message", Map.of("type", "keyword", "index", true))
        );
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    // ---- Error paths ----

    /** Delegated annotation with no serializer registered → IllegalStateException. */
    public void testMissingSerializerThrows() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        // Lucene mock accepts delegation but has NO serializers — predicate stays native
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public DelegatedSubtreeConvertor getDelegatedSubtreeConvertor() {
                return null;
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                return Map.of();
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
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry())
        );
        assertTrue(exception.getMessage().contains("No DelegatedPredicateSerializer"));
    }

    /** Performance-delegated predicate without serializer stays native (not combined). */
    public void testPerfDelegatedWithoutSerializerStaysNative() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // Lucene backend has serializer for MATCH_PHRASE but NOT for FUZZY
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                return Map.of(ScalarFunction.MATCH_PHRASE, serializer);
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
        var context = buildContext("parquet", fields, List.of(df, lucene));
        // MATCH_PHRASE is correctness-delegated (lucene-only), EQUALS is perf-delegated (dual-viable)
        // but EQUALS has no serializer on lucene → stays native
        RexNode condition = makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeEquals(0, SqlTypeName.VARCHAR, "world"));
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        // Only MATCH_PHRASE delegated; EQUALS stays native (no serializer)
        assertEquals("Only serializable predicates delegated", 1, plan.delegatedExpressions().size());
    }

    /** Performance-delegated predicate WITH serializer gets combined with correctness-delegated. */
    public void testPerfDelegatedWithSerializerCombines() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // Lucene backend has serializers for both MATCH_PHRASE and EQUALS
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>();
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.EQUALS, serializer);
                return map;
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
        var context = buildContext("parquet", fields, List.of(df, lucene));
        // Both MATCH_PHRASE (correctness) and EQUALS (perf) have serializers → combined into 1
        RexNode condition = makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeEquals(0, SqlTypeName.VARCHAR, "world"));
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        // Correctness and performance delegated stay separate — 2 DelegatedExpressions
        assertEquals("Correctness and performance delegated stay separate", 2, plan.delegatedExpressions().size());
    }

    // ---- Plan shape verification tests (match verified DF logical plans) ----

    /**
     * match(Title,'ru') AND Referer='google' AND URL='US' AND GoodEvent=1
     * Expected: delegated_predicate(0) AND delegation_possible(Referer='google' AND URL='US', 1) AND GoodEvent=1
     */
    public void testAndCorrectnessAndMultiplePerfAndNative() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // 4 fields: Title(keyword,indexed), Referer(keyword,indexed), URL(keyword,indexed), GoodEvent(int,NOT indexed)
        QueryDAG dag = buildDelegationDag(
            makeAnd(
                makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru"),
                makeEquals(1, SqlTypeName.VARCHAR, "google"),
                makeEquals(2, SqlTypeName.VARCHAR, "US"),
                makeEquals(3, SqlTypeName.INTEGER, 1)
            ),
            dfConvertor,
            serializer,
            new String[] { "Title", "Referer", "URL", "GoodEvent" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.INTEGER },
            Map.of(
                "Title",
                Map.of("type", "keyword", "index", true),
                "Referer",
                Map.of("type", "keyword", "index", true),
                "URL",
                Map.of("type", "keyword", "index", true),
                "GoodEvent",
                Map.of("type", "integer", "index", false)
            )
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        // 1 correctness (match) + 1 combined performance (Referer AND URL) = 2 DelegatedExpressions
        assertEquals(2, plan.delegatedExpressions().size());
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        LOGGER.info("Plan (AND correctness + multi-perf + native):\n{}", strippedPlan);
        assertTrue("Should have delegated_predicate", strippedPlan.contains(DelegatedPredicateFunction.NAME));
        assertTrue("Should have delegation_possible", strippedPlan.contains(DelegationPossibleFunction.NAME));
        assertTrue("Should have native GoodEvent =", strippedPlan.contains("="));
        assertEquals(FilterTreeShape.CONJUNCTIVE, treeShapeOf(plan));
    }

    /**
     * Referer='google' AND URL='US' AND GoodEvent=1 (no correctness delegation)
     * Expected: delegation_possible(Referer='google' AND URL='US', 0) AND GoodEvent=1
     */
    public void testAndOnlyPerfAndNative() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildDelegationDag(
            makeAnd(
                makeEquals(0, SqlTypeName.VARCHAR, "google"),
                makeEquals(1, SqlTypeName.VARCHAR, "US"),
                makeEquals(2, SqlTypeName.INTEGER, 1)
            ),
            dfConvertor,
            serializer,
            new String[] { "Referer", "URL", "GoodEvent" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.INTEGER },
            Map.of(
                "Referer",
                Map.of("type", "keyword", "index", true),
                "URL",
                Map.of("type", "keyword", "index", true),
                "GoodEvent",
                Map.of("type", "integer", "index", false)
            )
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        // 1 combined performance (Referer AND URL) = 1 DelegatedExpression
        assertEquals(1, plan.delegatedExpressions().size());
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        LOGGER.info("Plan (AND only-perf + native):\n{}", strippedPlan);
        assertFalse("Should NOT have delegated_predicate", strippedPlan.contains(DelegatedPredicateFunction.NAME));
        assertTrue("Should have delegation_possible", strippedPlan.contains(DelegationPossibleFunction.NAME));
        assertEquals(FilterTreeShape.CONJUNCTIVE, treeShapeOf(plan));
    }

    /**
     * match(Title,'ru') OR (Referer='google' AND URL='US' AND GoodEvent=1)
     * Under OR the Referer/URL perf leaves ship to Lucene (reclassified to correctness), so the plan
     * carries delegated_predicate but no delegation_possible. GoodEvent is native.
     */
    public void testOrCorrectnessWithPerfAndNative() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // OR(match, AND(Referer=, URL=, GoodEvent=))
        QueryDAG dag = buildDelegationDag(
            rexBuilder.makeCall(
                SqlStdOperatorTable.OR,
                makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru"),
                makeAnd(
                    makeEquals(1, SqlTypeName.VARCHAR, "google"),
                    makeEquals(2, SqlTypeName.VARCHAR, "US"),
                    makeEquals(3, SqlTypeName.INTEGER, 1)
                )
            ),
            dfConvertor,
            serializer,
            new String[] { "Title", "Referer", "URL", "GoodEvent" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.INTEGER },
            Map.of(
                "Title",
                Map.of("type", "keyword", "index", true),
                "Referer",
                Map.of("type", "keyword", "index", true),
                "URL",
                Map.of("type", "keyword", "index", true),
                "GoodEvent",
                Map.of("type", "integer", "index", false)
            )
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertEquals(2, plan.delegatedExpressions().size());
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        LOGGER.info("Plan (OR correctness with perf+native):\n{}", strippedPlan);
        assertTrue("Should have delegated_predicate", strippedPlan.contains(DelegatedPredicateFunction.NAME));
        assertFalse("perf ships to Lucene under OR — no delegation_possible", strippedPlan.contains(DelegationPossibleFunction.NAME));
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(plan));
    }

    /**
     * match(Title,'ru') AND Referer='google' OR URL='US' AND GoodEvent=1
     * Operator precedence: (match AND Referer=) OR (URL= AND GoodEvent=)
     * Expected: delegated_predicate(0) AND delegation_possible(Referer, 1) OR delegation_possible(URL, 2) AND GoodEvent=1
     * Tree shape: INTERLEAVED (delegation under OR)
     */
    public void testAndCorrectnessOrPerfWithNative() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // OR( AND(match, Referer=), AND(URL=, GoodEvent=) )
        QueryDAG dag = buildDelegationDag(
            rexBuilder.makeCall(
                SqlStdOperatorTable.OR,
                makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru"), makeEquals(1, SqlTypeName.VARCHAR, "google")),
                makeAnd(makeEquals(2, SqlTypeName.VARCHAR, "US"), makeEquals(3, SqlTypeName.INTEGER, 1))
            ),
            dfConvertor,
            serializer,
            new String[] { "Title", "Referer", "URL", "GoodEvent" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.INTEGER },
            Map.of(
                "Title",
                Map.of("type", "keyword", "index", true),
                "Referer",
                Map.of("type", "keyword", "index", true),
                "URL",
                Map.of("type", "keyword", "index", true),
                "GoodEvent",
                Map.of("type", "integer", "index", false)
            )
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        LOGGER.info("Plan (AND correctness OR perf + native):\n{}", strippedPlan);
        assertTrue("Should have delegated_predicate", strippedPlan.contains(DelegatedPredicateFunction.NAME));
        assertFalse(
            "perf can't survive under OR — must not have delegation_possible",
            strippedPlan.contains(DelegationPossibleFunction.NAME)
        );
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(plan));
    }

    /**
     * NOT match(Title,'ru')
     * Expected: delegated_predicate(0) — correctness delegation, NOT absorbed into BoolQuery(must_not).
     * Tree shape: CONJUNCTIVE (single combined expression, no interleaving).
     */
    public void testNotCorrectnessDelegation() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildSingleFieldDelegationDag(
            rexBuilder.makeCall(SqlStdOperatorTable.NOT, makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru")),
            dfConvertor,
            serializer
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertEquals(1, plan.delegatedExpressions().size());
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        LOGGER.info("Plan (NOT correctness):\n{}", strippedPlan);
        assertTrue("Should have delegated_predicate", strippedPlan.contains(DelegatedPredicateFunction.NAME));
        assertFalse("Should NOT have delegation_possible", strippedPlan.contains(DelegationPossibleFunction.NAME));
        assertEquals(FilterTreeShape.CONJUNCTIVE, treeShapeOf(plan));
    }

    /**
     * NOT tag='hello' — Calcite normalizes to tag!='hello' (NOT_EQUALS).
     * NOT_EQUALS has no serializer, stays fully native, no delegation.
     */
    public void testNotEqualsStaysNative() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildTwoFieldDelegationDag(
            rexBuilder.makeCall(SqlStdOperatorTable.NOT, makeEquals(0, SqlTypeName.INTEGER, 200)),
            dfConvertor,
            serializer
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        LOGGER.info("Plan (NOT equals → NOT_EQUALS, no serializer):\n{}", strippedPlan);
        // NOT_EQUALS has no serializer → no delegation
        assertEquals(0, plan.delegatedExpressions().size());
        assertFalse("Should NOT have delegated_predicate", strippedPlan.contains(DelegatedPredicateFunction.NAME));
        assertFalse("Should NOT have delegation_possible", strippedPlan.contains(DelegationPossibleFunction.NAME));
    }

    /**
     * NOT match(message,'hello') AND tag='hello'
     * NOT(correctness) combined into BoolQuery(must_not) + perf-delegated under AND combined separately.
     * Expected: delegated_predicate(0) AND delegation_possible(tag='hello', 1)
     * Tree shape: CONJUNCTIVE (both under AND, no OR/NOT in final plan).
     */
    public void testNotCorrectnessAndPerfDelegation() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // field 0 = status (int, indexed), field 1 = message (keyword, indexed)
        QueryDAG dag = buildTwoFieldDelegationDag(
            makeAnd(
                rexBuilder.makeCall(SqlStdOperatorTable.NOT, makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "hello")),
                makeEquals(0, SqlTypeName.INTEGER, 200)
            ),
            dfConvertor,
            serializer
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertEquals(2, plan.delegatedExpressions().size());
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        LOGGER.info("Plan (NOT correctness AND perf):\n{}", strippedPlan);
        assertTrue("Should have delegated_predicate", strippedPlan.contains(DelegatedPredicateFunction.NAME));
        assertTrue("Should have delegation_possible", strippedPlan.contains(DelegationPossibleFunction.NAME));
        assertEquals(FilterTreeShape.CONJUNCTIVE, treeShapeOf(plan));
    }

}

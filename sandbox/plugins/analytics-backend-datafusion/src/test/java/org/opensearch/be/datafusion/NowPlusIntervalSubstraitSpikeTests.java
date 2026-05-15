/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.Expression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;

/**
 * Spike: does a Calcite RelNode containing
 * {@code Filter(>=($ts, DATETIME_PLUS(CURRENT_TIMESTAMP, INTERVAL '-7' DAY)))}
 * round-trip cleanly through isthmus → substrait?
 *
 * <p>We're investigating whether we can normalise scalar PPL
 * {@code earliest('-7d', ts)} to a {@code now()-symbolic} form on the SQL plugin
 * side and let DataFusion's optimizer fold it via {@code SimplifyExpressions} —
 * keeping the engine-level "now" coherent across all migrated datetime functions.
 *
 * <p>Step 1 (this spike): just the Java side. Build the RelNode by hand, run it
 * through {@link DataFusionFragmentConvertor#convertShardScanFragment}, decode
 * the resulting bytes, and check whether the substrait emit succeeded and what
 * shape it produced. If isthmus chokes on {@code DATETIME_PLUS} or
 * {@code INTERVAL_DAY} literals, this is where we'd find out.
 */
public class NowPlusIntervalSubstraitSpikeTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(NowPlusIntervalSubstraitSpikeTests.class);

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private SimpleExtension.ExtensionCollection extensions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);

        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(NowPlusIntervalSubstraitSpikeTests.class.getClassLoader());
            SimpleExtension.ExtensionCollection delegationExtensions =
                SimpleExtension.load(List.of("/delegation_functions.yaml"));
            SimpleExtension.ExtensionCollection aggregateExtensions =
                SimpleExtension.load(List.of("/opensearch_aggregate_functions.yaml"));
            SimpleExtension.ExtensionCollection scalarExtensions =
                SimpleExtension.load(List.of("/opensearch_scalar_functions.yaml"));
            extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION
                .merge(delegationExtensions)
                .merge(aggregateExtensions)
                .merge(scalarExtensions);
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    /**
     * The minimum question we want answered: does isthmus emit anything at all
     * for {@code DATETIME_PLUS(CURRENT_TIMESTAMP, INTERVAL '-7' DAY)}? If it
     * throws, we know the symbolic-now path is blocked at substrait emit.
     */
    public void testEmitNowMinusSevenDays() throws Exception {
        RelNode scan = buildTableScan("events", "ts");

        // Use the very SqlOperator instance the convertor registers as substrait
        // "now" — DataFusionFragmentConvertor's FunctionMappings.s() match by
        // operator instance identity, not by name. DateTimeAdapters.LOCAL_NOW_OP
        // is package-private, reach it via reflection.
        java.lang.reflect.Field f = DateTimeAdapters.class.getDeclaredField("LOCAL_NOW_OP");
        f.setAccessible(true);
        org.apache.calcite.sql.SqlOperator nowOp =
            (org.apache.calcite.sql.SqlOperator) f.get(null);
        RexNode now = rexBuilder.makeCall(nowOp);
        SqlIntervalQualifier dayQualifier = new SqlIntervalQualifier("DAY", SqlParserPos.ZERO);
        RexNode minusSevenDays = rexBuilder.makeIntervalLiteral(
            BigDecimal.valueOf(-7L * 24 * 3600 * 1000),
            dayQualifier
        );
        RexNode plus = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, now, minusSevenDays);
        logger.info("[spike] DATETIME_PLUS expression: {} (type={})", plus, plus.getType());

        RexNode tsRef = rexBuilder.makeInputRef(scan, 0);
        RexNode predicate = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            tsRef,
            plus
        );
        logger.info("[spike] predicate: {}", predicate);

        RelNode filter = LogicalFilter.create(scan, predicate);

        byte[] bytes;
        try {
            bytes = new DataFusionFragmentConvertor(extensions).convertShardScanFragment("events", filter);
        } catch (Throwable t) {
            logger.error("[spike] isthmus emit FAILED: {}", t.toString(), t);
            fail("isthmus emit threw: " + t);
            return;
        }
        logger.info("[spike] emit ok, {} bytes", bytes.length);

        Plan plan = Plan.parseFrom(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be FilterRel", root.hasFilter());
        FilterRel filterRel = root.getFilter();

        Expression cond = filterRel.getCondition();
        logger.info("[spike] filter condition proto:\n{}", cond);
        logger.info("[spike] full plan proto:\n{}", plan);
    }

    /** Comparison baseline: same shape, but RHS is a pre-resolved TIMESTAMP literal. */
    public void testEmitTimestampLiteralBaseline() throws Exception {
        RelNode scan = buildTableScan("events", "ts");

        long nowMillis = System.currentTimeMillis() - 7L * 24 * 3600 * 1000;
        RexNode tsLit = rexBuilder.makeTimestampLiteral(
            org.apache.calcite.util.TimestampString.fromMillisSinceEpoch(nowMillis), 3);
        RexNode tsRef = rexBuilder.makeInputRef(scan, 0);
        RexNode predicate = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            tsRef,
            tsLit
        );
        RelNode filter = LogicalFilter.create(scan, predicate);

        byte[] bytes = new DataFusionFragmentConvertor(extensions).convertShardScanFragment("events", filter);
        Plan plan = Plan.parseFrom(bytes);
        Rel root = rootRel(plan);
        assertTrue("baseline root must be FilterRel", root.hasFilter());
        logger.info("[spike-baseline] filter condition proto:\n{}", root.getFilter().getCondition());
    }

    // ── helpers (mirror DataFusionFragmentConvertorTests) ──────────────────────

    private RelDataType timestampRowType(String... columns) {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        for (String c : columns) {
            b.add(c, typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3), true));
        }
        return b.build();
    }

    private RelNode buildTableScan(String tableName, String... columns) {
        return new DataFusionFragmentConvertor.StageInputTableScan(
            cluster, cluster.traitSet(), tableName, timestampRowType(columns));
    }

    private Rel rootRel(Plan plan) {
        assertFalse("plan must contain at least one relation", plan.getRelationsList().isEmpty());
        PlanRel planRel = plan.getRelationsList().get(0);
        assertTrue("plan relation must carry a root", planRel.hasRoot());
        return planRel.getRoot().getInput();
    }
}

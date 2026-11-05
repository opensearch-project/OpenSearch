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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.spi.RuntimeFilterFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.Expression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.proto.SimpleExtensionDeclaration;

/**
 * Substrait round-trip for the two runtime-filter operators.
 *
 * <p>Both are planner-injected and backend-implemented, which means the only thing standing between a
 * planted predicate and a query that cannot run is the extension declaration these tests exercise: a
 * Calcite operator maps to a name, and the name has to resolve in the loaded extension collection before
 * a plan can reference it. A missing declaration fails at conversion time, on the query, so it is worth
 * asserting directly rather than discovering on a cluster.
 *
 * <p>The extension collection is assembled exactly as {@code DataFusionPlugin#loadSubstraitExtensions}
 * does, so a YAML that loads here loads in production too.
 */
public class RuntimeFilterSubstraitConversionTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private SimpleExtension.ExtensionCollection extensions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);

        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(RuntimeFilterSubstraitConversionTests.class.getClassLoader());
            extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(SimpleExtension.load(List.of("/runtime_filter_functions.yaml")));
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    public void testProbePredicateConvertsToAnOsRuntimeFilterCall() throws Exception {
        RelNode scan = scan("lineitem", "l_orderkey", "l_quantity");
        RexNode probe = RuntimeFilterFunction.makeProbeCall(rexBuilder, /* filterId */ 7, rexBuilder.makeInputRef(scan, 0));
        byte[] bytes = new DataFusionFragmentConvertor(extensions).convertFragment(LogicalFilter.create(scan, probe));

        Plan plan = Plan.parseFrom(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be a FilterRel", root.hasFilter());
        FilterRel filter = root.getFilter();
        Expression.ScalarFunction call = filter.getCondition().getScalarFunction();
        assertEquals(RuntimeFilterFunction.PROBE_NAME, resolveFunctionName(plan, call.getFunctionReference()));
        assertEquals(
            "the declared signature key proves the (i32, any) impl in the YAML is what matched",
            RuntimeFilterFunction.PROBE_NAME + ":i32_any",
            declaredName(plan, call.getFunctionReference())
        );

        // (filterId, key) in that order. Reversed, the backend would read a data column as an id.
        assertEquals(2, call.getArgumentsCount());
        assertEquals(
            "arg 0 must be the id literal, resolved once per batch rather than per row",
            7,
            call.getArguments(0).getValue().getLiteral().getI32()
        );
        assertTrue("arg 1 must be the key column", call.getArguments(1).getValue().hasSelection());
    }

    public void testBloomAggregateConvertsToAnOsBloomAggMeasure() throws Exception {
        // Shape of the pre-pass: a global aggregate over (key, numBytes), where numBytes is a constant
        // column because an AggregateCall's arguments are field ordinals, not expressions.
        RelNode scan = scan("orders", "o_orderkey", "__rf_bytes");
        AggregateCall bloomAgg = AggregateCall.create(
            RuntimeFilterFunction.BLOOM_AGG,
            /* distinct */ false,
            /* approximate */ false,
            /* ignoreNulls */ false,
            List.of(),
            List.of(0, 1),
            /* filterArg */ -1,
            /* distinctKeys */ null,
            RelCollations.EMPTY,
            /* hasEmptyGroup */ true,
            scan,
            typeFactory.createSqlType(SqlTypeName.VARBINARY),
            "__rf_bloom"
        );
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(bloomAgg));

        byte[] bytes = new DataFusionFragmentConvertor(extensions).convertFragment(aggregate);
        Plan plan = Plan.parseFrom(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be an AggregateRel", root.hasAggregate());
        AggregateRel agg = root.getAggregate();
        assertEquals("one measure", 1, agg.getMeasuresCount());
        assertEquals(
            RuntimeFilterFunction.BLOOM_AGG_NAME,
            resolveFunctionName(plan, agg.getMeasures(0).getMeasure().getFunctionReference())
        );
        assertEquals(
            "the declared signature key proves the (any, i32) impl in the YAML is what matched",
            RuntimeFilterFunction.BLOOM_AGG_NAME + ":any_i32",
            declaredName(plan, agg.getMeasures(0).getMeasure().getFunctionReference())
        );
        assertEquals(
            "the size argument must survive conversion — without it the shards cannot agree on a size",
            2,
            agg.getMeasures(0).getMeasure().getArgumentsCount()
        );
        assertTrue(
            "a global aggregate has no grouping",
            agg.getGroupingsList().isEmpty() || agg.getGroupings(0).getGroupingExpressionsCount() == 0
        );
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private RelNode scan(String table, String... columns) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (String column : columns) {
            builder.add(column, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true));
        }
        RelDataType rowType = builder.build();
        return new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), table, rowType);
    }

    private static Rel rootRel(Plan plan) {
        assertFalse("plan must contain a relation", plan.getRelationsList().isEmpty());
        PlanRel planRel = plan.getRelationsList().get(0);
        assertTrue("plan relation must carry a root", planRel.hasRoot());
        return planRel.getRoot().getInput();
    }

    /**
     * The base name behind a function anchor, which is what the backend binds its UDF by.
     *
     * <p>A declaration's name is {@code <name>:<signature key>} — the anchor identifies one overload, not
     * one function — so the suffix is stripped here, as the delegation conversion tests do.
     */
    private static String resolveFunctionName(Plan plan, int functionReference) {
        String declared = declaredName(plan, functionReference);
        int colon = declared.indexOf(':');
        return colon >= 0 ? declared.substring(0, colon) : declared;
    }

    /** The declaration verbatim, including the signature key that records which impl matched. */
    private static String declaredName(Plan plan, int functionReference) {
        for (SimpleExtensionDeclaration declaration : plan.getExtensionsList()) {
            if (declaration.hasExtensionFunction() && declaration.getExtensionFunction().getFunctionAnchor() == functionReference) {
                return declaration.getExtensionFunction().getName();
            }
        }
        throw new AssertionError("no extension declaration for function anchor " + functionReference + " in\n" + plan);
    }
}

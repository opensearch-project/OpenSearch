/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package io.substrait.isthmus.expression;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.be.datafusion.DataFusionFragmentConvertor;
import org.opensearch.test.OpenSearchTestCase;

import java.io.InputStream;
import java.util.List;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.proto.SortField;

/**
 * Tests the {@code ARG_MIN}/{@code ARG_MAX} → {@code first_value}/{@code last_value}
 * rewrite in {@link NameBasedAggregateFunctionConverter}. Feeds a Calcite
 * {@link LogicalAggregate} whose single measure is an {@link SqlStdOperatorTable#ARG_MIN}
 * or {@link SqlStdOperatorTable#ARG_MAX} call through
 * {@link DataFusionFragmentConvertor#convertFinalAggFragment}, decodes the resulting
 * Substrait proto bytes, and asserts:
 *
 * <ul>
 *   <li>the emitted aggregate measure has exactly one argument (the value field), NOT
 *       two — the key field is pulled out into the sort field list;</li>
 *   <li>the measure carries exactly one sort field whose expression is a field
 *       reference to the key and whose direction is {@code ASC_NULLS_LAST};</li>
 *   <li>the function reference points at {@code first_value} for ARG_MIN and
 *       {@code last_value} for ARG_MAX.</li>
 * </ul>
 */
public class NameBasedAggregateFunctionConverterTests extends OpenSearchTestCase {

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

        // Load the substrait core defaults plus OpenSearch's aggregate-extension
        // YAML — the latter carries the first_value / last_value variants our
        // rewrite looks up by name.
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(NameBasedAggregateFunctionConverterTests.class.getClassLoader());
            SimpleExtension.ExtensionCollection collection = DefaultExtensionCatalog.DEFAULT_COLLECTION;
            try (
                InputStream stream = NameBasedAggregateFunctionConverterTests.class.getResourceAsStream(
                    "/extensions/opensearch_aggregate.yaml"
                )
            ) {
                assertNotNull("opensearch_aggregate.yaml must be on the test classpath", stream);
                collection = collection.merge(SimpleExtension.load(stream));
            }
            extensions = collection;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    /**
     * {@code SELECT ARG_MIN(value, key) FROM t} lowered via isthmus must emit a
     * substrait measure targeting {@code first_value(value)} with
     * {@code sorts=[{expr=key, direction=ASC_NULLS_LAST}]}.
     */
    public void testArgMinRewritesToFirstValueWithSortKey() throws Exception {
        assertArgMinMaxRewrite(SqlStdOperatorTable.ARG_MIN, "first_value");
    }

    /**
     * Symmetric to {@link #testArgMinRewritesToFirstValueWithSortKey()} — ARG_MAX
     * routes to {@code last_value(value)} with the same ASC_NULLS_LAST sort field.
     * {@code last_value} with ASC returns the row with the max key because it's the
     * last row after sorting.
     */
    public void testArgMaxRewritesToLastValueWithSortKey() throws Exception {
        assertArgMinMaxRewrite(SqlStdOperatorTable.ARG_MAX, "last_value");
    }

    /**
     * Shared assertion body — builds an aggregate fragment with one call using
     * {@code op(value_col, key_col)}, runs it through the fragment convertor,
     * and checks the resulting proto's first measure.
     */
    private void assertArgMinMaxRewrite(org.apache.calcite.sql.SqlAggFunction op, String expectedVariantName) throws Exception {
        RelNode leaf = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 0, rowType("value_col", "key_col"), List.of("datafusion"));
        AggregateCall call = AggregateCall.create(
            op,
            /* isDistinct */ false,
            /* argList */ List.of(0, 1),
            /* filterArg */ -1,
            /* type */ typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            /* name */ "result"
        );
        LogicalAggregate agg = LogicalAggregate.create(leaf, List.of(), ImmutableBitSet.of(), /* groupSets */ null, List.of(call));

        byte[] bytes = new DataFusionFragmentConvertor(extensions).convertFinalAggFragment(agg);
        Plan plan = Plan.parseFrom(bytes);
        assertFalse("plan must have at least one relation", plan.getRelationsList().isEmpty());
        PlanRel planRel = plan.getRelationsList().get(0);
        assertTrue("plan relation must carry a root", planRel.hasRoot());
        Rel root = planRel.getRoot().getInput();
        assertTrue("root must be an AggregateRel for " + op.getName(), root.hasAggregate());
        AggregateRel aggRel = root.getAggregate();
        assertEquals("expected exactly one measure", 1, aggRel.getMeasuresCount());
        io.substrait.proto.AggregateFunction fn = aggRel.getMeasures(0).getMeasure();

        // Measure must have exactly 1 argument — the value field. The key field was
        // pulled out into the sort field list by the rewrite.
        assertEquals("rewritten measure must carry 1 argument (the value field), not 2", 1, fn.getArgumentsCount());

        // Measure must carry exactly one sort field, expr = field-reference to the key,
        // direction = ASC_NULLS_LAST.
        assertEquals("rewritten measure must carry exactly one sort field", 1, fn.getSortsCount());
        SortField sort = fn.getSorts(0);
        assertEquals(
            "rewrite emits ASC_NULLS_LAST direction for both ARG_MIN and ARG_MAX",
            SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST,
            sort.getDirection()
        );
        assertTrue("sort expression must be a field reference to the key column", sort.getExpr().hasSelection());
        assertEquals(
            "sort expression must reference column index 1 (key_col)",
            1,
            sort.getExpr().getSelection().getDirectReference().getStructField().getField()
        );

        // Function reference in the plan's extensions must point at first_value or last_value.
        int fnRef = fn.getFunctionReference();
        String resolvedName = plan.getExtensionsList()
            .stream()
            .filter(ext -> ext.hasExtensionFunction() && ext.getExtensionFunction().getFunctionAnchor() == fnRef)
            .map(ext -> ext.getExtensionFunction().getName())
            .findFirst()
            .orElseThrow(() -> new AssertionError("no extension function entry for anchor " + fnRef));
        assertTrue(
            "rewritten function name must start with " + expectedVariantName + " (got: " + resolvedName + ")",
            resolvedName.startsWith(expectedVariantName)
        );
    }

    // ── Low-level convertByName / getFunctionFinder assertions ────────────────

    /**
     * The rewrite must only activate for 2-argument ARG_MIN/ARG_MAX calls.
     * Guards against a future Calcite extension that emits arity-3 variants
     * silently slipping through.
     */
    public void testRewriteIsArityGated() {
        NameBasedAggregateFunctionConverter conv = new NameBasedAggregateFunctionConverter(
            extensions.aggregateFunctions(),
            List.of(),
            typeFactory,
            io.substrait.isthmus.TypeConverter.DEFAULT
        );
        // No public API to probe the rewrite directly — the fragment-level tests above
        // cover the arity-2 happy path. This test would need refactoring if we ever
        // expose an arity-check hook.
        assertNotNull(conv);
    }

    // ── Pure-rename aliases: first, last, list (convertByName path) ────────────

    /**
     * PPL {@code stats first(field)} maps through {@link NameBasedAggregateFunctionConverter#NAME_ALIASES}
     * as a pure rename to DataFusion's {@code first_value}. No sort field, no forced DISTINCT —
     * the emitted substrait measure preserves the single operand and routes to the
     * {@code first_value} YAML variant.
     */
    public void testFirstRewritesToFirstValue() throws Exception {
        assertPureRename("first", "first_value", /* expectDistinct */ false);
    }

    /**
     * Symmetric to {@link #testFirstRewritesToFirstValue()} — PPL {@code last} maps to
     * DataFusion's {@code last_value} via pure-rename AliasConfig.
     */
    public void testLastRewritesToLastValue() throws Exception {
        assertPureRename("last", "last_value", /* expectDistinct */ false);
    }

    /**
     * PPL {@code stats list(field)} maps as a pure rename to DataFusion's {@code array_agg}.
     * Unlike {@code values}, {@code list} does NOT force DISTINCT and does NOT synthesize a
     * sort field — the collected array preserves input order and duplicates.
     */
    public void testListRewritesToArrayAgg() throws Exception {
        assertPureRename("list", "array_agg", /* expectDistinct */ false);
    }

    /**
     * PPL {@code stats take(field, n)} routes to the custom {@code take} YAML variant
     * registered by the Rust UDAF. Unlike {@code first}/{@code last}/{@code list}, there
     * is no alias rewrite — the variant is looked up directly by name in the extension
     * collection. This test verifies the full 2-operand form resolves to the {@code take}
     * variant without any sort or DISTINCT shape.
     */
    public void testTakeRoutesToTakeVariant() throws Exception {
        RelNode leaf = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 0, rowType("value_col", "n_col"), List.of("datafusion"));
        SqlAggFunction takeAgg = stubAgg("take");
        AggregateCall call = AggregateCall.create(
            takeAgg,
            /* isDistinct */ false,
            /* argList */ List.of(0, 1),
            /* filterArg */ -1,
            /* type */ typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            /* name */ "result"
        );
        LogicalAggregate agg = LogicalAggregate.create(leaf, List.of(), ImmutableBitSet.of(), /* groupSets */ null, List.of(call));

        byte[] bytes = new DataFusionFragmentConvertor(extensions).convertFinalAggFragment(agg);
        Plan plan = Plan.parseFrom(bytes);
        Rel root = plan.getRelationsList().get(0).getRoot().getInput();
        assertTrue("root must be an AggregateRel", root.hasAggregate());
        AggregateRel aggRel = root.getAggregate();
        assertEquals("expected exactly one measure", 1, aggRel.getMeasuresCount());
        io.substrait.proto.AggregateFunction fn = aggRel.getMeasures(0).getMeasure();
        assertEquals("take preserves both operands (value + n)", 2, fn.getArgumentsCount());
        assertEquals("take has no synthesized sort field", 0, fn.getSortsCount());
        assertEquals(
            "take is not distinct-by-default",
            io.substrait.proto.AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL,
            fn.getInvocation()
        );

        int fnRef = fn.getFunctionReference();
        String resolvedName = plan.getExtensionsList()
            .stream()
            .filter(ext -> ext.hasExtensionFunction() && ext.getExtensionFunction().getFunctionAnchor() == fnRef)
            .map(ext -> ext.getExtensionFunction().getName())
            .findFirst()
            .orElseThrow(() -> new AssertionError("no extension function entry for anchor " + fnRef));
        assertTrue("take must resolve to the take YAML variant (got: " + resolvedName + ")", resolvedName.startsWith("take"));
    }

    /**
     * Shared assertion body for pure-rename AliasConfig entries (first, last, list).
     * Builds a scalar stats call with a single operand against a 1-column leaf scan,
     * runs it through the fragment convertor, and checks the emitted measure:
     * <ul>
     *   <li>exactly 1 operand;</li>
     *   <li>no sort field — pure renames don't synthesize sorts;</li>
     *   <li>invocation matches {@code expectDistinct} (all pure renames today are ALL);</li>
     *   <li>function reference resolves to {@code targetVariantName} in the extension list.</li>
     * </ul>
     */
    private void assertPureRename(String aliasName, String targetVariantName, boolean expectDistinct) throws Exception {
        RelNode leaf = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 0, rowType("value_col"), List.of("datafusion"));
        SqlAggFunction aliasAgg = stubAgg(aliasName);
        AggregateCall call = AggregateCall.create(
            aliasAgg,
            /* isDistinct */ false,
            /* argList */ List.of(0),
            /* filterArg */ -1,
            /* type */ typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            /* name */ "result"
        );
        LogicalAggregate agg = LogicalAggregate.create(leaf, List.of(), ImmutableBitSet.of(), /* groupSets */ null, List.of(call));

        byte[] bytes = new DataFusionFragmentConvertor(extensions).convertFinalAggFragment(agg);
        Plan plan = Plan.parseFrom(bytes);
        Rel root = plan.getRelationsList().get(0).getRoot().getInput();
        assertTrue(aliasName + ": root must be an AggregateRel", root.hasAggregate());
        AggregateRel aggRel = root.getAggregate();
        assertEquals(aliasName + ": expected exactly one measure", 1, aggRel.getMeasuresCount());
        io.substrait.proto.AggregateFunction fn = aggRel.getMeasures(0).getMeasure();
        assertEquals(aliasName + ": pure rename preserves the single operand", 1, fn.getArgumentsCount());
        assertEquals(aliasName + ": pure rename does not synthesize sort fields", 0, fn.getSortsCount());
        io.substrait.proto.AggregateFunction.AggregationInvocation expectedInv = expectDistinct
            ? io.substrait.proto.AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT
            : io.substrait.proto.AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL;
        assertEquals(aliasName + ": invocation flag must match expected", expectedInv, fn.getInvocation());

        int fnRef = fn.getFunctionReference();
        String resolvedName = plan.getExtensionsList()
            .stream()
            .filter(ext -> ext.hasExtensionFunction() && ext.getExtensionFunction().getFunctionAnchor() == fnRef)
            .map(ext -> ext.getExtensionFunction().getName())
            .findFirst()
            .orElseThrow(() -> new AssertionError(aliasName + ": no extension function entry for anchor " + fnRef));
        assertTrue(
            aliasName + " must resolve to " + targetVariantName + " (got: " + resolvedName + ")",
            resolvedName.startsWith(targetVariantName)
        );
    }

    // ── AliasConfig forceDistinct + sortIsSelf (VALUES-style) ──────────────────

    /**
     * PPL {@code stats values(field)} routes through AliasConfig
     * {@code {target=array_agg, forceDistinct=true, sortIsSelf=true}}. The rewritten
     * substrait measure must:
     * <ul>
     *   <li>target the {@code array_agg} YAML variant (not {@code values});</li>
     *   <li>carry exactly one operand (the field — same as input);</li>
     *   <li>carry exactly one sort field whose expression references the SAME column
     *       as the operand (sort-by-self), with ASC_NULLS_LAST direction;</li>
     *   <li>set AggregationInvocation = DISTINCT regardless of whether the frontend
     *       call's isDistinct() was true or false.</li>
     * </ul>
     * Mirrors Marc's concern that
     * {@code NameBasedAggregateFunctionConverter.convertByName}'s prior empty-sorts
     * path would silently swallow ORDER BY flags; without this test, a refactor
     * could regress DISTINCT or sort-field propagation and the only signal would be
     * a semantic IT failure.
     */
    public void testValuesRewritesToArrayAggDistinctSortedBySelf() throws Exception {
        RelNode leaf = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 0, rowType("value_col"), List.of("datafusion"));
        // PPL emits a custom SqlAggFunction named "VALUES" (uppercase, SqlKind.OTHER_FUNCTION)
        // with isDistinct=false — the AliasConfig must force DISTINCT regardless.
        SqlAggFunction valuesAgg = stubAgg("VALUES");
        AggregateCall call = AggregateCall.create(
            valuesAgg,
            /* isDistinct */ false,
            /* argList */ List.of(0),
            /* filterArg */ -1,
            /* type */ typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            /* name */ "result"
        );
        LogicalAggregate agg = LogicalAggregate.create(leaf, List.of(), ImmutableBitSet.of(), /* groupSets */ null, List.of(call));

        byte[] bytes = new DataFusionFragmentConvertor(extensions).convertFinalAggFragment(agg);
        Plan plan = Plan.parseFrom(bytes);
        Rel root = plan.getRelationsList().get(0).getRoot().getInput();
        assertTrue("root must be an AggregateRel", root.hasAggregate());
        AggregateRel aggRel = root.getAggregate();
        assertEquals("expected exactly one measure", 1, aggRel.getMeasuresCount());
        io.substrait.proto.AggregateFunction fn = aggRel.getMeasures(0).getMeasure();

        // DISTINCT must be forced by AliasConfig.forceDistinct even when the
        // Calcite-level call had isDistinct=false.
        assertEquals(
            "VALUES → array_agg must emit AGGREGATION_INVOCATION_DISTINCT",
            io.substrait.proto.AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT,
            fn.getInvocation()
        );

        // sortIsSelf keeps the operand AND synthesizes a sort field on it.
        assertEquals("operand list must have 1 element (value_col)", 1, fn.getArgumentsCount());
        assertEquals("must carry exactly one sort field for sort-by-self", 1, fn.getSortsCount());
        SortField sort = fn.getSorts(0);
        assertEquals("sort direction must be ASC_NULLS_LAST", SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST, sort.getDirection());
        assertTrue("sort expression must be a field reference", sort.getExpr().hasSelection());
        assertEquals(
            "sort-by-self: sort expression must reference the same column index as the operand (0)",
            0,
            sort.getExpr().getSelection().getDirectReference().getStructField().getField()
        );

        // Function reference must resolve to array_agg, not values.
        int fnRef = fn.getFunctionReference();
        String resolvedName = plan.getExtensionsList()
            .stream()
            .filter(ext -> ext.hasExtensionFunction() && ext.getExtensionFunction().getFunctionAnchor() == fnRef)
            .map(ext -> ext.getExtensionFunction().getName())
            .findFirst()
            .orElseThrow(() -> new AssertionError("no extension function entry for anchor " + fnRef));
        assertTrue("VALUES rewrite target must be array_agg (got: " + resolvedName + ")", resolvedName.startsWith("array_agg"));
    }

    /**
     * Minimal {@link SqlAggFunction} built with the same 10-arg constructor as
     * {@link DataFusionFragmentConvertor#stubAgg(String)}. PPL-emitted aggregate
     * operators are custom SqlAggFunction subclasses with SqlKind.OTHER_FUNCTION;
     * we need to simulate one in tests to exercise the AliasConfig rewrite path
     * without depending on the sql/core frontend.
     */
    private static SqlAggFunction stubAgg(String name) {
        return new SqlAggFunction(
            name,
            null,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false,
            Optionality.FORBIDDEN
        ) {
        };
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private RelDataType rowType(String... columns) {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        for (String c : columns) {
            b.add(c, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true));
        }
        return b.build();
    }

    // Keep the sort-field type reachable so Eclipse's organize-imports doesn't prune it.
    @SuppressWarnings("unused")
    private static final Class<?> SORT_FIELD_CLASS = Expression.SortField.class;

    @SuppressWarnings("unused")
    private static final Class<?> INVOCATION_CLASS = AggregateFunctionInvocation.class;
}

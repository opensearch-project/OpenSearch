/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumSet;
import java.util.List;

/**
 * Pins the two AGGREGATE halves of how {@code DslTypeSystems.NANO_TIMESTAMP} differs from
 * {@link RelDataTypeSystem#DEFAULT}: the width a {@code SUM} is declared with, and the type an {@code AVG}
 * is declared with. They are pinned in one class because they are one mechanism — the engine reduces an
 * {@code AVG} into a {@code SUM}/{@code COUNT}/{@code DIVIDE} plus a CAST back to the {@code AVG}'s own
 * declared type, so an {@code AVG} left at an integral width casts the widened {@code SUM} straight back
 * down and makes the division integral on top of it. Fixing either alone leaves the other's symptom.
 */
public class DslTypeSystemsTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new SqlTypeFactoryImpl(DslTypeSystems.NANO_TIMESTAMP);
    }

    /**
     * The failure this whole class exists for: on a {@code price: integer} field the engine's
     * {@code OpenSearchAggregateReduceRule} turns the DSL's {@code AVG} into an unnamed {@code SUM} plus
     * an unnamed {@code COUNT}. With Calcite's default type system that {@code SUM} is declared
     * {@code INTEGER} while the DataFusion backend accumulates it in {@code Int64}, and on a 2-shard
     * index the cross-fragment schema check rejects the plan
     * ({@code Field '$f2' ... (Int32) ... table schema (Int64)}). Asserting {@code BIGINT} here is
     * asserting that plan is accepted.
     */
    public void testReducedAvgSumIsBigintOnAnIntegerField() throws ConversionException {
        RelNode aggPlan = nestedAvgAggregationPlan();

        Aggregate reduced = reduceAggregateFunctions(aggPlan);
        AggregateCall sum = onlyCallOfKind(reduced, SqlKind.SUM);

        assertEquals(
            "the reduced AVG's SUM must be declared as wide as the engine accumulates it, or the "
                + "2-shard exchange schema check rejects the plan",
            SqlTypeName.BIGINT,
            sum.getType().getSqlTypeName()
        );
        // The count half is already i64 on both sides; asserted so a change that narrows either half of
        // the decomposition is caught here rather than on a cluster.
        for (AggregateCall count : callsOfKind(reduced, SqlKind.COUNT)) {
            assertEquals("every COUNT of the decomposition must stay i64", SqlTypeName.BIGINT, count.getType().getSqlTypeName());
        }
    }

    /**
     * The written-{@code sum} counterpart of the test above, and the regression this pair was missing:
     * the {@code SUM} a DSL {@code sum} aggregation is translated into directly (no reduction rule
     * involved) must be DECLARED at the same width the type system INFERS.
     */
    public void testWrittenSumOverAnIntegerFieldIsDeclaredBigint() throws ConversionException {
        RelNode plan = deepestAggregationPlan(
            new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new SumAggregationBuilder("sum_price").field("price"))
        );

        AggregateCall sum = onlyCallOfKind(findAggregate(plan), SqlKind.SUM);
        assertEquals(
            "a written sum over an integer field must be declared as wide as the type system derives it",
            SqlTypeName.BIGINT,
            sum.getType().getSqlTypeName()
        );
        assertTrue("nullability must be carried from the summed column", sum.getType().isNullable());
    }

    /**
     * Same invariant on the other aggregate shape: a root-level {@code sum} produces a no-GROUP-BY
     * aggregate, where {@code ReturnTypes.AGG_SUM} forces the inferred type nullable
     * ({@code groupCount == 0}) and {@code AggregationMetadataBuilder} independently wraps the declared
     * type nullable. Both have to land on the same type or the plan is rejected, so the widening and
     * that wrapping are pinned together here rather than assumed to compose.
     */
    public void testRootLevelSumOverAnIntegerFieldIsDeclaredNullableBigint() throws ConversionException {
        RelNode plan = deepestAggregationPlan(new SumAggregationBuilder("sum_price").field("price"));

        AggregateCall sum = onlyCallOfKind(findAggregate(plan), SqlKind.SUM);
        assertEquals(SqlTypeName.BIGINT, sum.getType().getSqlTypeName());
        assertTrue("a no-GROUP-BY sum is nullable on both the declared and the inferred side", sum.getType().isNullable());
    }

    /**
     * The contrast half of the test above: with Calcite's default type system the same field yields
     * {@code INTEGER}. Without this, a reader cannot tell whether the assertion above is pinning
     * anything.
     */
    public void testDefaultTypeSystemWouldDeclareTheSumAsInteger() {
        RelDataTypeFactory defaultFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType integer = nullable(defaultFactory, SqlTypeName.INTEGER);

        assertEquals(SqlTypeName.INTEGER, defaultFactory.getTypeSystem().deriveSumType(defaultFactory, integer).getSqlTypeName());
    }

    public void testSignedIntegerFamilyWidensToBigint() {
        for (SqlTypeName narrower : List.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
            RelDataType sumType = deriveSum(nullable(typeFactory, narrower));
            assertEquals(narrower + " must sum as BIGINT", SqlTypeName.BIGINT, sumType.getSqlTypeName());
        }
    }

    public void testApproximateNumericFamilyWidensToDouble() {
        for (SqlTypeName approximate : List.of(SqlTypeName.REAL, SqlTypeName.FLOAT, SqlTypeName.DOUBLE)) {
            RelDataType sumType = deriveSum(nullable(typeFactory, approximate));
            assertEquals(approximate + " must sum as DOUBLE", SqlTypeName.DOUBLE, sumType.getSqlTypeName());
        }
    }

    /**
     * Nullability is part of the sum type, not an afterthought: Calcite's empty-group handling reads it,
     * so a widening that returned a NOT NULL type would change results, not just the declared width.
     */
    public void testNullabilityIsCarriedFromTheArgument() {
        assertTrue(deriveSum(nullable(typeFactory, SqlTypeName.INTEGER)).isNullable());
        assertFalse(deriveSum(typeFactory.createSqlType(SqlTypeName.INTEGER)).isNullable());
    }

    /** A family with no known accumulator widening falls through to Calcite, never to a guess. */
    public void testUnknownFamilyFallsBackToCalciteDefault() {
        RelDataTypeFactory defaultFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        for (SqlTypeName untouched : List.of(SqlTypeName.DECIMAL, SqlTypeName.VARCHAR, SqlTypeName.BOOLEAN)) {
            RelDataType argument = nullable(typeFactory, untouched);
            assertEquals(
                untouched + " must keep Calcite's derivation",
                defaultFactory.getTypeSystem().deriveSumType(defaultFactory, argument).getSqlTypeName(),
                deriveSum(argument).getSqlTypeName()
            );
        }
    }

    // ── AVG ─────────────────────────────────────────────────────────────────

    /**
     * Both numeric families average to {@code DOUBLE}, unlike {@code deriveSumType}'s two separate
     * targets. A mean is a quotient, not an accumulator: declared anywhere integral, the {@code DIVIDE} the
     * reduce rule emits is an integer division, which is a wrong number rather than a wrong width.
     */
    public void testEveryNumericFamilyAveragesToDouble() {
        List<SqlTypeName> numeric = List.of(
            SqlTypeName.TINYINT,
            SqlTypeName.SMALLINT,
            SqlTypeName.INTEGER,
            SqlTypeName.BIGINT,
            SqlTypeName.REAL,
            SqlTypeName.FLOAT,
            SqlTypeName.DOUBLE
        );
        for (SqlTypeName argument : numeric) {
            assertEquals(
                argument + " must average as DOUBLE, or the reduced AVG's DIVIDE truncates",
                SqlTypeName.DOUBLE,
                deriveAvg(nullable(typeFactory, argument)).getSqlTypeName()
            );
        }
    }

    /**
     * The contrast half of the test above, and the defect stated as a one-liner: Calcite's default returns
     * the argument type unchanged, so {@code AVG} over an {@code integer} column is declared {@code INTEGER}
     * and the reduce rule's CAST target is {@code INTEGER}. Without this, a reader cannot tell whether the
     * assertion above pins anything.
     */
    public void testDefaultTypeSystemWouldDeclareTheAvgAsInteger() {
        RelDataTypeFactory defaultFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType integer = nullable(defaultFactory, SqlTypeName.INTEGER);

        assertEquals(SqlTypeName.INTEGER, defaultFactory.getTypeSystem().deriveAvgAggType(defaultFactory, integer).getSqlTypeName());
    }

    /**
     * Nullability is carried from the argument for {@code AVG} as it is for {@code SUM}, and nothing
     * downstream would report it if it were not: {@code ReturnTypes.AVG_AGG_FUNCTION} forces nullability
     * only for an empty group, a filtered call or {@code STDDEV_SAMP}, so in a GROUPED aggregate the
     * declared and the inferred type both come from this hook — they would AGREE on a NOT NULL type and
     * {@code Aggregate}'s check would not fire. This assertion is the only guard, which is why it is here
     * and not left to a plan test.
     */
    public void testNullabilityIsCarriedFromTheAveragedArgument() {
        assertTrue(deriveAvg(nullable(typeFactory, SqlTypeName.INTEGER)).isNullable());
        assertFalse(deriveAvg(typeFactory.createSqlType(SqlTypeName.INTEGER)).isNullable());
    }

    /**
     * A family with no known mean type falls through to Calcite, never to a guess — same discipline as the
     * {@code SUM} half. {@code DECIMAL} is deliberately in this list rather than mapped to {@code DOUBLE}:
     * the engine has its own decimal rule ({@code Decimal128(p,s)} averages to
     * {@code Decimal128(p+4,s+4)}), no OpenSearch field type maps to {@code DECIMAL} today, and inventing
     * a third answer for an untestable case would contradict the engine.
     */
    public void testUnknownFamilyFallsBackToCalciteDefaultForAvg() {
        RelDataTypeFactory defaultFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        for (SqlTypeName untouched : List.of(SqlTypeName.DECIMAL, SqlTypeName.VARCHAR, SqlTypeName.BOOLEAN)) {
            RelDataType argument = nullable(typeFactory, untouched);
            assertEquals(
                untouched + " must keep Calcite's derivation",
                defaultFactory.getTypeSystem().deriveAvgAggType(defaultFactory, argument).getSqlTypeName(),
                deriveAvg(argument).getSqlTypeName()
            );
        }
    }

    /**
     * The written-{@code avg} counterpart of {@link #testWrittenSumOverAnIntegerFieldIsDeclaredBigint()},
     * and the tripwire for a HALF fix: with the type system overridden but
     * {@code AggregationMetadataBuilder#deriveTypeThroughTypeSystem} still gating on {@code SUM} alone, the
     * translator-declared {@code INTEGER} meets the inferred {@code DOUBLE} and building this plan throws
     * {@code AssertionError: type mismatch: aggCall type: INTEGER inferred type: DOUBLE} from
     * {@code LogicalAggregate.create}. Constructing the plan is the assertion.
     */
    public void testWrittenAvgOverAnIntegerFieldIsDeclaredDouble() throws ConversionException {
        RelNode plan = deepestAggregationPlan(
            new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
        );

        AggregateCall avg = onlyCallOfKind(findAggregate(plan), SqlKind.AVG);
        assertEquals(
            "a written avg over an integer field must be declared as the type system derives it",
            SqlTypeName.DOUBLE,
            avg.getType().getSqlTypeName()
        );
        assertTrue("nullability must be carried from the averaged column", avg.getType().isNullable());
    }

    /**
     * The reason declaring {@code AVG} {@code DOUBLE} is not merely cosmetic, asserted on the plan the
     * engine actually executes rather than on the hook: after the reduction there is no {@code AVG} left,
     * only a {@code SUM}, a {@code COUNT} and a rule-written {@code DIVIDE} wrapped in casts that ALL target
     * the type {@code AVG} was declared at. So a declaration at the column's own {@code INTEGER} width is
     * an executable narrowing — it casts the {@code BIGINT} the sum was correctly widened to back down to
     * i32 (the {@code Can't cast value 6400000000 to type Int32} 500) and makes the division integral (the
     * {@code 13.0}-instead-of-{@code 13.875} wrong answer). Both assertions below fail with Calcite's
     * default derivation and neither can be satisfied by fixing {@code deriveSumType} alone.
     */
    public void testReducedAvgDividesInDoubleAndNeverCastsBackToAnIntegralWidth() throws ConversionException {
        RelNode reduced = reducePlan(nestedAvgAggregationPlan());

        List<RexCall> divides = rexCallsOfKind(reduced, SqlKind.DIVIDE);
        assertFalse("the reduction must emit the AVG's DIVIDE, or this test asserts nothing", divides.isEmpty());
        for (RexCall divide : divides) {
            assertEquals(
                "the reduced AVG's division must be a floating-point division, or the mean is truncated",
                SqlTypeName.DOUBLE,
                divide.getType().getSqlTypeName()
            );
        }
        for (RexCall cast : rexCallsOfKind(reduced, SqlKind.CAST)) {
            assertFalse(
                "no cast the reduction emits may narrow to an integral width — that is the cast that "
                    + "undoes deriveSumType's widening: "
                    + cast,
                INTEGRAL.contains(cast.getType().getSqlTypeName())
            );
        }
    }

    // ── Harness ─────────────────────────────────────────────────────────────

    /** The widths a reduced {@code AVG}'s cast must never target. */
    private static final EnumSet<SqlTypeName> INTEGRAL = EnumSet.of(
        SqlTypeName.TINYINT,
        SqlTypeName.SMALLINT,
        SqlTypeName.INTEGER,
        SqlTypeName.BIGINT
    );

    private RelDataType deriveSum(RelDataType argumentType) {
        return typeFactory.getTypeSystem().deriveSumType(typeFactory, argumentType);
    }

    private RelDataType deriveAvg(RelDataType argumentType) {
        return typeFactory.getTypeSystem().deriveAvgAggType(typeFactory, argumentType);
    }

    private static RelDataType nullable(RelDataTypeFactory factory, SqlTypeName typeName) {
        return factory.createTypeWithNullability(factory.createSqlType(typeName), true);
    }

    /**
     * The AGGREGATION plan of {@code terms(brand) > terms(name) > avg(price)} over the mapping the
     * failing 2-shard IT provisions.
     */
    private static RelNode nestedAvgAggregationPlan() throws ConversionException {
        return deepestAggregationPlan(
            new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(
                    new TermsAggregationBuilder("by_name").field("name")
                        .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                )
        );
    }

    /**
     * The deepest AGGREGATION plan of the given aggregation tree, over the mapping the failing 2-shard
     * IT provisions: {@code price} integer, {@code brand}/{@code name} keyword, {@code rating} double.
     * @param rootAggregation the top-level aggregation to translate
     */
    private static RelNode deepestAggregationPlan(AggregationBuilder rootAggregation) throws ConversionException {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test-index", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder()
                    .add("name", nullable(factory, SqlTypeName.VARCHAR))
                    .add("price", nullable(factory, SqlTypeName.INTEGER))
                    .add("brand", nullable(factory, SqlTypeName.VARCHAR))
                    .add("rating", nullable(factory, SqlTypeName.DOUBLE))
                    .build();
            }
        });
        SearchSourceBuilder source = new SearchSourceBuilder().size(10).aggregation(rootAggregation);
        QueryPlans plans = new SearchSourceConverter(schema).convert(source, "test-index");
        List<QueryPlans.QueryPlan> aggregations = plans.get(QueryPlans.Type.AGGREGATION);
        assertFalse("the fixture must emit an aggregation plan", aggregations.isEmpty());
        // The deepest granularity is the one carrying the metric.
        return aggregations.get(aggregations.size() - 1).relNode();
    }

    /**
     * Runs the reduction the engine runs ({@code AVG} to {@code SUM}/{@code COUNT}) and returns the
     * aggregate. The rule is built with the same three arguments {@code OpenSearchAggregateReduceRule}
     * passes, so the reduction under test is the production one.
     */
    private static Aggregate reduceAggregateFunctions(RelNode plan) {
        return findAggregate(reducePlan(plan));
    }

    /**
     * The same reduction, returning the WHOLE reduced plan rather than just its aggregate — the casts and
     * the {@code DIVIDE} the rule writes live in the Project above the aggregate, not in the aggregate.
     *
     * @param plan the un-reduced aggregation plan
     */
    private static RelNode reducePlan(RelNode plan) {
        RelOptRule reduceRule = new AggregateReduceFunctionsRule(
            LogicalAggregate.class,
            RelBuilder.proto(Contexts.empty()),
            EnumSet.of(SqlKind.AVG, SqlKind.STDDEV_POP, SqlKind.STDDEV_SAMP, SqlKind.VAR_POP, SqlKind.VAR_SAMP)
        );
        HepPlanner planner = new HepPlanner(HepProgram.builder().addRuleInstance(reduceRule).build());
        planner.setRoot(plan);
        return planner.findBestExp();
    }

    /**
     * Every {@link RexCall} of the given kind anywhere in the plan's expressions, nested calls included.
     *
     * @param root the plan to walk
     * @param kind the call kind to collect
     */
    private static List<RexCall> rexCallsOfKind(RelNode root, SqlKind kind) {
        List<RexCall> collected = new ArrayList<>();
        Deque<RelNode> queue = new ArrayDeque<>();
        queue.add(root);
        while (queue.isEmpty() == false) {
            RelNode node = queue.removeFirst();
            node.accept(new RexShuttle() {
                @Override
                public RexNode visitCall(RexCall call) {
                    if (call.getKind() == kind) {
                        collected.add(call);
                    }
                    return super.visitCall(call);
                }
            });
            queue.addAll(node.getInputs());
        }
        return collected;
    }

    private static Aggregate findAggregate(RelNode root) {
        Deque<RelNode> queue = new ArrayDeque<>();
        queue.add(root);
        while (queue.isEmpty() == false) {
            RelNode node = queue.removeFirst();
            if (node instanceof Aggregate aggregate) {
                return aggregate;
            }
            queue.addAll(node.getInputs());
        }
        throw new AssertionError("no Aggregate in the reduced plan: " + root);
    }

    private static AggregateCall onlyCallOfKind(Aggregate aggregate, SqlKind kind) {
        List<AggregateCall> matching = callsOfKind(aggregate, kind);
        assertEquals("expected exactly one " + kind + " in " + aggregate.getAggCallList(), 1, matching.size());
        return matching.get(0);
    }

    private static List<AggregateCall> callsOfKind(Aggregate aggregate, SqlKind kind) {
        return aggregate.getAggCallList().stream().filter(call -> call.getAggregation().getKind() == kind).toList();
    }
}

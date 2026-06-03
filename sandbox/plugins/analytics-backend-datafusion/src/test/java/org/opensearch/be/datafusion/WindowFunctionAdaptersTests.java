/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlBasicAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link WindowFunctionAdapters} — DataFusion-side rewrites of PPL-form
 * window aggregates. Each test pins one rewrite contract so future planner / Calcite
 * upgrades can't silently change what isthmus emits to substrait:
 * <ul>
 *   <li>{@code ARG_MIN(value, ts)} → {@code FIRST_VALUE(value) ORDER BY ts ASC}</li>
 *   <li>{@code ARG_MAX(value, ts)} → {@code LAST_VALUE(value) ORDER BY ts ASC}</li>
 *   <li>{@code DISTINCT_COUNT_APPROX(x)} → {@code APPROX_COUNT_DISTINCT(x)}</li>
 * </ul>
 */
public class WindowFunctionAdaptersTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType varcharNullable;
    private RelDataType bigintNullable;
    private RelDataType timestampNullable;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        timestampNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
    }

    /** {@code ARG_MIN(value, ts)} → {@code FIRST_VALUE(value) OVER (... ORDER BY ts ASC)}.
     *  Operator must flip, the {@code ts} operand must move out of operands and onto the end
     *  of the order-key list; partition keys, frame bounds, and return type stay put. */
    public void testArgMinRewritesToFirstValueWithTsOrderBy() {
        RexNode value = rexBuilder.makeInputRef(varcharNullable, 0);
        RexNode ts = rexBuilder.makeInputRef(timestampNullable, 1);
        RexNode partitionKey = rexBuilder.makeInputRef(varcharNullable, 2);

        RexOver original = (RexOver) makeOverFull(
            SqlStdOperatorTable.ARG_MIN,
            varcharNullable,
            List.of(value, ts),
            List.of(partitionKey),
            ImmutableList.of()
        );

        RexNode adapted = WindowFunctionAdapters.argMin()
            .adapt(original, List.of(value, ts), List.of(partitionKey), ImmutableList.of(), cluster);

        assertTrue("ARG_MIN must rewrite to a RexOver, got " + adapted.getClass(), adapted instanceof RexOver);
        RexOver over = (RexOver) adapted;
        assertSame("ARG_MIN must rewrite to FIRST_VALUE", SqlStdOperatorTable.FIRST_VALUE, over.getAggOperator());
        assertEquals("rewritten call must keep only the value operand", 1, over.getOperands().size());
        assertSame("operand[0] must be the original value operand", value, over.getOperands().getFirst());
        assertEquals("partition keys must be preserved", List.of(partitionKey), over.getWindow().partitionKeys);
        assertEquals("ts must be appended to ORDER BY", 1, over.getWindow().orderKeys.size());
        assertSame("ORDER BY key must be the original ts operand", ts, over.getWindow().orderKeys.getFirst().left);
        assertEquals("rewritten call's return type must equal the original", original.getType(), over.getType());
    }

    /** {@code ARG_MAX(value, ts)} → {@code LAST_VALUE(value) OVER (... ORDER BY ts ASC)}. */
    public void testArgMaxRewritesToLastValueWithTsOrderBy() {
        RexNode value = rexBuilder.makeInputRef(varcharNullable, 0);
        RexNode ts = rexBuilder.makeInputRef(timestampNullable, 1);

        RexOver original = (RexOver) makeOverFull(
            SqlStdOperatorTable.ARG_MAX,
            varcharNullable,
            List.of(value, ts),
            ImmutableList.of(),
            ImmutableList.of()
        );

        RexNode adapted = WindowFunctionAdapters.argMax()
            .adapt(original, List.of(value, ts), ImmutableList.of(), ImmutableList.of(), cluster);

        RexOver over = (RexOver) adapted;
        assertSame("ARG_MAX must rewrite to LAST_VALUE", SqlStdOperatorTable.LAST_VALUE, over.getAggOperator());
        assertEquals("rewritten call must keep only the value operand", 1, over.getOperands().size());
        assertSame(value, over.getOperands().getFirst());
        assertEquals("ts must be appended to ORDER BY", 1, over.getWindow().orderKeys.size());
        assertSame(ts, over.getWindow().orderKeys.getFirst().left);
    }

    /** ARG_MIN/ARG_MAX should preserve any pre-existing ORDER BY keys and append ts after them. */
    public void testArgMinPreservesExistingOrderKeysAndAppendsTs() {
        RexNode value = rexBuilder.makeInputRef(varcharNullable, 0);
        RexNode ts = rexBuilder.makeInputRef(timestampNullable, 1);
        RexNode existingOrderKey = rexBuilder.makeInputRef(bigintNullable, 2);
        RexFieldCollation existingCollation = new RexFieldCollation(existingOrderKey, Collections.emptySet());

        RexOver original = (RexOver) makeOverFull(
            SqlStdOperatorTable.ARG_MIN,
            varcharNullable,
            List.of(value, ts),
            ImmutableList.of(),
            ImmutableList.of(existingCollation)
        );

        RexNode adapted = WindowFunctionAdapters.argMin()
            .adapt(original, List.of(value, ts), ImmutableList.of(), List.of(existingCollation), cluster);

        RexOver over = (RexOver) adapted;
        assertEquals("existing ORDER BY key must be preserved before ts", 2, over.getWindow().orderKeys.size());
        assertSame("first ORDER BY key must be the existing one", existingOrderKey, over.getWindow().orderKeys.get(0).left);
        assertSame("second ORDER BY key must be the ts operand", ts, over.getWindow().orderKeys.get(1).left);
    }

    /** {@code DISTINCT_COUNT_APPROX(x)} → {@code APPROX_COUNT_DISTINCT(x)}. The underlying UDAF is a
     *  {@link SqlKind#OTHER_FUNCTION} with operator name {@code DISTINCT_COUNT_APPROX}; the
     *  adapter rebinds the operator to Calcite's standard APPROX_COUNT_DISTINCT and preserves
     *  operands, partition keys, order keys, and the original distinct flag. */
    public void testDistinctCountApproxRewritesToApproxCountDistinct() {
        SqlAggFunction dcApproxUdaf = SqlBasicAggFunction.create(
            "DISTINCT_COUNT_APPROX",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT_FORCE_NULLABLE,
            OperandTypes.ANY
        );
        RexNode argument = rexBuilder.makeInputRef(varcharNullable, 0);
        RexNode partitionKey = rexBuilder.makeInputRef(varcharNullable, 1);

        RexOver original = (RexOver) makeOverFull(
            dcApproxUdaf,
            bigintNullable,
            List.of(argument),
            List.of(partitionKey),
            ImmutableList.of()
        );

        RexNode adapted = WindowFunctionAdapters.distinctCountApprox()
            .adapt(original, List.of(argument), List.of(partitionKey), ImmutableList.of(), cluster);

        RexOver over = (RexOver) adapted;
        assertSame(
            "DISTINCT_COUNT_APPROX UDAF must rewrite to standard APPROX_COUNT_DISTINCT",
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            over.getAggOperator()
        );
        assertEquals("operand list must be preserved", List.of(argument), over.getOperands());
        assertEquals("partition keys must be preserved", List.of(partitionKey), over.getWindow().partitionKeys);
        assertTrue("order keys must remain empty", over.getWindow().orderKeys.isEmpty());
        assertEquals("rewritten call's return type must equal the original", original.getType(), over.getType());
    }

    /** Build a {@link RexOver} with the given function, operand list, partition keys, order keys,
     *  and an UNBOUNDED PRECEDING / UNBOUNDED FOLLOWING frame — the default frame eventstats
     *  uses for ARG_MIN/ARG_MAX/DISTINCT_COUNT_APPROX. The 13-arg {@link RexBuilder#makeOver}
     *  is the same invocation {@code WindowFunctionAdapters.rebuild} uses, so this fixture
     *  exercises the same code path the production rewrite will produce. */
    private RexNode makeOverFull(
        SqlAggFunction op,
        RelDataType returnType,
        List<RexNode> operands,
        List<RexNode> partitions,
        List<RexFieldCollation> orderKeys
    ) {
        return rexBuilder.makeOver(
            returnType,
            op,
            operands,
            partitions,
            ImmutableList.copyOf(orderKeys),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            true,
            true,
            false,
            false,
            false
        );
    }
}

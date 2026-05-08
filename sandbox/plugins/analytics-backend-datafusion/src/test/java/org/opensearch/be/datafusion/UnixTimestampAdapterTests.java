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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link UnixTimestampAdapter} — the cat-3a rename adapter that
 * rewrites PPL's bespoke {@code UNIX_TIMESTAMP} operator to a locally-declared
 * {@code to_unixtime} {@link SqlFunction} whose {@code FunctionMappings.Sig} we
 * own. Target name {@code to_unixtime} matches DataFusion's native function; no
 * UDF registration required on the Rust side.
 */
public class UnixTimestampAdapterTests extends OpenSearchTestCase {

    public void testUnixTimestampRewritesToLocalToUnixtimeOperator() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        // Synthesize UNIX_TIMESTAMP(ts) with PPL's return type (DOUBLE_FORCE_NULLABLE).
        RelDataType tsType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        SqlFunction unixTimestampOp = new SqlFunction(
            "UNIX_TIMESTAMP",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(doubleNullable),
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode tsRef = rexBuilder.makeInputRef(tsType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(unixTimestampOp, List.of(tsRef));

        RexNode adapted = new UnixTimestampAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(
            "adapted call must target UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP so the "
                + "FunctionMappings.Sig in DataFusionFragmentConvertor can bind by reference",
            UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP,
            call.getOperator()
        );
        assertEquals("to_unixtime is a pure rename — 1 operand preserved", 1, call.getOperands().size());
        assertSame("arg 0 must be the original timestamp operand", tsRef, call.getOperands().get(0));
    }

    /**
     * Regression guard mirroring {@code YearAdapterTests.testAdaptedCallPreservesOriginalReturnType}.
     * PPL's {@code UNIX_TIMESTAMP} is typed {@code DOUBLE_FORCE_NULLABLE}; DF's
     * {@code to_unixtime} is typed {@code Int64}. The adapter must preserve the
     * original DOUBLE type so the enclosing Project / Filter's cached rowType
     * doesn't mismatch during fragment conversion. (DataFusion's substrait
     * consumer re-resolves {@code to_unixtime} by name at plan time and applies
     * its own coerce_types pass — the Calcite-inferred return type at isthmus
     * time is purely a plan-validity artifact.)
     */
    public void testAdaptedCallPreservesOriginalReturnType() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType tsType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        SqlFunction unixTimestampOp = new SqlFunction(
            "UNIX_TIMESTAMP",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(doubleNullable),
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode tsRef = rexBuilder.makeInputRef(tsType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(unixTimestampOp, List.of(tsRef));
        assertEquals(doubleNullable, original.getType());

        RexNode adapted = new UnixTimestampAdapter().adapt(original, List.of(), cluster);

        assertEquals(
            "adapted call's return type must equal the original — otherwise the enclosing Project.rowType "
                + "assertion fails during fragment conversion",
            original.getType(),
            adapted.getType()
        );
    }
}

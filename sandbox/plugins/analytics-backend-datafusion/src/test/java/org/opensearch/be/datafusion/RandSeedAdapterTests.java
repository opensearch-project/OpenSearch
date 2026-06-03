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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link RandSeedAdapter}. PPL {@code rand([seed])} accepts an optional seed that
 * DataFusion's niladic {@code random()} cannot express. The adapter drops the seed and emits the
 * niladic {@link SqlStdOperatorTable#RAND}, which {@code DataFusionFragmentConvertor} maps to
 * DataFusion's {@code random()} — avoiding {@code Unable to convert call RAND(i32)}.
 */
public class RandSeedAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType doubleType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    }

    public void testNiladicRandPassedThrough() {
        RexCall original = (RexCall) rexBuilder.makeCall(doubleType, SqlStdOperatorTable.RAND, List.of());

        RexNode adapted = new RandSeedAdapter().adapt(original, List.of(), cluster);

        RexCall call = (RexCall) adapted;
        assertSame("operator stays RAND", SqlStdOperatorTable.RAND, call.getOperator());
        assertTrue("no operands", call.getOperands().isEmpty());
    }

    public void testSeededRandRejectedWithClearError() {
        RexNode seed = rexBuilder.makeLiteral(42, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
        RexCall original = (RexCall) rexBuilder.makeCall(doubleType, SqlStdOperatorTable.RAND, List.of(seed));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RandSeedAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue("error should explain seeded RAND is unsupported", e.getMessage().toLowerCase(java.util.Locale.ROOT).contains("seed"));
    }
}

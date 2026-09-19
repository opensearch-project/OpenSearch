/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;

/**
 * The two Calcite operators a join runtime filter is expressed with.
 *
 * <p>Both live here rather than in a backend plugin because the planner builds plans containing
 * them — the pre-pass fragment aggregates with {@link #BLOOM_AGG}, and the probe-side fragment
 * carries a {@link #PROBE} predicate — while only a backend can execute them. This mirrors
 * {@link DelegatedPredicateFunction}, which is a planner-injected placeholder for the same reason.
 *
 * <p>Neither needs an entry in a backend's function YAML. Those describe user-callable PPL
 * functions so the frontend can resolve them; these are injected by the planner and bound by the
 * backend's own operator-to-extension mapping, exactly as {@code delegated_predicate} is.
 *
 * @opensearch.internal
 */
public final class RuntimeFilterFunction {

    /** Build side: accumulates one fixed-size Bloom over the join key, per shard. */
    public static final String BLOOM_AGG_NAME = "os_bloom_agg";

    /** Probe side: `true` keeps the row, `false` proves the key is absent from the build side. */
    public static final String PROBE_NAME = "os_runtime_filter";

    /**
     * {@code os_bloom_agg(key, numBytes) → VARBINARY}.
     *
     * <p>{@code numBytes} is an argument rather than a backend setting because every shard's
     * contribution must be built to the same size for the coordinator's union to be defined, and an
     * argument in the plan is what makes that agreement per-query and visible.
     */
    public static final SqlAggFunction BLOOM_AGG = new SqlAggFunction(
        BLOOM_AGG_NAME,
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARBINARY,
        null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC),
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /**
     * {@code os_runtime_filter(filterId, key) → BOOLEAN}.
     *
     * <p>Carries only the id; the filter's bytes arrive separately as a
     * {@link RuntimeFilterInstructionNode}. A backend that has no filter under that id must answer
     * {@code true} for every row, which is what makes the predicate safe to plant before knowing
     * whether the payload will arrive.
     */
    public static final SqlFunction PROBE = new SqlFunction(
        PROBE_NAME,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private RuntimeFilterFunction() {}

    /** Builds an {@code os_runtime_filter(filterId, key)} predicate over {@code key}. */
    public static RexNode makeProbeCall(RexBuilder rexBuilder, int filterId, RexNode key) {
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        return rexBuilder.makeCall(PROBE, rexBuilder.makeLiteral(filterId, intType, false), key);
    }

    /** The {@code numBytes} literal for a {@link #BLOOM_AGG} call. */
    public static RexNode makeSizeLiteral(RexBuilder rexBuilder, int numBytes) {
        RelDataType intType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        return rexBuilder.makeLiteral(numBytes, intType, false);
    }
}

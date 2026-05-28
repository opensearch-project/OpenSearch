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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;

/**
 * Custom Calcite {@link SqlAggFunction} singletons defined by the OpenSearch analytics
 * engine but referenced by frontends (the SQL/PPL parser library, planner-side rules) —
 * kept in the SPI module so any party can import the same singleton.
 *
 * <p>Currently defines:
 * <ul>
 *   <li>{@link #STATS} — bundle aggregate returning {@code STRUCT(count, min, max, avg, sum)}.
 *       Decomposed at HEP planning time by
 *       {@code OpenSearchStatsReduceRule} into primitive COUNT/MIN/MAX/SUM aggregate calls
 *       plus a Project that builds the struct, so no executor ever sees STATS.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class OpenSearchAggregateOperators {

    private OpenSearchAggregateOperators() {}

    /**
     * Field names of the struct returned by {@link #STATS}. Public so the planner-side rule
     * (which builds the Project that re-assembles the struct) and any test fixture can use
     * the same names without re-declaring constants.
     */
    public static final String STATS_FIELD_COUNT = "count";
    public static final String STATS_FIELD_MIN = "min";
    public static final String STATS_FIELD_MAX = "max";
    public static final String STATS_FIELD_SUM = "sum";
    public static final String STATS_FIELD_AVG = "avg";

    /**
     * Return-type inference for {@link #STATS}. Produces a struct whose component types match
     * what Calcite's built-in COUNT/MIN/MAX/SUM aggregates would produce for the same operand,
     * with AVG fixed to DOUBLE. Matching matters because the Project the decomposition rule
     * emits must produce the same row type the original STATS aggregate declared.
     */
    private static final SqlReturnTypeInference STATS_RETURN_TYPE_INFERENCE = OpenSearchAggregateOperators::inferStatsReturnType;

    /**
     * STATS bundle aggregate. Decomposed at HEP planning time — never reaches the executor.
     */
    public static final SqlAggFunction STATS = new SqlAggFunction(
        "STATS",
        null,
        SqlKind.OTHER,
        STATS_RETURN_TYPE_INFERENCE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    private static RelDataType inferStatsReturnType(SqlOperatorBinding opBinding) {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        RelDataType inputType = opBinding.getOperandType(0);

        RelDataType countType = typeFactory.createSqlType(SqlTypeName.BIGINT);

        RelDataType nullableInput = typeFactory.createTypeWithNullability(inputType, true);

        // Delegate to Calcite's SUM type inference. AGG_SUM widens INTEGER → BIGINT,
        // BIGINT → DECIMAL, DOUBLE → DOUBLE, etc. Falls back to the input type if the
        // inference returns null (shouldn't happen for numeric-validated operands).
        RelDataType sumType = ReturnTypes.AGG_SUM.inferReturnType(opBinding);
        if (sumType == null) {
            sumType = nullableInput;
        } else {
            sumType = typeFactory.createTypeWithNullability(sumType, true);
        }

        RelDataType avgType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);

        return typeFactory.builder()
            .add(STATS_FIELD_COUNT, countType)
            .add(STATS_FIELD_MIN, nullableInput)
            .add(STATS_FIELD_MAX, nullableInput)
            .add(STATS_FIELD_AVG, avgType)
            .add(STATS_FIELD_SUM, sumType)
            .build();
    }
}

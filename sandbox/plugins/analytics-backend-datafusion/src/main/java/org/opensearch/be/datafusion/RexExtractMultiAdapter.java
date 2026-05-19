/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapter for the PPL {@code rex field=f "(?<g>...)" max_match=N} extract
 * command's multi-match form. Rewrites the SQL plugin's
 * {@code PPLBuiltinOperators.REX_EXTRACT_MULTI} call to
 * {@link #LOCAL_REX_EXTRACT_MULTI_OP}, which
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} maps to the
 * {@code rex_extract_multi} Substrait extension.
 *
 * <p>Same literal-validation contract as {@link RexExtractAdapter} for the
 * pattern and group operands. The {@code max_match} integer can be either a
 * literal or a column ref — the Rust UDF handles per-row max_match values.
 *
 * <p>Return type is {@code ARRAY<VARCHAR_2000?>} (matching the SQL plugin's
 * {@code RexExtractMultiFunction.getReturnTypeInference}).
 *
 * @opensearch.internal
 */
class RexExtractMultiAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator. The {@code returnTypeInference} produces
     * an {@code ARRAY<VARCHAR>} matching the SQL plugin's UDF return type, so
     * downstream Project / Filter nodes don't see a type mismatch when the
     * adapter swaps the operator.
     */
    static final SqlOperator LOCAL_REX_EXTRACT_MULTI_OP = new SqlFunction("rex_extract_multi", SqlKind.OTHER_FUNCTION, opBinding -> {
        RelDataType varchar = opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 2000);
        RelDataType nullableVarchar = opBinding.getTypeFactory().createTypeWithNullability(varchar, true);
        return opBinding.getTypeFactory().createArrayType(nullableVarchar, -1);
    },
        null,
        OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
        SqlFunctionCategory.STRING
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 4) {
            throw new IllegalArgumentException(
                "rex_extract_multi: expected 4 operands (input, pattern, group, max_match), got " + original.getOperands().size()
            );
        }
        // Pattern and group must be literals — same reasoning as RexExtractAdapter.
        // max_match (operand 3) can be column-valued; the Rust UDF handles it per-row.
        RexExtractAdapter.validateLiteral(original.getOperands().get(1), "pattern");
        RexExtractAdapter.validateLiteral(original.getOperands().get(2), "group");
        RexBuilder rexBuilder = cluster.getRexBuilder();
        return rexBuilder.makeCall(original.getType(), LOCAL_REX_EXTRACT_MULTI_OP, original.getOperands());
    }
}

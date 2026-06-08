/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapter for the PPL {@code rex field=f "(?<g>...)" offset_field=name}
 * command's offset-emission form. Rewrites the SQL plugin's
 * {@code PPLBuiltinOperators.REX_OFFSET} call to {@link #LOCAL_REX_OFFSET_OP},
 * which {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} maps to the
 * {@code rex_offset} Substrait extension.
 *
 * <p>Pattern operand MUST be a string literal. Same reasoning as
 * {@link RexExtractAdapter}: regex compilation is per-call, not per-row.
 *
 * <p>Return type is {@code VARCHAR_2000_NULLABLE} — the Rust UDF formats
 * group offsets as a single string {@code "name1=s1-e1&name2=s2-e2"} sorted
 * alphabetically. Matches the Java {@code RexOffsetFunction} return type.
 *
 * @opensearch.internal
 */
class RexOffsetAdapter implements ScalarFunctionAdapter {

    /** Local target operator — see RexExtractAdapter for the pattern rationale. */
    static final SqlOperator LOCAL_REX_OFFSET_OP = new SqlFunction(
        "rex_offset",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.STRING
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            throw new IllegalArgumentException("rex_offset: expected 2 operands (input, pattern), got " + original.getOperands().size());
        }
        RexExtractAdapter.validateLiteral(original.getOperands().get(1), "pattern");
        RexBuilder rexBuilder = cluster.getRexBuilder();
        return rexBuilder.makeCall(original.getType(), LOCAL_REX_OFFSET_OP, original.getOperands());
    }
}

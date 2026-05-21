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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * Rename adapter for PPL's {@code PATTERN_PARSER(pattern, field)}. Rewrites
 * to the locally-declared {@code pattern_parser} operator that
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} routes to the
 * substrait extension name registered in {@code opensearch_scalar_functions.yaml}.
 * The Rust side implementation lives in
 * {@code crate::udf::pattern_parser::PatternParserUdf}.
 *
 * <p>Two operand shapes are accepted by the same locally-declared operator:
 * {@code pattern_parser(string, string)} (evalField — single row, used by PPL
 * SIMPLE patterns mode with {@code show_numbered_token=true}), and
 * {@code pattern_parser(string, List<string>)} (evalSamples — aggregate
 * sample list, used by SIMPLE patterns aggregation mode with
 * {@code show_numbered_token=true}). The Rust UDF dispatches at runtime on
 * the second operand's Arrow type.
 *
 * <p>The 3-arg evalAgg shape used by BRAIN label mode goes through a separate
 * adapter / window-UDF pipeline (next milestone).
 *
 * @opensearch.internal
 */
class PatternParserAdapter extends AbstractNameMappingAdapter {

    /**
     * Locally-declared target operator. Name matches the substrait extension
     * entry in {@code opensearch_scalar_functions.yaml}. Return-type inference
     * is informational — the adapter preserves the original PPL declared
     * return type on the rewritten call so Calcite's {@code Project.isValid}
     * assertion holds.
     */
    static final SqlOperator LOCAL_PATTERN_PARSER_OP = new SqlFunction(
        "pattern_parser",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    PatternParserAdapter() {
        super(LOCAL_PATTERN_PARSER_OP, List.of(), List.of());
    }

    /**
     * The PPL declared return type of {@code PATTERN_PARSER} is
     * {@code MAP<VARCHAR, ANY>} (see {@code UserDefinedFunctionUtils.patternStruct})
     * because the legacy v2 path returns a heterogeneous Java map. The embedded
     * {@code ANY} cannot be serialised to Substrait — isthmus throws
     * {@code "Unable to convert the type ANY"}. Substitute a concrete struct
     * matching the Rust UDF's output Arrow schema
     * ({@code rust/src/udf/pattern_parser.rs}):
     *
     * <pre>{@code
     *   STRUCT<pattern: VARCHAR, tokens: MAP<VARCHAR, ARRAY<VARCHAR>>>
     * }</pre>
     *
     * <p>Downstream {@code flattenParsedPattern} in the PPL Calcite visitor
     * accesses both fields via {@code ITEM(parsedNode, "pattern" | "tokens")}.
     * Calcite's {@code ITEM} on a STRUCT with a constant string operand resolves
     * to named struct-field access, which works identically to the legacy
     * {@code ITEM(map, key)} lookup against {@code MAP<VARCHAR, ANY>}.
     */
    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType structType = patternStructType(rexBuilder.getTypeFactory(), original.getType().isNullable());
        return rexBuilder.makeCall(structType, LOCAL_PATTERN_PARSER_OP, original.getOperands());
    }

    private static RelDataType patternStructType(RelDataTypeFactory typeFactory, boolean nullable) {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType varcharArray = typeFactory.createArrayType(varchar, -1);
        RelDataType tokensMap = typeFactory.createMapType(varchar, varcharArray);
        RelDataType struct = typeFactory.createStructType(
            List.of(varchar, tokensMap),
            List.of("pattern", "tokens")
        );
        return typeFactory.createTypeWithNullability(struct, nullable);
    }
}

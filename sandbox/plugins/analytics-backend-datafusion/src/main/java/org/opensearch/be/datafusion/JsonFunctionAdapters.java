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
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Container for PPL JSON-function scalar adapters. Each inner class is a plain
 * name-mapping rewrite from a Calcite call to a locally-declared
 * {@link SqlOperator} whose name matches the corresponding Rust UDF at
 * {@code rust/src/udf/<name>.rs}. All validation (malformed JSON, malformed
 * path, arity / pairing, any-NULL propagation) lives in the Rust UDF; the
 * adapter does not inspect arguments. Return type is preserved from the
 * original PPL call by {@link AbstractNameMappingAdapter#adapt}, matching the
 * {@code *_FORCE_NULLABLE} declaration on the legacy {@code Json*FunctionImpl}.
 *
 * <p>Each {@code LOCAL_*_OP} must also be registered in
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} via a
 * {@code FunctionMappings.s(...)} entry keyed by the UDF's name.
 *
 * @opensearch.internal
 */
final class JsonFunctionAdapters {

    private JsonFunctionAdapters() {}

    /** {@code JSON_ARRAY_LENGTH(value)} → length of a JSON array; NULL on non-array / malformed input. */
    static class JsonArrayLengthAdapter extends AbstractNameMappingAdapter {

        static final SqlOperator LOCAL_JSON_ARRAY_LENGTH_OP = new SqlFunction(
            "json_array_length",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE,
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );

        JsonArrayLengthAdapter() {
            super(LOCAL_JSON_ARRAY_LENGTH_OP, List.of(), List.of());
        }
    }

    /** {@code JSON_KEYS(value)} → JSON-array-encoded top-level keys; NULL on non-object / malformed input. */
    static class JsonKeysAdapter extends AbstractNameMappingAdapter {

        static final SqlOperator LOCAL_JSON_KEYS_OP = new SqlFunction(
            "json_keys",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );

        JsonKeysAdapter() {
            super(LOCAL_JSON_KEYS_OP, List.of(), List.of());
        }
    }

    /** {@code JSON_EXTRACT(value, path1, [path2, ...])} — single path → stringified match; multi-path → JSON-array wrap with {@code null} slots for misses. */
    static class JsonExtractAdapter extends AbstractNameMappingAdapter {

        static final SqlOperator LOCAL_JSON_EXTRACT_OP = new SqlFunction(
            "json_extract",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING
        );

        JsonExtractAdapter() {
            super(LOCAL_JSON_EXTRACT_OP, List.of(), List.of());
        }
    }

    /**
     * {@code JSON_EXTRACT_ALL(value)} — flatten a JSON document to a
     * {@code MAP<VARCHAR, VARCHAR>} keyed by dot-separated path; non-object /
     * top-level scalar / null / empty / whitespace inputs → NULL map; malformed
     * → empty map. Matches the legacy {@code JsonExtractAllFunctionImpl} on the
     * v2 / Calcite path. The locally-declared return type here is a placeholder
     * — {@link AbstractNameMappingAdapter#adapt} preserves the original PPL
     * call's MAP RelDataType, so isthmus emits a substrait map type and the
     * DataFusion-side substrait consumer decodes it to Arrow {@code Map<Utf8, Utf8>}.
     */
    static class JsonExtractAllAdapter extends AbstractNameMappingAdapter {

        static final SqlOperator LOCAL_JSON_EXTRACT_ALL_OP = new SqlFunction(
            "json_extract_all",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );

        JsonExtractAllAdapter() {
            super(LOCAL_JSON_EXTRACT_ALL_OP, List.of(), List.of());
        }
    }

    /** {@code JSON_DELETE(value, path1, [path2, ...])} — remove PPL-path matches; missing paths are no-ops. */
    static class JsonDeleteAdapter extends AbstractNameMappingAdapter {

        static final SqlOperator LOCAL_JSON_DELETE_OP = new SqlFunction(
            "json_delete",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING
        );

        JsonDeleteAdapter() {
            super(LOCAL_JSON_DELETE_OP, List.of(), List.of());
        }
    }

    /** {@code JSON_SET(value, path1, val1, [path2, val2, ...])} — replace-only; missing paths are no-ops (parity with legacy {@code ctx.read != null} guard). */
    static class JsonSetAdapter extends AbstractNameMappingAdapter {

        static final SqlOperator LOCAL_JSON_SET_OP = new SqlFunction(
            "json_set",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING
        );

        JsonSetAdapter() {
            super(LOCAL_JSON_SET_OP, List.of(), List.of());
        }
    }

    /** {@code JSON_APPEND(value, path1, val1, [path2, val2, ...])} — push-only onto array-valued targets; non-array / missing targets are no-ops. */
    static class JsonAppendAdapter extends AbstractNameMappingAdapter {

        static final SqlOperator LOCAL_JSON_APPEND_OP = new SqlFunction(
            "json_append",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING
        );

        JsonAppendAdapter() {
            super(LOCAL_JSON_APPEND_OP, List.of(), List.of());
        }
    }

    /** {@code JSON_EXTEND(value, path1, val1, [path2, val2, ...])} — spread-or-append: JSON-array values are spread element-wise; otherwise the whole value is pushed as one string element. */
    static class JsonExtendAdapter extends AbstractNameMappingAdapter {

        static final SqlOperator LOCAL_JSON_EXTEND_OP = new SqlFunction(
            "json_extend",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING
        );

        JsonExtendAdapter() {
            super(LOCAL_JSON_EXTEND_OP, List.of(), List.of());
        }
    }

    /**
     * {@code JSON_VALID(str)} → boolean; TRUE iff the input parses as JSON,
     * FALSE on malformed input, NULL on NULL input.
     *
     * <p>Source PPL call uses {@code SqlStdOperatorTable.IS_JSON_VALUE} — a
     * {@link org.apache.calcite.sql.SqlPostfixOperator} named {@code "IS JSON VALUE"},
     * which isthmus cannot serialise (no Substrait mapping) and DataFusion does
     * not recognise. The adapter rewrites it to a locally-declared {@code json_valid}
     * {@link SqlFunction} whose name matches the Rust UDF at {@code rust/src/udf/json_valid.rs}.
     *
     * <p>Null-propagating, matching Calcite {@code JsonFunctions.isJsonValue}
     * ({@code if (input == null) return null}) and the official PPL doc
     * (sql/docs/user/ppl/functions/json.md — "NULL input returns NULL"). This
     * is the SQL:2016 scalar-UDF convention and the majority industry contract
     * (MySQL, SQL Server, Snowflake, Trino, DuckDB). Return type
     * {@link ReturnTypes#BOOLEAN_NULLABLE} matches the postfix operator's declared
     * type so {@link AbstractNameMappingAdapter#adapt} preserves it unchanged.
     */
    static class JsonValidAdapter extends AbstractNameMappingAdapter {

        static final SqlOperator LOCAL_JSON_VALID_OP = new SqlFunction(
            "json_valid",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN_NULLABLE,
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );

        JsonValidAdapter() {
            super(LOCAL_JSON_VALID_OP, List.of(), List.of());
        }
    }

    /** {@code JSON(s)} — round-trip parse a JSON string; NULL on malformed input. */
    static class JsonAdapter extends AbstractNameMappingAdapter {

        static final SqlOperator LOCAL_JSON_OP = new SqlFunction(
            "json",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );

        JsonAdapter() {
            super(LOCAL_JSON_OP, List.of(), List.of());
        }
    }

    /**
     * {@code JSON_OBJECT(k1, v1, k2, v2, ...)} — JSON-object constructor.
     *
     * <p>Calcite lowers PPL's {@code json_object} to {@code SqlStdOperatorTable.JSON_OBJECT}
     * with a leading {@code SqlJsonConstructorNullClause.NULL_ON_NULL} flag operand
     * prepended to the user args. This adapter strips that flag, casts every key + value
     * to VARCHAR (substrait variadic-consistency), and inserts a per-value type-tag
     * literal so the Rust UDF can recover the original Calcite type when emitting JSON
     * (numerics unquoted, strings quoted, nested-JSON results escaped). Final operand
     * shape passed to {@link #LOCAL_JSON_OBJECT_OP}: {@code [k1, tag1, v1, k2, tag2, v2, ...]}.
     */
    static class JsonObjectAdapter implements ScalarFunctionAdapter {

        static final SqlOperator LOCAL_JSON_OBJECT_OP = new SqlFunction(
            "json_object",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING
        );

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RelDataType varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
            List<RexNode> userArgs = stripLeadingFlag(original.getOperands());
            List<RexNode> rewritten = new ArrayList<>(userArgs.size() * 3 / 2);
            // Key/value pairs. Even index = key (always cast to VARCHAR);
            // odd index = value (tag + cast).
            for (int i = 0; i < userArgs.size(); i += 2) {
                RexNode key = userArgs.get(i);
                rewritten.add(castToVarchar(rexBuilder, varchar, key));
                if (i + 1 < userArgs.size()) {
                    RexNode value = userArgs.get(i + 1);
                    rewritten.add(rexBuilder.makeLiteral(typeTag(value), varchar, true));
                    rewritten.add(castToVarchar(rexBuilder, varchar, value));
                }
            }
            return rexBuilder.makeCall(original.getType(), LOCAL_JSON_OBJECT_OP, rewritten);
        }
    }

    /**
     * {@code JSON_ARRAY(v1, v2, ...)} — JSON-array constructor.
     *
     * <p>Same flag-stripping + type-tagging pattern as {@link JsonObjectAdapter}, but
     * the operands here are values only (no keys). Final operand shape:
     * {@code [tag1, v1, tag2, v2, ...]}.
     */
    static class JsonArrayAdapter implements ScalarFunctionAdapter {

        static final SqlOperator LOCAL_JSON_ARRAY_OP = new SqlFunction(
            "json_array",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_NULLABLE,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING
        );

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RelDataType varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
            List<RexNode> userArgs = stripLeadingFlag(original.getOperands());
            List<RexNode> rewritten = new ArrayList<>(userArgs.size() * 2);
            for (RexNode value : userArgs) {
                rewritten.add(rexBuilder.makeLiteral(typeTag(value), varchar, true));
                rewritten.add(castToVarchar(rexBuilder, varchar, value));
            }
            return rexBuilder.makeCall(original.getType(), LOCAL_JSON_ARRAY_OP, rewritten);
        }
    }

    /**
     * Drop the leading {@link org.apache.calcite.sql.SqlJsonConstructorNullClause}
     * flag operand that {@code PPLFuncImpTable} prepends to JSON_OBJECT / JSON_ARRAY
     * calls. Calcite emits the flag as a {@code RexLiteral} of {@code SYMBOL} kind;
     * this method removes that single leading literal if present, leaving the
     * caller-supplied user operands untouched.
     */
    private static List<RexNode> stripLeadingFlag(List<RexNode> operands) {
        if (operands.isEmpty()) return operands;
        RexNode head = operands.get(0);
        if (head.getType().getSqlTypeName() == SqlTypeName.SYMBOL) {
            return operands.subList(1, operands.size());
        }
        return operands;
    }

    /**
     * Classify an operand for the JSON_OBJECT / JSON_ARRAY tag stream. Tags drive
     * the Rust UDF's per-value emission rule (number unquoted, bool unquoted,
     * nested-JSON-result quoted-and-escaped, plain string quoted).
     */
    private static String typeTag(RexNode value) {
        if (value instanceof RexCall call) {
            String op = call.getOperator().getName();
            if (op.equals("json_object") || op.equals("json_array") || op.equals("json")) {
                return "j";
            }
        }
        SqlTypeName tn = value.getType().getSqlTypeName();
        if (tn == SqlTypeName.BOOLEAN) return "b";
        if (SqlTypeFamily.NUMERIC.contains(value.getType())) return "n";
        return "s";
    }

    /** Insert a CAST-to-VARCHAR if the operand isn't already character-typed. */
    private static RexNode castToVarchar(RexBuilder rexBuilder, RelDataType varchar, RexNode operand) {
        if (SqlTypeFamily.CHARACTER.contains(operand.getType())) {
            return operand;
        }
        return rexBuilder.makeCast(varchar, operand, true, false);
    }
}

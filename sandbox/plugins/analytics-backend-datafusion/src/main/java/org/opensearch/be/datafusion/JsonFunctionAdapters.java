/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;

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
}

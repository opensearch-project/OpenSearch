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
 * Container for PPL JSON-function scalar adapters. Each inner class rewrites a
 * Calcite call for a PPL {@code json_*} operator to a locally-declared
 * {@link SqlOperator} whose name matches the corresponding Rust UDF registered
 * by {@code rust/src/udf/<name>.rs}.
 *
 * <p>All JSON UDFs ship as new Rust UDFs paired with a Java adapter rename: no
 * DataFusion builtin and no Substrait stdlib equivalent. Simple renames extend
 * {@link AbstractNameMappingAdapter}.
 *
 * <p>Each inner adapter's {@code LOCAL_*_OP} must also be registered in
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} via a
 * {@code FunctionMappings.s(...)} entry matching the UDF's name.
 *
 * @opensearch.internal
 */
final class JsonFunctionAdapters {

    private JsonFunctionAdapters() {}

    /**
     * Adapter for PPL's {@code JSON_ARRAY_LENGTH(value)}. Rewrites to the
     * locally-declared {@link #LOCAL_JSON_ARRAY_LENGTH_OP}; isthmus resolves it
     * against the {@code json_array_length} YAML signature; DataFusion routes to
     * the Rust UDF ({@code rust/src/udf/json_array_length.rs}). All validation
     * (malformed JSON, non-array) happens inside the UDF — the adapter is a
     * plain rename.
     *
     * <p>Return type is preserved from the original PPL call via
     * {@link AbstractNameMappingAdapter#adapt}, matching PPL's
     * {@code INTEGER_FORCE_NULLABLE} declaration from
     * {@code JsonArrayLengthFunctionImpl}.
     */
    static class JsonArrayLengthAdapter extends AbstractNameMappingAdapter {

        /**
         * Locally-declared target operator. The name {@code "json_array_length"}
         * matches the Rust UDF registered in {@code rust/src/udf/json_array_length.rs}
         * so DataFusion's substrait consumer resolves to it by name.
         * {@link SqlKind#OTHER_FUNCTION} keeps this off Calcite's builtin
         * resolution path; {@link OperandTypes#STRING} reflects the PPL contract.
         * {@link ReturnTypes#INTEGER_NULLABLE} is a placeholder — the return
         * type on the rewritten call is preserved from the original by
         * {@link AbstractNameMappingAdapter#adapt}.
         */
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

    /**
     * Adapter for PPL's {@code JSON_KEYS(value)}. Plain rename to the
     * Rust UDF at {@code rust/src/udf/json_keys.rs}; all validation
     * (malformed JSON, non-object input) lives in the UDF. Return type is
     * preserved from the original PPL call, matching {@code STRING_FORCE_NULLABLE}
     * declared on {@code JsonKeysFunctionImpl}.
     */
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

    /**
     * Adapter for PPL's {@code JSON_EXTRACT(value, path1, [path2, ...])}. Plain
     * rename to the Rust UDF at {@code rust/src/udf/json_extract.rs}; all
     * validation (arity short-circuit, malformed JSON, malformed path, per-path
     * NULL) lives in the UDF. Return type is preserved from the original PPL
     * call, matching {@code STRING_FORCE_NULLABLE} declared on
     * {@code JsonExtractFunctionImpl}.
     *
     * <p>Operands are homogeneously-typed strings, so the substrait YAML
     * signature uses a single {@code variadic: {min: 1}} declaration.
     */
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
     * Adapter for PPL's {@code JSON_DELETE(value, path1, [path2, ...])}. Plain
     * rename to the Rust UDF at {@code rust/src/udf/json_delete.rs}; all
     * validation (malformed JSON, malformed path, any-NULL-arg propagation)
     * lives in the UDF. Return type is preserved from the original PPL call,
     * matching {@code STRING_FORCE_NULLABLE} declared on
     * {@code JsonDeleteFunctionImpl}.
     *
     * <p>Operands are homogeneously-typed strings; the substrait YAML
     * signature uses {@code variadic: {min: 1}} so isthmus accepts any
     * non-zero path count.
     */
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
}

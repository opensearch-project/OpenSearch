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
 * <p>All JSON UDFs are Cat-4 (new Rust UDF + Java adapter rename): no DataFusion
 * builtin and no Substrait stdlib equivalent. Simple renames extend
 * {@link AbstractNameMappingAdapter}; path-aware / literal-canonicalizing JSON
 * functions implement {@link org.opensearch.analytics.spi.ScalarFunctionAdapter}
 * directly (see {@link ConvertTzAdapter} for the precedent).
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
     * Cat-4 adapter for PPL's {@code JSON_ARRAY_LENGTH(value)}. Rewrites to the
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
}

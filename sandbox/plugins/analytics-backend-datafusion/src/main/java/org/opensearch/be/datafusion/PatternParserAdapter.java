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
}

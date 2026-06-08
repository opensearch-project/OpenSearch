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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;

import java.util.List;

/**
 * Cat-4 rename adapter for PPL's {@code conv(n, fromBase, toBase)} — base conversion. Rewrites
 * the PPL UDF (registered upstream under the name {@code "CONVERT"} by
 * {@code ConvFunction.toUDF}) to a locally-declared {@link SqlFunction} whose substrait sig is
 * mapped to the {@code conv} Rust UDF (see {@code rust/src/udf/conv.rs} and the YAML signature in
 * {@code src/main/resources/opensearch_scalar_functions.yaml}).
 *
 * <p>Semantics mirror {@code Long.toString(Long.parseLong(n, fromBase), toBase)} — same as PPL's
 * {@code ConvFunction.conv}. Operand types: any string/integer for {@code n}, integer for the
 * two bases. Return type: string.
 *
 * @opensearch.internal
 */
class ConvAdapter extends AbstractNameMappingAdapter {

    static final SqlOperator LOCAL_CONV_OP = new SqlFunction(
        "conv",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    ConvAdapter() {
        super(LOCAL_CONV_OP, List.of(), List.of());
    }
}

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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Dedicated Calcite {@link SqlFunction} paired with the {@code signum} Substrait
 * extension declared in {@code opensearch_scalar_functions.yaml}. The PPL
 * frontend emits {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#SIGN},
 * which isthmus's default {@code SCALAR_SIGS} maps to the Substrait name
 * {@code sign} — the name DataFusion's substrait consumer does not accept
 * (DataFusion registers the UDF as {@code signum}).
 *
 * <p>An {@link org.opensearch.analytics.spi.AbstractNameMappingAdapter} registered
 * against {@code ScalarFunction.SIGN} rewrites the incoming PPL {@code SIGN} call
 * to use {@code SignumFunction.FUNCTION}, and {@code ADDITIONAL_SCALAR_SIGS} in
 * {@link DataFusionFragmentConvertor} maps this operator to the {@code signum}
 * extension name. Keeping a separate Calcite operator avoids a collision with
 * the default {@code SIGN → sign} mapping and makes isthmus serialisation
 * deterministic independent of map iteration order.
 *
 * @opensearch.internal
 */
final class SignumFunction {

    /** Substrait extension function name declared in opensearch_scalar_functions.yaml. */
    static final String NAME = "signum";

    /** Calcite operator binding: {@code signum(NUMERIC) → DOUBLE}. */
    static final SqlFunction FUNCTION = new SqlFunction(
        NAME.toUpperCase(java.util.Locale.ROOT),
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    );

    private SignumFunction() {}
}

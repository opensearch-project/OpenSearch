/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Optionality;

/**
 * Aggregate function stubs for {@code PERCENTILE_APPROX(field, percent)} and the
 * explicit-compression {@code PERCENTILE_APPROX_N(field, percent, centroids)}. The
 * DataFusion backend's rewriter matches both by name and binds them to
 * {@code approx_percentile_cont}, rescaling the percent literal to [0, 1] at emission.
 */
final class PercentileApproxFunction extends SqlAggFunction {

    /** Two-arg form: {@code PERCENTILE_APPROX(field, percent)} — engine-default compression. */
    static final PercentileApproxFunction INSTANCE = new PercentileApproxFunction("PERCENTILE_APPROX", OperandTypes.ANY_ANY);

    /** Three-arg form: {@code PERCENTILE_APPROX_N(field, percent, centroids)}. */
    static final PercentileApproxFunction INSTANCE_N = new PercentileApproxFunction(
        "PERCENTILE_APPROX_N",
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY)
    );

    private PercentileApproxFunction(String name, SqlOperandTypeChecker operandTypeChecker) {
        super(
            name,
            null,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0.andThen(SqlTypeTransforms.FORCE_NULLABLE),
            null,
            operandTypeChecker,
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false,
            Optionality.FORBIDDEN
        );
    }
}

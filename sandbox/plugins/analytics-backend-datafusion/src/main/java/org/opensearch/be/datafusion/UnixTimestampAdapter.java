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
 * Cat-3a rename adapter for PPL's {@code UNIX_TIMESTAMP(ts)}. Rewrites to a
 * locally-declared {@link SqlFunction} named {@code to_unixtime} — the name
 * DataFusion's substrait consumer recognizes for its native
 * {@code ToUnixtimeFunc} (no UDF registration required on the Rust side).
 *
 * <p>Same machinery as {@link ConvertTzAdapter}: locally-declared operator is
 * the referent of the {@link io.substrait.isthmus.expression.FunctionMappings.Sig}
 * in {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS}.
 *
 * <p><b>Type note.</b> PPL's {@code UNIX_TIMESTAMP} returns
 * {@code DOUBLE_FORCE_NULLABLE}; DataFusion's {@code to_unixtime} returns
 * {@code Int64}. {@link AbstractNameMappingAdapter} preserves the PPL-declared
 * return type on the rewritten call so Calcite's {@code Project.isValid}
 * assertion holds. The downstream substrait consumer (DataFusion) re-resolves
 * {@code to_unixtime} by name and applies its own {@code coerce_types}, so the
 * Calcite-inferred type is purely plan-validity bookkeeping.
 *
 * @opensearch.internal
 */
class UnixTimestampAdapter extends AbstractNameMappingAdapter {

    /**
     * Locally-declared target operator. Name matches DataFusion's native
     * {@code to_unixtime}. Return-type inference is irrelevant — the adapter
     * clones with the original PPL return type.
     */
    static final SqlOperator LOCAL_TO_UNIXTIME_OP = new SqlFunction(
        "to_unixtime",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    UnixTimestampAdapter() {
        super(LOCAL_TO_UNIXTIME_OP, List.of(), List.of());
    }
}

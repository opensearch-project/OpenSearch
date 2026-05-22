/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
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
import org.opensearch.analytics.schema.BinaryType;
import org.opensearch.analytics.schema.IpType;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rewrites {@code CAST}/{@code SAFE_CAST} of an analytics-engine UDT to VARCHAR
 * into a UDF call so DataFusion's native byte-buffer-as-Latin-1 cast kernel is
 * bypassed:
 * <ul>
 *   <li>{@code IpType} &rarr; {@link #IP_TO_STRING_OP}
 *   <li>{@code BinaryType} &rarr; {@link #BINARY_TO_BASE64_OP}
 *   <li>anything else &rarr; {@code original} unchanged
 * </ul>
 *
 * @opensearch.internal
 */
final class IpBinaryCastFunctionAdapter implements ScalarFunctionAdapter {

    /** Bound to Substrait extension {@code "ip_to_string"} (Rust UDF in {@code rust/src/udf/ip_to_string.rs}). */
    static final SqlOperator IP_TO_STRING_OP = new SqlFunction(
        "ip_to_string",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM
    );

    /** Bound to Substrait extension {@code "binary_to_base64"} (Rust UDF in {@code rust/src/udf/binary_to_base64.rs}). */
    static final SqlOperator BINARY_TO_BASE64_OP = new SqlFunction(
        "binary_to_base64",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            return original;
        }
        RexNode source = original.getOperands().get(0);
        RexBuilder rexBuilder = cluster.getRexBuilder();
        if (source.getType() instanceof IpType) {
            return rexBuilder.makeCall(original.getType(), IP_TO_STRING_OP, List.of(source));
        }
        if (source.getType() instanceof BinaryType) {
            return rexBuilder.makeCall(original.getType(), BINARY_TO_BASE64_OP, List.of(source));
        }
        return original;
    }
}

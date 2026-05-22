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
 * Backend-side dispatch for {@code CAST(<IpType|BinaryType> AS VARCHAR)} —
 * including {@code SAFE_CAST}, which is what PPL's {@code cast(... AS STRING)}
 * actually emits via {@code CalciteRexNodeVisitor.visitCast}'s
 * {@code rexBuilder.makeCast(..., true /*matchNullability*&#47;, true /*safe*&#47;)}.
 *
 * <p>Without this rewrite, DataFusion's built-in {@code cast(binary, utf8)}
 * kernel buffer-reinterprets the 16-byte ipv6-mapped buffer (resp. binary
 * payload) as Latin-1, producing garbage strings (or NULL when the bytes
 * aren't valid UTF-8) for queries like {@code | eval s = cast(host as STRING)}.
 *
 * <p>The adapter rewrites VARCHAR-target casts whose source is one of the
 * analytics-engine UDTs into a UDF call:
 * <ul>
 *   <li>{@code IpType} source &rarr; {@link #IP_TO_STRING_OP} call
 *   <li>{@code BinaryType} source &rarr; {@link #BINARY_TO_BASE64_OP} call
 *   <li>everything else &rarr; {@code original} unchanged (plain VARBINARY,
 *       INTEGER, etc. all delegate to DataFusion's native cast kernel as before).
 * </ul>
 *
 * <p>The two emitted UDF calls bind through the analytics-engine's existing
 * Substrait extension catalog (see {@code opensearch_scalar_functions.yaml}).
 * Their Rust implementations live in
 * {@code analytics-backend-datafusion/rust/src/udf/ip_to_string.rs} and
 * {@code binary_to_base64.rs}.
 *
 * <p>Registered for both {@code ScalarFunction.CAST} and
 * {@code ScalarFunction.SAFE_CAST} in {@code DataFusionAnalyticsBackendPlugin};
 * a single stateless instance is shared between the two registrations. The
 * dispatcher ({@code BackendPlanAdapter.adaptRex}) walks every CAST/SAFE_CAST
 * RexCall in the plan tree, so casts at any depth (output Project, inner eval
 * Project, nested in CASE/COALESCE/UDF args) all reach this adapter.
 *
 * @opensearch.internal
 */
final class IpBinaryCastFunctionAdapter implements ScalarFunctionAdapter {

    /**
     * UDF call emitted in place of {@code CAST(<IpType> AS VARCHAR)}. Bound to the
     * Substrait extension {@code "ip_to_string"} via
     * {@link DataFusionFragmentConvertor}'s function mapping table; resolved to
     * the Rust UDF in {@code rust/src/udf/ip_to_string.rs}.
     */
    static final SqlOperator IP_TO_STRING_OP = new SqlFunction(
        "ip_to_string",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM
    );

    /**
     * UDF call emitted in place of {@code CAST(<BinaryType> AS VARCHAR)}. Bound to
     * the Substrait extension {@code "binary_to_base64"} via
     * {@link DataFusionFragmentConvertor}'s function mapping table; resolved to
     * the Rust UDF in {@code rust/src/udf/binary_to_base64.rs}.
     */
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
        // BackendPlanAdapter only dispatches here for ScalarFunction.CAST / SAFE_CAST,
        // so the call's SqlKind is already guaranteed by the framework.
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

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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Normalizes the SQL plugin's PPL IP-comparison UDFs — {@code EQUALS_IP}, {@code NOT_EQUALS_IP},
 * {@code LESS_IP}, {@code GREATER_IP}, {@code LTE_IP}, {@code GTE_IP} — back to their native
 * Calcite comparators ({@link SqlStdOperatorTable#EQUALS} etc.) before Substrait conversion.
 *
 * <p>Why this is needed: {@code ip} (and {@code binary}) fields reach the analytics route typed as
 * VARBINARY, so PPL {@code where ip_field = '1.2.3.4'} resolves to the {@code EQUALS_IP} user-defined
 * function rather than a plain {@code =} (the UDF's {@code (IP_UDT, IP_UDT)} signature accepts a
 * VARBINARY operand). {@code EQUALS_IP} is an SQL-plugin Enumerable-only UDF with no Substrait binding
 * and no backend classpath presence, so isthmus rejects the call with
 * {@code Unable to convert call EQUALS_IP(binary?, binary?)}. Swapping in the native comparator lets
 * DataFusion run its byte-wise VARBINARY comparison — the same path a plain {@code =} takes — after
 * {@link BinaryFunctionAdapter} has resolved the string literal into matching on-disk bytes (adapter
 * recursion in {@code BackendPlanAdapter} runs operands bottom-up, so the literal is already
 * VARBINARY by the time this parent adapter runs).
 *
 * <p>Detection is structural, not by operator identity: the backend cannot reference
 * {@code PPLBuiltinOperators.EQUALS_IP} (it lives in the SQL plugin's {@code core} module, off the
 * analytics backend classpath). A call qualifies when its operator carries a comparison
 * {@link org.apache.calcite.sql.SqlKind} but is <em>not</em> the canonical {@code SqlStdOperatorTable}
 * singleton for that kind, and both operands are VARBINARY. Plain numeric/string/temporal comparisons
 * already use the standard operator, so they pass through untouched.
 *
 * <p>This adapter occupies the six comparison-operator slots that would otherwise hold
 * {@link ComparisonTemporalCoercionAdapter}, so it delegates every call (rewritten or not) to a
 * wrapped temporal-coercion instance to preserve that behavior. IP operands are never temporal, so
 * the delegation is a no-op for the rewritten calls.
 *
 * @opensearch.internal
 */
class IpComparisonNormalizationAdapter implements ScalarFunctionAdapter {

    private final ScalarFunctionAdapter delegate;

    IpComparisonNormalizationAdapter(ScalarFunctionAdapter delegate) {
        this.delegate = delegate;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexCall normalized = normalizeIpComparison(original, cluster);
        return delegate.adapt(normalized, fieldStorage, cluster);
    }

    /** Rewrite an IP-comparison UDF call to its native comparator; return {@code original} otherwise. */
    private static RexCall normalizeIpComparison(RexCall original, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            return original;
        }
        SqlOperator nativeOp = nativeComparator(original.getOperator());
        if (nativeOp == null || original.getOperator() == nativeOp) {
            return original;
        }
        if (!isVarbinary(original.getOperands().get(0)) || !isVarbinary(original.getOperands().get(1))) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        // Preserve the original (BOOLEAN) result type so the plan schema is unchanged.
        return (RexCall) rexBuilder.makeCall(original.getType(), nativeOp, original.getOperands());
    }

    /** The canonical {@code SqlStdOperatorTable} comparator for a comparison {@link org.apache.calcite.sql.SqlKind}, or null. */
    private static SqlOperator nativeComparator(SqlOperator operator) {
        return switch (operator.getKind()) {
            case EQUALS -> SqlStdOperatorTable.EQUALS;
            case NOT_EQUALS -> SqlStdOperatorTable.NOT_EQUALS;
            case LESS_THAN -> SqlStdOperatorTable.LESS_THAN;
            case LESS_THAN_OR_EQUAL -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            case GREATER_THAN -> SqlStdOperatorTable.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            default -> null;
        };
    }

    private static boolean isVarbinary(RexNode node) {
        return node.getType().getSqlTypeName() == SqlTypeName.VARBINARY;
    }
}

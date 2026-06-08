/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Reconciles the i32-vs-i64 return-type disagreement between Calcite and DataFusion for
 * functions like {@code ARRAY_LENGTH}.
 *
 * <p><b>Why:</b> Calcite types {@code array_length} as {@code INTEGER} (i32). DataFusion's
 * UDF actually returns {@code BIGINT} (i64). On 1 shard nobody cross-checks; on 2 shards
 * the cross-fragment schema check sees {@code Int32 ≠ Int64} and fails the query.
 *
 * <p><b>What:</b> rewrite {@code fn(...)} → {@code CAST(fn(...) AS INTEGER)}.
 * <ul>
 *   <li>The inner call's declared return type is forced to {@code BIGINT}, so it matches
 *       what DataFusion actually produces.</li>
 *   <li>The outer cast narrows the i64 result back to i32, so every operator above this
 *       call in the plan still sees the {@code INTEGER}</li>
 * </ul>
 *
 * <p>Applies to {@code INTEGER}/{@code SMALLINT}/{@code TINYINT} returns;
 * {@code BIGINT} and non-integer returns pass through unchanged (re-running on the inner widened call is a safe no-op).
 * @opensearch.internal
 */
public class IntegerReturnWideningCastAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        SqlTypeName originalReturn = original.getType().getSqlTypeName();
        // Only widen integer returns narrower than BIGINT; BIGINT/non-integer returns don't exhibit the i32/i64 mismatch.
        if (originalReturn != SqlTypeName.INTEGER && originalReturn != SqlTypeName.SMALLINT && originalReturn != SqlTypeName.TINYINT) {
            return original;
        }

        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType bigintType = factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.BIGINT),
            original.getType().isNullable()
        );

        // Rebuild with BIGINT return so isthmus binds DataFusion's i64 impl; operands unchanged.
        RexNode widenedCall = cluster.getRexBuilder().makeCall(bigintType, original.getOperator(), original.getOperands());
        // Outer CAST restores the original return type so parent stages see the same schema; inner call produces i64 internally.
        return cluster.getRexBuilder().makeCast(original.getType(), widenedCall);
    }
}

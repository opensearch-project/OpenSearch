/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapter for coalesce(field1, field2, ...),
 * returns the first non-null, non-missing value in the parameter list.
 *
 * <p>Stock {@link SqlStdOperatorTable#COALESCE} requires operands that already share a common type.
 * So naively rewriting the call to stock {@code COALESCE} with the original operands would reject
 * mixed-type inputs like {@code coalesce(VARCHAR, INTEGER, "fallback")} at plan validation.
 *
 * <p>This adapter moves the coercion step to plan time by:
 * <ol>
 *   <li>Computing {@code leastRestrictive} across all operand types</li>
 *   <li>Falling back to nullable VARCHAR when the unification returns {@code null}</li>
 *   <li>Inserting a plain CAST on each operand whose type differs from the common type.
 *       Calcite's CAST covers numeric / boolean / character / date / time / timestamp
 *       / decimal conversions</li>
 *   <li>Rebuilding the call with {@link SqlStdOperatorTable#COALESCE} so isthmus serialises
 *       it through the default Substrait catalog</li>
 * </ol>
 *
 * @opensearch.internal
 */
class CoalesceAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        List<RexNode> operands = original.getOperands();
        if (operands.isEmpty()) {
            return original;
        }

        // leastRestrictive([T]) is T, so single-operand coalesce degenerates to that operand's
        // type and no CAST is needed. leastRestrictive across mixed numeric types returns the
        // widest; across string + numeric returns VARCHAR; across incompatible type families
        // (e.g. DATE + BIGINT) returns null, which we backstop with VARCHAR
        List<RelDataType> operandTypes = operands.stream().map(RexNode::getType).toList();
        RelDataType commonType = typeFactory.leastRestrictive(operandTypes);
        if (commonType == null) {
            commonType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        }

        List<RexNode> coerced = new ArrayList<>(operands.size());
        boolean anyCast = false;
        for (RexNode operand : operands) {
            if (operand.getType().equals(commonType)) {
                coerced.add(operand);
            } else {
                coerced.add(rexBuilder.makeCast(commonType, operand, true, false));
                anyCast = true;
            }
        }
        List<RexNode> finalOperands = anyCast ? coerced : operands;
        return rexBuilder.makeCall(original.getType(), SqlStdOperatorTable.COALESCE, finalOperands);
    }
}

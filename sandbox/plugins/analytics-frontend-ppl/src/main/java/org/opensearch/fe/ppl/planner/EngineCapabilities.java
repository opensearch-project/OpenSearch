/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.ppl.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Declares what the custom engine supports using Calcite's own types.
 *
 * <p>{@code supportedOperators} lists the RelNode classes the engine can handle
 * (e.g., LogicalTableScan, LogicalFilter). {@code supportedFunctions} lists the
 * SqlOperator instances the engine can evaluate (e.g., EQUALS, AND).
 *
 * <p>Push-down rules consult this class before absorbing operators into the
 * boundary node.
 */
public class EngineCapabilities {

    private final Set<Class<? extends RelNode>> supportedOperators;
    private final Set<SqlOperator> supportedFunctions;

    public EngineCapabilities(Set<Class<? extends RelNode>> supportedOperators, Set<SqlOperator> supportedFunctions) {
        this.supportedOperators = Set.copyOf(supportedOperators);
        this.supportedFunctions = Set.copyOf(supportedFunctions);
    }

    /**
     * Static factory with default OpenSearch/DataFusion capabilities:
     * scan, filter, aggregate, and sort operators, plus all standard SQL
     * functions from Calcite's {@link SqlStdOperatorTable}. Additional
     * UDFs can be added via the constructor.
     */
    public static EngineCapabilities defaultCapabilities() {
        // TODO: Maybe build this dynamically from back-end capabilities, for now static list.
        return new EngineCapabilities(
            Set.of(LogicalTableScan.class, LogicalFilter.class, LogicalAggregate.class, LogicalSort.class),
            new HashSet<>(SqlStdOperatorTable.instance().getOperatorList())
        );
    }

    /**
     * Checks whether the engine supports the given RelNode's operator type.
     */
    public boolean supportsOperator(RelNode node) {
        return supportedOperators.contains(node.getClass());
    }

    /**
     * Walks the expression tree and checks that every SqlOperator found
     * in RexCall nodes is in the supported set. Returns true only if all
     * functions are supported.
     */
    public boolean supportsAllFunctions(RexNode expression) {
        if (expression == null) {
            return true;
        }
        Boolean result = expression.accept(new FunctionSupportVisitor());
        // null means the expression is a leaf node with no SqlOperator to check
        return result == null || result;
    }

    /**
     * RexVisitor that returns false as soon as an unsupported SqlOperator is found.
     */
    private class FunctionSupportVisitor extends RexVisitorImpl<Boolean> {

        FunctionSupportVisitor() {
            super(true);
        }

        @Override
        public Boolean visitCall(RexCall call) {
            if (!supportedFunctions.contains(call.getOperator())) {
                return false;
            }
            for (RexNode operand : call.getOperands()) {
                Boolean childResult = operand.accept(this);
                // null means the operand is a leaf node (RexInputRef, RexLiteral, etc.)
                // which contains no SqlOperator to check — treat as supported.
                if (childResult != null && !childResult) {
                    return false;
                }
            }
            return true;
        }

    }

    /**
     * Checks that every aggregate function in the call list is in the supported set.
     * Returns true if the list is null/empty or all aggregate functions are supported.
     */
    public boolean supportsAllAggFunctions(List<AggregateCall> aggCalls) {
        if (aggCalls == null || aggCalls.isEmpty()) {
            return true;
        }
        for (AggregateCall aggCall : aggCalls) {
            if (!supportedFunctions.contains(aggCall.getAggregation())) {
                return false;
            }
        }
        return true;
    }
}

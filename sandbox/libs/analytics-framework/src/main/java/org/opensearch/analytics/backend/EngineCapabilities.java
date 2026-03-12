/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

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
 */
public class EngineCapabilities {

    private final Set<Class<? extends RelNode>> supportedOperators;
    private final Set<SqlOperator> supportedFunctions;

    /**
     * Creates capabilities from explicit operator and function sets.
     *
     * @param supportedOperators relational operator classes the engine can execute
     * @param supportedFunctions scalar and aggregate functions the engine supports
     */
    public EngineCapabilities(Set<Class<? extends RelNode>> supportedOperators, Set<SqlOperator> supportedFunctions) {
        this.supportedOperators = Set.copyOf(supportedOperators);
        this.supportedFunctions = Set.copyOf(supportedFunctions);
    }

    /** Returns capabilities covering standard Calcite logical operators and all built-in functions. */
    public static EngineCapabilities defaultCapabilities() {
        return new EngineCapabilities(
            Set.of(LogicalTableScan.class, LogicalFilter.class, LogicalAggregate.class, LogicalSort.class),
            new HashSet<>(SqlStdOperatorTable.instance().getOperatorList())
        );
    }

    /**
     * Returns {@code true} if the engine can execute the given relational operator.
     *
     * @param node the relational operator to check
     */
    public boolean supportsOperator(RelNode node) {
        return supportedOperators.contains(node.getClass());
    }

    /**
     * Returns {@code true} if every scalar function in the expression tree is supported.
     *
     * @param expression the row expression tree to check
     */
    public boolean supportsAllFunctions(RexNode expression) {
        if (expression == null) {
            return true;
        }
        Boolean result = expression.accept(new FunctionSupportVisitor());
        return result == null || result;
    }

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
                if (childResult != null && !childResult) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Returns {@code true} if every aggregate function in the list is supported.
     *
     * @param aggCalls the aggregate calls to check
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

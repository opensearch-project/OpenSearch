/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

/**
 * Rewrites a {@link RexCall} whose operator is a PPL / library-specific UDF to the equivalent
 * {@link org.apache.calcite.sql.fun.SqlStdOperatorTable SqlStdOperatorTable} operator. Used to
 * normalize PPL emissions so Isthmus's built-in {@code FunctionMappings.SCALAR_SIGS} can resolve
 * them to the Substrait default extension catalog.
 *
 * <p>Examples:
 * <ul>
 *   <li>PPL's {@code DIVIDE} UDF ({@code PPLBuiltinOperators.DIVIDE}, a {@code SqlFunction}
 *       named "DIVIDE") → {@code SqlStdOperatorTable.DIVIDE} → substrait {@code divide}.</li>
 *   <li>PPL's {@code MOD} UDF → {@code SqlStdOperatorTable.MOD} → substrait {@code modulus}.</li>
 * </ul>
 *
 * <p>Adapter-level rewriting (rather than extending Isthmus's {@code ADDITIONAL_SCALAR_SIGS})
 * keeps the rewrite scoped to a single backend registration and avoids cross-cutting changes
 * to Isthmus. The rewrite preserves operand order and result type.
 *
 * @opensearch.internal
 */
public class StdOperatorRewriteAdapter implements ScalarFunctionAdapter {

    /** Canonical Calcite operator this adapter substitutes in. */
    private final SqlOperator target;

    /**
     * Operator name we expect to rewrite. Matching on name (case-insensitive) guards against
     * applying the rewrite when the call already uses the target operator — an adapter is
     * keyed by {@link org.opensearch.analytics.spi.ScalarFunction} which can map to either
     * the PPL UDF or the std operator depending on how the call was constructed upstream.
     */
    private final String expectedName;

    /**
     * @param expectedName case-insensitive match against {@code call.getOperator().getName()};
     *                     if the call already uses {@code target}, the rewrite is a no-op.
     * @param target       the {@code SqlStdOperatorTable} (or other Isthmus-mapped) operator
     *                     to substitute in.
     */
    public StdOperatorRewriteAdapter(String expectedName, SqlOperator target) {
        this.expectedName = expectedName;
        this.target = target;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        // Already the target operator — e.g. PLUS arrived via SqlStdOperatorTable.PLUS. No-op.
        if (original.getOperator() == target) {
            return original;
        }
        String actualName = original.getOperator().getName();
        if (actualName == null || !actualName.equalsIgnoreCase(expectedName)) {
            return original;
        }
        // Re-construct with the standard operator, preserving operands and result type.
        return cluster.getRexBuilder().makeCall(original.getType(), target, original.getOperands());
    }
}

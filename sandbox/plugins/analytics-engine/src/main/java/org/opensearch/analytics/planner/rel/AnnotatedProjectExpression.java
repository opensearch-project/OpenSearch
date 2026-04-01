/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.ReturnTypes;

import java.util.List;

/**
 * Wraps an opaque project expression (painless, highlight, etc.) with the
 * backend that will evaluate it. Always a single backend — project expressions
 * are not split across viable backends like filter predicates.
 *
 * <p>During fragment conversion this becomes a {@code DelegatedBackendPlan}
 * if the delegation backend differs from the operator's primary backend.
 *
 * @opensearch.internal
 */
public class AnnotatedProjectExpression extends RexCall {

    private static final SqlOperator ANNOTATED_PROJECT_EXPR_OP = new SqlOperator(
        "ANNOTATED_PROJECT_EXPR",
        SqlKind.OTHER_FUNCTION,
        0,
        0,
        ReturnTypes.ARG0,
        null,
        null
    ) {
        @Override
        public SqlSyntax getSyntax() {
            return SqlSyntax.FUNCTION;
        }
    };

    private final RexNode original;
    private final String backend;

    public AnnotatedProjectExpression(RelDataType type, RexNode original, String backend) {
        super(type, ANNOTATED_PROJECT_EXPR_OP, List.of(original));
        this.original = original;
        this.backend = backend;
    }

    public RexNode getOriginal() {
        return original;
    }

    /** The backend that evaluates this expression. */
    public String getBackend() {
        return backend;
    }

    @Override
    protected String computeDigest(boolean withType) {
        return "ANNOTATED_PROJECT_EXPR(backend=" + backend + ", " + original + ")";
    }
}

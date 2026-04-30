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
 * Custom RexNode wrapping an original predicate with backend routing metadata.
 * During planning: carries which backends can evaluate this predicate.
 * During fragment conversion: becomes a DelegatedPredicate if delegation is chosen.
 *
 * @opensearch.internal
 */
public class AnnotatedPredicate extends RexCall implements OperatorAnnotation {

    private static final SqlOperator ANNOTATED_PREDICATE_OP = new SqlOperator(
        "ANNOTATED_PREDICATE",
        SqlKind.OTHER_FUNCTION,
        0,
        0,
        ReturnTypes.BOOLEAN,
        null,
        null
    ) {
        @Override
        public SqlSyntax getSyntax() {
            return SqlSyntax.FUNCTION;
        }
    };

    private final RexNode original;
    private final List<String> viableBackends;
    private final int annotationId;

    public AnnotatedPredicate(RelDataType type, RexNode original, List<String> viableBackends, int annotationId) {
        super(type, ANNOTATED_PREDICATE_OP, List.of(original));
        this.original = original;
        this.viableBackends = viableBackends;
        this.annotationId = annotationId;
    }

    public RexNode getOriginal() {
        return original;
    }

    /** Backends that can evaluate this predicate. */
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public int getAnnotationId() {
        return annotationId;
    }

    @Override
    public OperatorAnnotation narrowTo(String backend) {
        return new AnnotatedPredicate(type, original, List.of(backend), annotationId);
    }

    @Override
    public RexNode unwrap() {
        return original;
    }

    @Override
    public RexNode withAdaptedOriginal(RexNode adaptedOriginal) {
        return new AnnotatedPredicate(type, adaptedOriginal, viableBackends, annotationId);
    }

    @Override
    protected String computeDigest(boolean withType) {
        return "ANNOTATED_PREDICATE(id=" + annotationId + ", backends=" + viableBackends + ", " + original + ")";
    }
}

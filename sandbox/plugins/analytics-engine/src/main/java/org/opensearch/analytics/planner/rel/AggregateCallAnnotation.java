/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Per-call backend routing metadata stored on {@link OpenSearchAggregate} in a
 * side map keyed by aggregate-call index. Same role as {@link AnnotatedPredicate}
 * for filter predicates, but lives outside the {@link org.apache.calcite.rel.core.AggregateCall}
 * itself — embedding it in {@code rexList} would expose its type as a "preOperand"
 * to Calcite's {@code AggCallBinding}, shifting positional argument inference for
 * aggregates whose return-type inference reads {@code argTypes.getFirst()}
 * (TAKE, FIRST, LAST, etc.).
 *
 * @opensearch.internal
 */
public final class AggregateCallAnnotation implements OperatorAnnotation {

    private final List<String> viableBackends;
    private final int annotationId;

    public AggregateCallAnnotation(List<String> viableBackends, int annotationId) {
        this.viableBackends = viableBackends;
        this.annotationId = annotationId;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public int getAnnotationId() {
        return annotationId;
    }

    @Override
    public OperatorAnnotation narrowTo(String backend) {
        return new AggregateCallAnnotation(List.of(backend), annotationId);
    }

    @Override
    public RexNode unwrap() {
        // Not embedded in any RexNode tree — there's nothing to unwrap.
        return null;
    }

    @Override
    public RexNode withAdaptedOriginal(RexNode adaptedOriginal) {
        // AggregateCallAnnotation is a marker, not a wrapper — adaptation does not apply.
        return null;
    }
}

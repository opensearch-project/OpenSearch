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
 * Per-call backend routing metadata for an {@link org.apache.calcite.rel.core.AggregateCall}.
 * Stored on {@link OpenSearchAggregate} as a side-map keyed by call index — keeping it out
 * of {@code AggregateCall#rexList} avoids contaminating Calcite's {@code AggCallBinding}
 * preOperands, which would corrupt return-type inference for PPL's {@code ARG0_ARRAY}.
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
        // Marker — stripping handled out-of-band by OpenSearchAggregate#stripAnnotations.
        return null;
    }

    @Override
    public RexNode withAdaptedOriginal(RexNode adaptedOriginal) {
        return null;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.ReturnTypes;

import java.util.ArrayList;
import java.util.List;

/**
 * Marker {@link RexNode} embedded in an {@link AggregateCall#rexList} to carry
 * per-call backend routing metadata. Same pattern as {@link AnnotatedPredicate}
 * for filter predicates.
 *
 * <p>The aggregate rule appends this to each call's rexList via {@link #annotate}.
 * During fragment conversion it is stripped out.
 * {@code BackendResolver.generateCandidatePlans()} reads it for StagePlan
 * alternative generation.
 *
 * @opensearch.internal
 */
public class AggregateCallAnnotation extends RexCall implements OperatorAnnotation {

    private static final SqlOperator AGG_CALL_ANNOTATION_OP = new SqlOperator(
        "AGG_CALL_ANNOTATION",
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

    private final List<String> viableBackends;
    private final int annotationId;

    private AggregateCallAnnotation(RelDataType type, List<String> viableBackends, int annotationId) {
        super(type, AGG_CALL_ANNOTATION_OP, List.of());
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
        return new AggregateCallAnnotation(type, List.of(backend), annotationId);
    }

    @Override
    public RexNode unwrap() {
        // AggregateCallAnnotation is a marker in rexList, not a wrapper around an expression.
        // Unwrapping means removing it from the rexList — handled by the operator's stripAnnotations.
        return null;
    }

    @Override
    public RexNode withAdaptedOriginal(RexNode adaptedOriginal) {
        // AggregateCallAnnotation is a marker, not a wrapper — adaptation does not apply.
        return this;
    }

    /** Extracts the annotation from an AggregateCall's rexList, or null if absent.
     *
     * <p>TODO: window function aggregate calls may have ORDER BY expressions in rexList
     * alongside our annotation. find() is safe (searches by type) and stripAnnotations
     * filters by type, but consider moving annotations to a separate
     * {@code Map<Integer, AggregateCallAnnotation>} on {@link OpenSearchAggregate} keyed
     * by call index to decouple from rexList entirely.
     */
    public static AggregateCallAnnotation find(AggregateCall call) {
        for (RexNode rex : call.rexList) {
            if (rex instanceof AggregateCallAnnotation annotation) {
                return annotation;
            }
        }
        return null;
    }

    /** Creates a new AggregateCall with this annotation appended to its rexList. */
    public static AggregateCall annotate(AggregateCall call, List<String> viableBackends, int annotationId) {
        List<RexNode> newRexList = new ArrayList<>(call.rexList);
        newRexList.add(new AggregateCallAnnotation(call.type, viableBackends, annotationId));
        return AggregateCall.create(
            call.getAggregation(),
            call.isDistinct(),
            call.isApproximate(),
            call.ignoreNulls(),
            newRexList,
            call.getArgList(),
            call.filterArg,
            call.distinctKeys,
            call.collation,
            call.type,
            call.name
        );
    }

    @Override
    protected String computeDigest(boolean withType) {
        return "AGG_CALL_ANNOTATION(id=" + annotationId + ", viableBackends=" + viableBackends + ")";
    }
}

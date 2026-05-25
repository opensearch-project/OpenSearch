/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;

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
    /**
     * Peer backends that could have evaluated this predicate but lost the narrow.
     * Empty when the predicate is single-viable (no peer to consult) or hasn't
     * been narrowed yet. Non-empty when the predicate was dual-viable AND was
     * narrowed onto one of its viable backends — the listed backends are valid
     * peers for opportunistic consultation at runtime.
     *
     * <p>FragmentConversion uses this list (a) to detect performance-delegation
     * candidates ({@code !isEmpty()}) and (b) to pick the peer to serialize the
     * predicate for. The driving backend wraps with
     * {@code delegation_possible(original, id)} so the peer may be consulted
     * per-RG when the driving backend's own pruning isn't selective enough.
     *
     * <p>NOTE: temp workaround — derived in {@link #narrowTo(String)} from the
     * original viableBackends. Cleaner long-term shape is a typed Resolver
     * returning the decision. Needs revisiting.
     */
    private final List<String> performanceDelegationBackends;

    public AnnotatedPredicate(RelDataType type, RexNode original, List<String> viableBackends, int annotationId) {
        this(type, original, viableBackends, annotationId, List.of());
    }

    private AnnotatedPredicate(
        RelDataType type,
        RexNode original,
        List<String> viableBackends,
        int annotationId,
        List<String> performanceDelegationBackends
    ) {
        super(type, ANNOTATED_PREDICATE_OP, List.of(original));
        this.original = original;
        this.viableBackends = viableBackends;
        this.annotationId = annotationId;
        this.performanceDelegationBackends = performanceDelegationBackends;
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

    /**
     * Peer backends valid for opportunistic consultation. Empty unless this predicate
     * was dual-viable and was narrowed onto one of its viable backends.
     */
    public List<String> getPerformanceDelegationBackends() {
        return performanceDelegationBackends;
    }

    @Override
    public OperatorAnnotation narrowTo(String backend) {
        List<String> peers = (viableBackends.size() > 1 && viableBackends.contains(backend))
            ? viableBackends.stream().filter(b -> !b.equals(backend)).toList()
            : List.of();
        return new AnnotatedPredicate(type, original, List.of(backend), annotationId, peers);
    }

    @Override
    public RexNode unwrap() {
        return original;
    }

    @Override
    public RexNode withAdaptedOriginal(RexNode adaptedOriginal) {
        return new AnnotatedPredicate(type, adaptedOriginal, viableBackends, annotationId, performanceDelegationBackends);
    }

    @Override
    public RexNode makePlaceholder(RexBuilder rexBuilder) {
        return DelegatedPredicateFunction.makeCall(rexBuilder, annotationId);
    }

    @Override
    protected String computeDigest(boolean withType) {
        return "ANNOTATED_PREDICATE(id="
            + annotationId
            + ", backends="
            + viableBackends
            + (performanceDelegationBackends.isEmpty() ? "" : ", peers=" + performanceDelegationBackends)
            + ", "
            + original
            + ")";
    }
}

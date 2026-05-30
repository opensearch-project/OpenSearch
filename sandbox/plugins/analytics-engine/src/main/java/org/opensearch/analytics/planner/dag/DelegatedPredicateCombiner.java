/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegatedSubtreeConvertor;
import org.opensearch.analytics.spi.DelegationPossibleFunction;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Bottom-up classifier for filter condition trees. Classifies each node as
 * delegated (targets a non-operator backend) or resolved (native), combining
 * same-backend delegated siblings under AND/OR/NOT into a single serialized
 * expression via the backend's {@link DelegatedSubtreeConvertor}.
 *
 * @opensearch.internal
 */
final class DelegatedPredicateCombiner {

    private final String operatorBackend;
    private final List<FieldStorageInfo> fieldStorage;
    private final CapabilityRegistry registry;
    private final RexBuilder rexBuilder;
    private final List<DelegatedExpression> delegatedExpressions;
    /**
     * When true, performance-delegated leaves stay in the delegation pool under OR/NOT
     * (instead of being thrown back to native). Lets the entire boolean structure ship to
     * the peer backend as one delegated expression. See
     * {@code AnalyticsPlugin.DELEGATION_FUSE_DUAL_VIABLE} for the tradeoff.
     */
    private final boolean fuseDualViable;

    DelegatedPredicateCombiner(
        String operatorBackend,
        List<FieldStorageInfo> fieldStorage,
        CapabilityRegistry registry,
        RexBuilder rexBuilder,
        List<DelegatedExpression> delegatedExpressions
    ) {
        this(operatorBackend, fieldStorage, registry, rexBuilder, delegatedExpressions, false);
    }

    DelegatedPredicateCombiner(
        String operatorBackend,
        List<FieldStorageInfo> fieldStorage,
        CapabilityRegistry registry,
        RexBuilder rexBuilder,
        List<DelegatedExpression> delegatedExpressions,
        boolean fuseDualViable
    ) {
        this.operatorBackend = operatorBackend;
        this.fieldStorage = fieldStorage;
        this.registry = registry;
        this.rexBuilder = rexBuilder;
        this.delegatedExpressions = delegatedExpressions;
        this.fuseDualViable = fuseDualViable;
    }

    /** Bottom-up: classify each node as Delegated (carries the RexNode subtree) or Resolved. */
    Classified classify(RexNode node, Function<OperatorAnnotation, RexNode> applyFn) {
        if (node instanceof AnnotatedPredicate ap) {
            String backend = ap.getViableBackends().getFirst();
            if (!backend.equals(operatorBackend) && canSerialize(ap, backend)) {
                return new Delegated(backend, node, ap.getAnnotationId(), false);
            } else if (!ap.getPerformanceDelegationBackends().isEmpty()) {
                String peerBackend = ap.getPerformanceDelegationBackends().getFirst();
                if (canSerialize(ap, peerBackend)) {
                    return new Delegated(peerBackend, node, ap.getAnnotationId(), true);
                }
            }
            return new Resolved(applyFn.apply(ap));
        }

        if (node instanceof RexCall call) {
            SqlKind kind = call.getKind();
            if (kind != SqlKind.AND && kind != SqlKind.OR && kind != SqlKind.NOT) {
                return new Resolved(resolveCallChildren(call, applyFn));
            }

            List<Classified> kids = new ArrayList<>(call.getOperands().size());
            for (RexNode operand : call.getOperands()) {
                kids.add(classify(operand, applyFn));
            }
            return combine(call, kids, applyFn);
        }

        return new Resolved(node);
    }

    /** Combines classified children of an AND/OR/NOT call into a single Classified result. */
    private Classified combine(RexCall call, List<Classified> kids, Function<OperatorAnnotation, RexNode> applyFn) {
        boolean isOrNot = call.getKind() == SqlKind.OR || call.getKind() == SqlKind.NOT;

        List<Delegated> correctnessChildren = new ArrayList<>();
        List<Delegated> performanceChildren = new ArrayList<>();
        List<Object> ordered = new ArrayList<>();
        String commonBackend = null;
        boolean multiBackend = false;

        for (Classified c : kids) {
            if (c instanceof Delegated d) {
                // Under OR/NOT, performance-delegated leaves are thrown back to native by
                // default — the {@code delegation_possible} pattern doesn't compose with
                // disjunction (the driver can't tell "leaf didn't match" from "no leaf matched"
                // when the peer misses). When {@code fuseDualViable} is on the carve-out is
                // skipped so the entire boolean ships to the peer as one delegated expression.
                if (isOrNot && d.performanceDelegation() && !fuseDualViable) {
                    ordered.add(applyFn.apply((AnnotatedPredicate) d.subtree()));
                } else {
                    (d.performanceDelegation() ? performanceChildren : correctnessChildren).add(d);
                    ordered.add(d);
                    if (commonBackend == null) commonBackend = d.backend();
                    else if (!commonBackend.equals(d.backend())) multiBackend = true;
                }
            } else {
                ordered.add(((Resolved) c).node());
            }
        }

        if ((correctnessChildren.isEmpty() && performanceChildren.isEmpty()) || multiBackend) {
            return new Resolved(materializeIndividually(call, ordered));
        }

        DelegatedSubtreeConvertor convertor = registry.getBackend(commonBackend).getDelegatedSubtreeConvertor();
        if (convertor == null) {
            return new Resolved(materializeIndividually(call, ordered));
        }

        if (ordered.size() == correctnessChildren.size() && performanceChildren.isEmpty()) {
            // All children are correctness-delegated to the same backend — bubble up
            int firstId = correctnessChildren.getFirst().firstAnnotationId();
            return new Delegated(commonBackend, call, firstId, false);
        }

        if (ordered.size() == performanceChildren.size() && correctnessChildren.isEmpty()) {
            // All children are performance-delegated to the same backend — bubble up
            int firstId = performanceChildren.getFirst().firstAnnotationId();
            return new Delegated(commonBackend, call, firstId, true);
        }

        // Mixed: combine correctness-delegated into one expression, performance-delegated into another.
        byte[] correctnessCombined = null;
        int correctnessFirstId = 0;
        if (!correctnessChildren.isEmpty()) {
            correctnessFirstId = correctnessChildren.getFirst().firstAnnotationId();
            RexNode correctnessSubtree = buildCombinedSubtree(call, correctnessChildren);
            correctnessCombined = convertor.convertSubtree(correctnessSubtree, fieldStorage);
        }

        byte[] performanceCombined = null;
        int performanceFirstId = 0;
        if (!performanceChildren.isEmpty()) {
            performanceFirstId = performanceChildren.getFirst().firstAnnotationId();
            RexNode performanceSubtree = buildCombinedSubtree(call, performanceChildren);
            performanceCombined = convertor.convertSubtree(performanceSubtree, fieldStorage);
        }

        return new Resolved(
            materializeCombined(
                call,
                ordered,
                correctnessFirstId,
                commonBackend,
                correctnessCombined,
                performanceFirstId,
                performanceCombined
            )
        );
    }

    private RexNode materializeIndividually(RexCall call, List<Object> ordered) {
        List<RexNode> newOperands = new ArrayList<>();
        for (Object item : ordered) {
            if (item instanceof Delegated d) {
                byte[] bytes = convertSingleDelegated(d);
                delegatedExpressions.add(new DelegatedExpression(d.firstAnnotationId(), d.backend(), bytes));
                newOperands.add(makePlaceholder(d));
            } else {
                newOperands.add((RexNode) item);
            }
        }
        return call.clone(call.getType(), newOperands);
    }

    private RexNode materializeCombined(
        RexCall call,
        List<Object> ordered,
        int correctnessFirstId,
        String backend,
        byte[] correctnessCombined,
        int perfFirstId,
        byte[] perfCombined
    ) {
        List<RexNode> newOperands = new ArrayList<>();
        if (correctnessCombined != null) {
            delegatedExpressions.add(new DelegatedExpression(correctnessFirstId, backend, correctnessCombined));
            newOperands.add(DelegatedPredicateFunction.makeCall(rexBuilder, correctnessFirstId));
        }
        if (perfCombined != null) {
            delegatedExpressions.add(new DelegatedExpression(perfFirstId, backend, perfCombined));
            List<RexNode> unwrapped = ordered.stream()
                .filter(o -> o instanceof Delegated d && d.performanceDelegation())
                .map(o -> ((AnnotatedPredicate) ((Delegated) o).subtree()).unwrap())
                .toList();
            RexNode perfOriginal = unwrapped.size() == 1 ? unwrapped.getFirst() : call.clone(call.getType(), unwrapped);
            newOperands.add(DelegationPossibleFunction.makeCall(rexBuilder, perfOriginal, perfFirstId));
        }
        for (Object item : ordered) {
            if (item instanceof Delegated == false) {
                newOperands.add((RexNode) item);
            }
        }
        return call.clone(call.getType(), newOperands);
    }

    /** Converts a single Delegated node (leaf or subtree) into bytes via the backend's convertor. */
    private byte[] convertSingleDelegated(Delegated d) {
        DelegatedSubtreeConvertor convertor = registry.getBackend(d.backend()).getDelegatedSubtreeConvertor();
        if (convertor == null) {
            throw new IllegalStateException("Backend [" + d.backend() + "] declares delegation but has no DelegatedSubtreeConvertor");
        }
        return convertor.convertSubtree(d.subtree(), fieldStorage);
    }

    /** Finalizes a Delegated result: converts to bytes, emits DelegatedExpression, returns placeholder. */
    RexNode finalizeDelegated(Delegated d) {
        byte[] bytes = convertSingleDelegated(d);
        delegatedExpressions.add(new DelegatedExpression(d.firstAnnotationId(), d.backend(), bytes));
        return makePlaceholder(d);
    }

    /**
     * Returns the appropriate placeholder for a delegated predicate:
     * - {@link DelegatedPredicateFunction} for correctness-delegation (driving backend cannot evaluate)
     * - {@link DelegationPossibleFunction} for performance-delegation (driving backend can evaluate natively,
     *   peer consulted opportunistically)
     *
     * <p>{@link Delegated#subtree()} is either a single {@link AnnotatedPredicate} (leaf
     * case) or a raw {@code RexCall} that bubbled up an AND/OR/NOT of same-backend children
     * (the bubble branches in {@link #classify}). The driving backend evaluates the unwrapped
     * expression natively, so every {@code AnnotatedPredicate} marker — leaf or under a
     * bubbled AND/OR — has to come off before the placeholder is built.
     */
    RexNode makePlaceholder(Delegated d) {
        if (d.performanceDelegation()) {
            RexNode original = unwrapAnnotations(d.subtree());
            return DelegationPossibleFunction.makeCall(rexBuilder, original, d.firstAnnotationId());
        }
        return DelegatedPredicateFunction.makeCall(rexBuilder, d.firstAnnotationId());
    }

    /**
     * Recursively strips {@link AnnotatedPredicate} wrappers from a RexNode tree. AND/OR/NOT
     * are reconstructed with unwrapped operands; bare RexCalls that aren't AnnotatedPredicates
     * pass through. Without this the driving backend's Substrait converter (e.g. Isthmus)
     * trips on the unknown {@code ANNOTATED_PREDICATE} SqlOperator.
     */
    private RexNode unwrapAnnotations(RexNode node) {
        if (node instanceof AnnotatedPredicate ap) {
            return ap.unwrap();
        }
        if (node instanceof RexCall call) {
            List<RexNode> unwrapped = new ArrayList<>(call.getOperands().size());
            boolean changed = false;
            for (RexNode op : call.getOperands()) {
                RexNode u = unwrapAnnotations(op);
                if (u != op) changed = true;
                unwrapped.add(u);
            }
            return changed ? call.clone(call.getType(), unwrapped) : call;
        }
        return node;
    }

    /** Checks if the peer backend has a serializer for this predicate's function. */
    private boolean canSerialize(AnnotatedPredicate ap, String peerBackend) {
        RexNode original = ap.unwrap();
        if (original instanceof RexCall originalCall) {
            ScalarFunction fn = ScalarFunction.fromSqlOperatorWithFallback(originalCall.getOperator());
            return fn != null && registry.getBackend(peerBackend).delegatedPredicateSerializers().containsKey(fn);
        }
        return false;
    }

    /**
     * Builds a RexNode subtree from the delegated children of a mixed AND/OR/NOT call,
     * preserving the boolean operator. Only includes the delegated operands.
     */
    private RexNode buildCombinedSubtree(RexCall call, List<Delegated> delegatedChildren) {
        if (delegatedChildren.size() == 1) {
            return delegatedChildren.getFirst().subtree();
        }
        List<RexNode> subtrees = delegatedChildren.stream().map(Delegated::subtree).map(n -> (RexNode) n).toList();
        return call.clone(call.getType(), subtrees);
    }

    private RexNode resolveCallChildren(RexCall call, Function<OperatorAnnotation, RexNode> applyFn) {
        List<RexNode> newOperands = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            Classified c = classify(operand, applyFn);
            if (c instanceof Delegated d) {
                byte[] bytes = convertSingleDelegated(d);
                delegatedExpressions.add(new DelegatedExpression(d.firstAnnotationId(), d.backend(), bytes));
                newOperands.add(makePlaceholder(d));
            } else {
                newOperands.add(((Resolved) c).node());
            }
        }
        return call.clone(call.getType(), newOperands);
    }

    /** Tagged result from bottom-up classification. */
    interface Classified {}

    record Delegated(String backend, RexNode subtree, int firstAnnotationId, boolean performanceDelegation) implements Classified {
    }

    record Resolved(RexNode node) implements Classified {
    }
}

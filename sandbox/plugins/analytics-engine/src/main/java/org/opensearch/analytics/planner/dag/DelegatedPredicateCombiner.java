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
     * When true, dual-viable leaves are classified as correctness-delegated everywhere —
     * the {@code delegation_possible} marker (driver-evaluates-natively + opportunistic peer
     * consult) is not emitted under fuse=true. Result: one merged {@code delegated_predicate}
     * ships the whole eligible subtree to the peer; the driver doesn't evaluate any
     * delegatable arm itself. Trade-off: AND-side opportunistic optimization gone in
     * exchange for cross-bucket merging under OR/NOT with native siblings.
     */
    private final boolean fuseDualViable;

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
                    // Under fuseDualViable, demote perf to correctness so the leaf merges
                    // freely with correctness siblings and ships entirely to the peer.
                    return new Delegated(peerBackend, node, ap.getAnnotationId(), !fuseDualViable);
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
                // Under OR/NOT with fuse=false, perf-delegated leaves carve back to native:
                // delegation_possible doesn't compose under disjunction (driver can't tell
                // "leaf didn't match" from "no leaf matched when the peer missed"). Under
                // fuse=true, classify() never emits perf, so this branch never fires.
                if ((isOrNot && !fuseDualViable) && d.performanceDelegation()) {
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
            // All children are correctness-delegated to the same backend — bubble up.
            int firstId = correctnessChildren.getFirst().firstAnnotationId();
            return new Delegated(commonBackend, call, firstId, false);
        }

        if (ordered.size() == performanceChildren.size() && correctnessChildren.isEmpty()) {
            // All children are performance-delegated to the same backend — bubble up so the
            // parent can merge further up the tree before the Mixed branch flattens. Only
            // reachable under fuse=false + AND (OR/NOT carved perf out above; fuse=true
            // demotes perf to correctness so this branch never fires under fuse=true).
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
     * <ul>
     *   <li>{@link DelegatedPredicateFunction} for correctness-delegation (driving backend
     *       cannot evaluate; peer runs the predicate)</li>
     *   <li>{@link DelegationPossibleFunction} for performance-delegation (driving backend
     *       evaluates natively, peer consulted opportunistically per-RG)</li>
     * </ul>
     *
     * <p>The performance-delegation branch only sees a leaf {@link AnnotatedPredicate} —
     * perf children are never bubbled up into a multi-leaf subtree (that would lose the
     * per-leaf semantic of {@code delegation_possible}). Correctness-delegation can carry a
     * bubbled-up subtree, but the placeholder only references the annotation id, not the
     * subtree contents.
     */
    RexNode makePlaceholder(Delegated d) {
        if (d.performanceDelegation()) {
            RexNode original = ((AnnotatedPredicate) d.subtree()).unwrap();
            return DelegationPossibleFunction.makeCall(rexBuilder, original, d.firstAnnotationId());
        }
        return DelegatedPredicateFunction.makeCall(rexBuilder, d.firstAnnotationId());
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
        List<RexNode> subtrees = delegatedChildren.stream().<RexNode>map(Delegated::subtree).toList();
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

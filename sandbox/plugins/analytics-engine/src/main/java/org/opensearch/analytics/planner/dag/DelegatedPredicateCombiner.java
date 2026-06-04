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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private static final Logger LOGGER = LogManager.getLogger(DelegatedPredicateCombiner.class);

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
                    boolean isPerf = !fuseDualViable;
                    return new Delegated(peerBackend, node, ap.getAnnotationId(), isPerf);
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
                    ordered.add(unwrapPreservingConnectors(d.subtree(), applyFn::apply));
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

        Classified result;
        String outcome;
        DelegatedSubtreeConvertor convertor = commonBackend == null
            ? null
            : registry.getBackend(commonBackend).getDelegatedSubtreeConvertor();

        if ((correctnessChildren.isEmpty() && performanceChildren.isEmpty()) || multiBackend) {
            outcome = "INDIVIDUAL";
            result = new Resolved(materializeIndividually(call, ordered));
        } else if (convertor == null) {
            outcome = "INDIVIDUAL (no convertor)";
            result = new Resolved(materializeIndividually(call, ordered));
        } else if (ordered.size() == correctnessChildren.size() && performanceChildren.isEmpty()) {
            // All children correctness-delegated to the same backend — bubble up.
            outcome = "BUBBLE_UP CORRECTNESS";
            int firstId = correctnessChildren.getFirst().firstAnnotationId();
            result = new Delegated(commonBackend, call, firstId, false);
        } else if (ordered.size() == performanceChildren.size() && correctnessChildren.isEmpty()) {
            // All children performance-delegated to the same backend — bubble up so the
            // parent can merge further up the tree before the Mixed branch flattens. Only
            // reachable under fuse=false + AND (OR/NOT carved perf out above; fuse=true
            // demotes perf to correctness so this branch never fires under fuse=true).
            outcome = "BUBBLE_UP PERF";
            int firstId = performanceChildren.getFirst().firstAnnotationId();
            result = new Delegated(commonBackend, call, firstId, true);
        } else {
            outcome = "MIXED";
            // Mixed: combine correctness-delegated into one expression, performance into another.
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
            result = new Resolved(
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

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "combine kind={} fuse={} kids={} (correctness={}, perf={}, multiBackend={}) → {}{}",
                call.getKind(),
                fuseDualViable,
                kids.size(),
                correctnessChildren.size(),
                performanceChildren.size(),
                multiBackend,
                outcome,
                commonBackend != null ? " backend=[" + commonBackend + "]" : ""
            );
        }
        return result;
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
                .map(o -> unwrapPreservingConnectors(((Delegated) o).subtree(), AnnotatedPredicate::unwrap))
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
     * <p>The perf subtree may be a leaf {@link AnnotatedPredicate} or — when bubble-up
     * fired in {@link #combine} — an AND/OR/NOT of leaves; in both cases the original
     * (peer-unaware) RexNode is reconstructed via {@link #unwrapPreservingConnectors}.
     * Correctness-delegation can also carry a bubbled subtree, but its placeholder only
     * references the annotation id, not the subtree contents.
     */
    RexNode makePlaceholder(Delegated d) {
        if (d.performanceDelegation()) {
            RexNode original = unwrapPreservingConnectors(d.subtree(), AnnotatedPredicate::unwrap);
            return DelegationPossibleFunction.makeCall(rexBuilder, original, d.firstAnnotationId());
        }
        return DelegatedPredicateFunction.makeCall(rexBuilder, d.firstAnnotationId());
    }

    /**
     * Walks a delegated subtree — a leaf {@link AnnotatedPredicate} or an AND/OR/NOT of
     * leaves bubbled up through {@link #combine} — applying {@code leafFn} to every leaf
     * while preserving the boolean structure. The cast on the leaf is intentional: any
     * other RexNode shape is a planner-invariant violation and should fail loudly.
     */
    private RexNode unwrapPreservingConnectors(RexNode node, Function<AnnotatedPredicate, RexNode> leafFn) {
        if (node instanceof RexCall call
            && (call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR || call.getKind() == SqlKind.NOT)) {
            List<RexNode> rewritten = new ArrayList<>(call.getOperands().size());
            for (RexNode operand : call.getOperands()) {
                rewritten.add(unwrapPreservingConnectors(operand, leafFn));
            }
            return call.clone(call.getType(), rewritten);
        }
        return leafFn.apply((AnnotatedPredicate) node);
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

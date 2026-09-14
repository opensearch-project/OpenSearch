/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.List;
import java.util.function.Function;

/**
 * Marker interface for all OpenSearch custom RelNodes that carry backend assignment
 * and per-column storage metadata.
 *
 * <p>Extends Calcite's {@link PhysicalNode} so distribution traits can propagate TOP-DOWN during
 * Volcano costing ({@code setTopDownOpt(true)}): {@link #passThroughTraits} answers "given this
 * required distribution, what do I demand of my inputs?" and {@link #deriveTraits} answers "given
 * this child's distribution, what can I output?". Exchange placement then becomes emergent AND
 * priced, instead of being re-derived after the fact.
 *
 * <p>TODO: consider making this an abstract class storing {@code viableBackends} centrally,
 * with a default {@link #copyResolved} that returns {@code this} when already narrowed to
 * a single backend — avoids unnecessary copies during plan forking.
 *
 * <p>TODO: when JMH benchmarks show RelNode copy/strip as a hotspot, consider
 * preserving the original LogicalXxx fragment alongside the marked fragment through
 * the DAG to avoid reconstruction via {@link #copyResolved} and {@link #stripAnnotations}.
 *
 * @opensearch.internal
 */
public interface OpenSearchRelNode extends PhysicalNode {

    /** Recursion cap for {@link #effectiveDistributionOf}, so a cyclic memo cannot spin. */
    int EFFECTIVE_DISTRIBUTION_MAX_DEPTH = 64;

    /**
     * The distribution the data reaching {@code rel} actually lives at, seeing THROUGH the UNRESOLVED
     * seeds the HEP marking phase leaves behind.
     *
     * <p>Why this exists rather than reading {@code rel.getTraitSet()} directly. The Volcano split rules
     * ask a placement question about their input — "are both arms the same single-shard table?", "is this
     * arm a shard-local scan?", "is this input partitioned?" — and they use the answer to decide whether to
     * REGISTER an alternative at all. A marking rule that seeds {@code any()} answers that question with
     * {@code locality=null, type=ANY}, which every one of those predicates reads as "no", so the
     * alternative is never registered and the plan silently falls back to coordinator-gathering. That is
     * not a costing question the search can recover from later: the alternative does not exist.
     *
     * <p>So an UNRESOLVED node is asked what it WOULD deliver over its input's effective distribution, via
     * the same {@link #deriveTraits} hook the top-down search uses. Delegating keeps this resolver from
     * drifting away from the hooks: an operator that changes its mind about what it delivers changes both
     * answers at once. An operator that declines to derive (or is not single-input, or whose input is
     * itself unresolved) keeps its UNRESOLVED answer, so callers stay as conservative as they were before.
     *
     * @return the effective distribution, or null when {@code rel} carries no distribution trait at all —
     *         which is NOT the same as UNRESOLVED and callers already treat as "unknown, do not act"
     */
    static OpenSearchDistribution effectiveDistributionOf(RelNode rel) {
        return effectiveDistributionOf(rel, EFFECTIVE_DISTRIBUTION_MAX_DEPTH);
    }

    private static OpenSearchDistribution effectiveDistributionOf(RelNode rel, int remainingDepth) {
        if (rel == null || remainingDepth <= 0) {
            return null;
        }
        RelNode current = RelNodeUtils.unwrapHep(rel);
        // The node's OWN trait wins whenever it makes a claim, and that includes a RelSubset: a subset's
        // trait set IS the placement its members deliver, so it must be read BEFORE resolving to a member.
        // Resolving first and reading the member's traits reports something else entirely — the member may
        // be the marking phase's UNRESOLVED seed, or sit at a different distribution than the subset — which
        // silently changes every predicate built on this.
        OpenSearchDistribution own = distributionOf(current.getTraitSet());
        if (own == null || own.getType() != RelDistribution.Type.ANY) {
            return own;
        }
        // UNRESOLVED from here down. During Volcano an input is a RelSubset whose getInputs() is empty, so
        // resolve it to a concrete member first or the descent stops one hop short.
        if (current instanceof RelSubset subset) {
            RelNode member = subset.getBestOrOriginal();
            if (member == null || member == current) {
                return own;
            }
            OpenSearchDistribution resolved = effectiveDistributionOf(member, remainingDepth - 1);
            return resolved == null ? own : resolved;
        }
        if (current.getInputs().size() != 1 || !(current instanceof OpenSearchRelNode physical)) {
            return own;
        }
        OpenSearchDistribution childDistribution = effectiveDistributionOf(current.getInput(0), remainingDepth - 1);
        if (childDistribution == null || childDistribution.getType() == RelDistribution.Type.ANY) {
            return own;
        }
        Pair<RelTraitSet, List<RelTraitSet>> derived = physical.deriveTraits(
            current.getInput(0).getTraitSet().replace(childDistribution),
            0
        );
        if (derived == null) {
            return own;
        }
        OpenSearchDistribution delivered = distributionOf(derived.left);
        return delivered == null ? own : delivered;
    }

    /** Returns the {@link OpenSearchDistribution} carried by {@code traits}, or null if absent. */
    static OpenSearchDistribution distributionOf(RelTraitSet traits) {
        for (int i = 0; i < traits.size(); i++) {
            RelTrait trait = traits.getTrait(i);
            if (trait instanceof OpenSearchDistribution distribution) {
                return distribution;
            }
        }
        return null;
    }

    /**
     * Most operators do not propagate a distribution. Returning {@code null} is Calcite's contract for
     * "no alternative for this request", which is the safe default: an operator that has not opted in
     * simply produces no top-down alternative, and the existing enforcement path still applies. This
     * is what lets the ~10 rel nodes with no distribution algebra of their own stay untouched.
     */
    @Override
    default Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        return null;
    }

    /**
     * True when any input's distribution is still UNRESOLVED ({@code Type.ANY}).
     *
     * <p>THE invariant that keeps placement legality out of the cost model's shape tables: an unresolved
     * input has no decided location, so nothing above it has a defined cost — or a defined correctness.
     * Every operator's {@code computeSelfCost} refuses such an input, which confines the seed nodes the HEP
     * marking phase produces (they cannot demand traits of their inputs, so they claim nothing) to the ANY
     * subset. Only their {@code passThroughTraits}/{@code deriveTraits} alternatives, which set self and
     * input traits together, are consumable.
     *
     * <p>Miss this check in ONE operator and that operator becomes the hole: a Union that skipped it let a
     * per-partition {@code SINGLE} aggregate through as an ANY subset, and the gather above concatenated
     * three partial results without merging them.
     */
    default boolean hasUnresolvedInput() {
        for (RelNode input : ((RelNode) this).getInputs()) {
            OpenSearchDistribution dist = distributionOf(input.getTraitSet());
            if (dist != null && dist.getType() == org.apache.calcite.rel.RelDistribution.Type.ANY) {
                return true;
            }
        }
        return false;
    }

    /** Counterpart to {@link #passThroughTraits} for bottom-up derivation. Null means "no alternative". */
    @Override
    default Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        return null;
    }

    /** All backends that could execute this operator, including via delegation. */
    List<String> getViableBackends();

    /** Per-column storage metadata aligned with this node's output row type field order. */
    List<FieldStorageInfo> getOutputFieldStorage();

    /** Returns annotations inside this operator (predicates, calls, expressions). Empty if none. */
    default List<OperatorAnnotation> getAnnotations() {
        return List.of();
    }

    /**
     * Creates a copy with viableBackends narrowed to the given backend and
     * annotations replaced with the resolved versions.
     *
     * <p>{@code children} contains the resolved inputs in the same order as
     * the node's inputs. Single-input operators (Filter, Aggregate, Sort)
     * use {@code children.getFirst()}; future multi-input operators (Join) will
     * use multiple entries.
     *
     * @param backend              the chosen backend for this operator
     * @param children             resolved child RelNodes
     * @param resolvedAnnotations  annotations narrowed to single backends, same order as {@link #getAnnotations()}
     */
    RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations);

    /**
     * Returns a clean standard Calcite RelNode with viableBackends dropped and
     * annotations unwrapped to original expressions. Passed to the backend's
     * {@link FragmentConvertor}.
     *
     * <p>{@code strippedChildren} follows the same ordering convention as
     * {@code children} in {@link #copyResolved}.
     *
     * @param strippedChildren children already stripped
     */
    RelNode stripAnnotations(List<RelNode> strippedChildren);

    /**
     * Returns a clean standard Calcite RelNode with annotations resolved via the given function.
     * The resolver decides per-annotation what to return: the unwrapped original for native
     * annotations, or a placeholder (e.g., {@code delegated_predicate(annotationId)}) for
     * delegated ones.
     *
     * <p>Default delegates to {@link #stripAnnotations(List)} — correct for operators
     * with no annotations (Sort, Scan, ExchangeReducer, StageInputScan).
     *
     * @param strippedChildren    children already stripped
     * @param annotationResolver  maps each annotation to its replacement RexNode
     */
    default RelNode stripAnnotations(List<RelNode> strippedChildren, Function<OperatorAnnotation, RexNode> annotationResolver) {
        return stripAnnotations(strippedChildren);
    }
}

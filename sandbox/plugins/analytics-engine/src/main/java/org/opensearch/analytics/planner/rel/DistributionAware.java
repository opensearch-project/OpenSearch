/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import java.util.List;

/**
 * Per-operator distribution algebra — the contract the post-CBO distribution-enforcement pass
 * (Option B, see {@code MPP-GENERAL-SCHEDULING-DESIGN.md}) consults to place exchanges generically,
 * the way Spark's {@code EnsureRequirements} and Presto's {@code AddExchanges} do.
 *
 * <p>An operator declares two things, both as pure functions over {@link OpenSearchDistribution}:
 * <ol>
 *   <li>{@link #requiredInputDistribution} — "what partitioning must input {@code i} deliver for me to
 *       run distributed?" The pass inserts an exchange on input {@code i} ONLY when the input's actual
 *       output distribution does not {@code satisfy()} this requirement (so a co-partitioned child needs
 *       no exchange — that's how the multi-tier cascade emerges for any depth/shape).</li>
 *   <li>{@link #deriveOutputDistribution} — "given my inputs' actual distributions, what do I output?"
 *       e.g. an INNER hash-join keyed on its left equi keys outputs {@code WORKER+HASH(leftKeys, N)}, so a
 *       parent join/aggregate keyed on the same column consumes it with no further exchange.</li>
 * </ol>
 *
 * <p>This is the SAME logic {@code presto_poc}'s {@code OpenSearchJoin.passThroughTraits}/
 * {@code deriveTraits} expressed as Volcano {@code PhysicalNode} hooks — but as plain methods the
 * enforcement pass calls in bottom-up mode, so we do NOT flip the optimizer to top-down. Each operator's
 * implementation is unit-testable in isolation.
 *
 * <p>An operator that does not implement this interface is treated by the pass as "no distribution
 * requirement on its inputs, output distribution unknown" — i.e. the pass leaves its inputs as the
 * CBO-chosen (gathered) shape. Only operators that can run distributed need implement it.
 *
 * @opensearch.internal
 */
public interface DistributionAware {

    /**
     * The distribution input {@code inputIndex} must deliver for this operator to run in its distributed
     * form at {@code partitionCount} partitions, or {@code null} when this operator imposes no requirement
     * on that input (the pass then leaves the input at its CBO-chosen distribution).
     *
     * <p>A non-null requirement is a DEMAND: the pass wraps the input in the matching exchange (via
     * {@code OpenSearchDistributionTraitDef.buildEnforcer}) unless the input already {@code satisfies()} it.
     *
     * @param inputIndex     0-based input position
     * @param partitionCount the resolved shuffle partition count (cluster setting → engine default)
     * @param traitDef       the per-query distribution trait def (the enforcement pass holds it; passing it
     *                       explicitly avoids relying on this node carrying a distribution trait, which a
     *                       freshly-built/marked node may not)
     */
    OpenSearchDistribution requiredInputDistribution(int inputIndex, int partitionCount, OpenSearchDistributionTraitDef traitDef);

    /**
     * The distribution this operator outputs given its inputs' ACTUAL output distributions (same order as
     * {@code getInputs()}), or {@code null} when the output distribution is not derivable / not useful to a
     * parent (the pass then treats this operator's output as un-co-partitionable and a parent that needs a
     * specific partitioning will demand an exchange).
     *
     * <p>Called only after the pass has decided each input's actual distribution, so an implementation may
     * assume {@code childDistributions} reflects any exchanges the pass already inserted below.
     *
     * @param childDistributions each input's actual output distribution (may contain {@code null} for an
     *                           input whose distribution is unknown)
     * @param traitDef           the per-query distribution trait def
     */
    OpenSearchDistribution deriveOutputDistribution(
        List<OpenSearchDistribution> childDistributions,
        OpenSearchDistributionTraitDef traitDef
    );
}

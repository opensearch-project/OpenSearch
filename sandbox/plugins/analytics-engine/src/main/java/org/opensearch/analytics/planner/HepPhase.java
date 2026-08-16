/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Builder + executor for a single HEP rule pass with {@link RuleProfilingListener}
 * integration. Functionally equivalent to Calcite's {@code Programs.of(HepProgram, ...)}
 * with two sandbox-specific extensions Calcite doesn't expose:
 *
 * <ol>
 *   <li><b>Listener attachment</b> — Calcite's {@code Programs.of(HepProgram)} constructs
 *       its inner {@link HepPlanner} without exposing a hook to add custom listeners.
 *       This builder instantiates {@code HepPlanner} directly so we can call {@code
 *       hepPlanner.addListener(listener)} and feed the existing
 *       {@link RuleProfilingListener}.</li>
 *   <li><b>Phase boundary callbacks</b> — Calcite's {@link org.apache.calcite.plan.RelOptListener}
 *       interface only has rule-level events (attempted / production / discarded /
 *       equivalence / chosen); it has no notion of "phase". The
 *       {@link RuleProfilingListener#beginPhase(String)} /
 *       {@link RuleProfilingListener#endPhase(String)} pair bracketing the inner
 *       {@code HepPlanner.findBestExp()} is where per-phase wall clock and rule-attempt
 *       attribution come from.</li>
 * </ol>
 *
 * <h2>Instruction model</h2>
 *
 * <p>{@link #addRuleInstance(RelOptRule)} and {@link #addRuleCollection(Collection)} mirror
 * the same-named methods on {@link HepProgramBuilder}. Each call appends one Hep
 * instruction; instructions execute in the order they were added. The two shapes are
 * <strong>not</strong> interchangeable:
 *
 * <ul>
 *   <li>{@code addRuleInstance(R)} — R runs to fixpoint, then the next instruction
 *       starts. Use this when a later rule must observe the post-fixpoint output of
 *       this one (e.g. {@code FILTER_MERGE} after the filter transposes have settled).</li>
 *   <li>{@code addRuleCollection([R1, R2, ...])} — every rule in the group is tried at
 *       each vertex within a single fixpoint loop, so a rewrite by one rule can cascade
 *       into a match by another in the same pass. Use this when the rules are mutually
 *       independent and should converge together.</li>
 * </ul>
 *
 * <p>Each {@code run(...)} invocation creates a fresh {@link HepPlanner} — {@code
 * HepPlanner} is not reusable across phases (its {@code mainProgram} is final, and
 * {@code findBestExp()} collects garbage and dumps rule-attempt counters at the end).
 * This mirrors Calcite's own design — every {@code Programs.of(HepProgram).run(...)}
 * also instantiates a fresh {@code HepPlanner} on each invocation.
 *
 * <p>Usage:
 * <pre>
 * RelNode out = HepPhase.named("pushdown-rules")
 *     .bottomUp()
 *     .addRuleCollection(List.of(CoreRules.FILTER_PROJECT_TRANSPOSE,
 *                                CoreRules.FILTER_AGGREGATE_TRANSPOSE,
 *                                CoreRules.FILTER_INTO_JOIN))   // cascade within one fixpoint
 *     .addRuleInstance(CoreRules.FILTER_MERGE)                  // only after the group converges
 *     .run(input, listener);
 * </pre>
 *
 * @opensearch.internal
 */
public final class HepPhase {

    private final String name;
    private final List<Consumer<HepProgramBuilder>> instructions = new ArrayList<>();
    private boolean bottomUp = false;
    private UnaryOperator<RelNode> postProcess = UnaryOperator.identity();

    private HepPhase(String name) {
        this.name = name;
    }

    /** Start building a phase. The {@code name} is what {@link RuleProfilingListener} records. */
    public static HepPhase named(String name) {
        return new HepPhase(name);
    }

    /** Set the Hep match order to {@code BOTTOM_UP}. Default is the planner's natural
     *  order (depth-first, top-down). */
    public HepPhase bottomUp() {
        this.bottomUp = true;
        return this;
    }

    /** Append a {@link HepProgramBuilder#addRuleInstance(RelOptRule)} instruction.
     *  The rule runs to fixpoint before any later instruction starts. */
    public HepPhase addRuleInstance(RelOptRule rule) {
        instructions.add(builder -> builder.addRuleInstance(rule));
        return this;
    }

    /** Append a {@link HepProgramBuilder#addRuleCollection(Collection)} instruction.
     *  All rules in the group cascade-fire within a single fixpoint loop. */
    public HepPhase addRuleCollection(Collection<? extends RelOptRule> rules) {
        List<RelOptRule> snapshot = List.copyOf(rules);
        instructions.add(builder -> builder.addRuleCollection(snapshot));
        return this;
    }

    /** Register a non-rule transformation that runs after the Hep planner completes,
     *  inside the same listener phase. Used by {@code subquery-remove} to invoke
     *  {@code RelDecorrelator.decorrelateQuery} on the Hep output — decorrelation is a
     *  visitor (not a {@link RelOptRule}) so it can't live inside Hep. */
    public HepPhase postProcess(UnaryOperator<RelNode> postProcess) {
        this.postProcess = postProcess;
        return this;
    }

    /** Build the Hep planner, run it on {@code input}, and apply the post-processing
     *  hook (if any). The listener (when non-null) sees a matched begin/end phase pair
     *  around both stages. */
    public RelNode run(RelNode input, RuleProfilingListener listener) {
        HepProgramBuilder builder = new HepProgramBuilder();
        if (bottomUp) {
            builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        }
        for (Consumer<HepProgramBuilder> instruction : instructions) {
            instruction.accept(builder);
        }
        HepPlanner planner = new HepPlanner(builder.build());
        if (listener != null) {
            planner.addListener(listener);
            listener.beginPhase(name);
        }
        try {
            planner.setRoot(input);
            return postProcess.apply(planner.findBestExp());
        } finally {
            if (listener != null) listener.endPhase(name);
        }
    }
}

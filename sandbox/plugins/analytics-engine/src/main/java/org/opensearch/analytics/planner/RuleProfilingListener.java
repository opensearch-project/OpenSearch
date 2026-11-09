/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptListener;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Per-query profiling listener for the planner. Single-threaded by construction —
 * Calcite invokes listener callbacks sequentially from {@code AbstractRelOptPlanner.fireRule}.
 *
 * <p>Records two kinds of measurements:
 * <ul>
 *   <li>Phase wall-clock time, set explicitly via {@link #beginPhase(String)} /
 *       {@link #endPhase(String)} bracketing each phase method's body.</li>
 *   <li>Per-rule {@link RuleMetrics} (attempts, productions, totalNanos) accumulated
 *       from {@code RuleAttemptedEvent} and {@code RuleProductionEvent} pairs.
 *       Mirrors the pattern Calcite uses internally in
 *       {@code AbstractRelOptPlanner.RuleAttemptsListener}.</li>
 * </ul>
 *
 * <p>Created only when profiling is enabled on {@link PlannerContext}; otherwise the
 * planner runs without any listener attached at all.
 *
 * @opensearch.internal
 */
public final class RuleProfilingListener implements RelOptListener {

    private final Map<String, Long> phaseDurationsNs = new LinkedHashMap<>();
    private final Map<String, RuleMetrics> ruleMetrics = new HashMap<>();

    private long phaseStartNs;
    private long ruleStartNs;

    public void beginPhase(String name) {
        phaseStartNs = System.nanoTime();
    }

    public void endPhase(String name) {
        phaseDurationsNs.merge(name, System.nanoTime() - phaseStartNs, Long::sum);
    }

    @Override
    public void ruleAttempted(RuleAttemptedEvent event) {
        if (event.isBefore()) {
            ruleStartNs = System.nanoTime();
            return;
        }
        String rule = event.getRuleCall().getRule().toString();
        long elapsed = System.nanoTime() - ruleStartNs;
        RuleMetrics metrics = ruleMetrics.computeIfAbsent(rule, k -> new RuleMetrics());
        metrics.attempts++;
        metrics.totalNanos += elapsed;
    }

    @Override
    public void ruleProductionSucceeded(RuleProductionEvent event) {
        // Calcite fires this twice per successful transform (before & after registration).
        // Count once on the "before" edge to match attempts semantics.
        if (!event.isBefore()) return;
        String rule = event.getRuleCall().getRule().toString();
        ruleMetrics.computeIfAbsent(rule, k -> new RuleMetrics()).productions++;
    }

    @Override
    public void relEquivalenceFound(RelEquivalenceEvent event) {}

    @Override
    public void relDiscarded(RelDiscardedEvent event) {}

    @Override
    public void relChosen(RelChosenEvent event) {}

    /**
     * Returns the live measurement maps. No defensive copy — callers must not mutate.
     * Sorting / formatting is a {@link PlannerProfile#format()} responsibility.
     */
    public PlannerProfile snapshot() {
        return new PlannerProfile(phaseDurationsNs, ruleMetrics);
    }

    /**
     * Number of times a Rule was attempted and applied with total time taken.
     */
    public static final class RuleMetrics {
        long attempts;
        long productions;
        long totalNanos;

        public long attempts() {
            return attempts;
        }

        public long productions() {
            return productions;
        }

        public long totalNanos() {
            return totalNanos;
        }
    }

    /** Profile snapshot returned by {@link #snapshot()}. Live views into the listener's state. */
    public record PlannerProfile(Map<String, Long> phaseDurationsNs, Map<String, RuleMetrics> rules) {

        /** Multi-line text dump suitable for INFO-level logging. Sorts rules by total time desc. */
        public String format() {
            StringBuilder sb = new StringBuilder();
            sb.append("Phase wall-clock:\n");
            phaseDurationsNs.forEach(
                (name, ns) -> sb.append(String.format(Locale.ROOT, "  %-28s %8d ms%n", name, TimeUnit.NANOSECONDS.toMillis(ns)))
            );
            sb.append("\nRules (sorted by total time):\n");
            sb.append(String.format(Locale.ROOT, "  %-60s %10s %12s %12s%n", "Rule", "Attempts", "Productions", "Time (us)"));
            rules.entrySet().stream().sorted((a, b) -> Long.compare(b.getValue().totalNanos, a.getValue().totalNanos)).forEach(e -> {
                RuleMetrics m = e.getValue();
                sb.append(
                    String.format(
                        Locale.ROOT,
                        "  %-60s %10d %12d %12d%n",
                        e.getKey(),
                        m.attempts,
                        m.productions,
                        TimeUnit.NANOSECONDS.toMicros(m.totalNanos)
                    )
                );
            });
            return sb.toString();
        }

        /** Phase names in insertion order. */
        public List<String> phases() {
            return List.copyOf(phaseDurationsNs.keySet());
        }
    }
}

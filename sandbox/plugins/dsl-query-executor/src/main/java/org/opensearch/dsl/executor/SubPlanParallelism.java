/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import java.util.OptionalInt;

/**
 * How many sub-plans of one DSL query may run concurrently ("K_eff"), as a pure function over an
 * injected {@link Inputs} record. Nothing here reads a setting, a thread pool or a cluster state —
 * every input has a named producer at the call site, which is what makes the whole grid unit-testable
 * without a live node.
 */
public final class SubPlanParallelism {

    /**
     * SEARCH threads withheld from the fan-out's budget. One is the coordinator's own: the DSL request
     * runs on a SEARCH thread for the whole fan-out ({@code TransportDslExecuteAction} dispatches onto
     * {@code ThreadPool.Names.SEARCH}), so that thread is not available to the sub-plans it launches.
     * The second is headroom, chosen rather than derived. Getting it wrong is bounded in one direction
     * only: the {@code K_search} term is dropped entirely when the pool size is unreadable, and the
     * result is floored at 1, so too large a reserve narrows the fan-out and can never widen it.
     */
    static final int SEARCH_RESERVE = 2;

    /**
     * Above this many plans the caller takes the sequential path instead of fanning out.
     *
     * <p>Sized against the plan count a request can actually emit, which is
     * {@code (size > 0 ? 1 : 0) + one per aggregation level + one per root aggregation with
     * min_doc_count > 1 + (1 flat COUNT)}. A 3-level nested aggregation with hits is 5; four sibling
     * root aggregations that each need their own HAVING-filtered count is 10. The bound has to sit
     * above the shapes a user can legitimately write, because a request past it silently never fans
     * out at any setting — the width line just reports {@code K_eff=1}.
     *
     * <p>What it protects is the inline drain: {@code PendingExecutions.finishAndRunNext} runs the next
     * queued plan on the <i>finishing</i> thread, so a plan that completes synchronously nests one
     * {@code finishAndRunNext -> tryRun -> run -> execute} frame group. In production that is only
     * reachable when a plan fails inline; a 64-plan inline test showed the depth itself is comfortable,
     * so this is headroom against pathological nesting, not a measured stack limit.
     */
    static final int MAX_FANOUT_PLANS = 15;

    /**
     * Hard ceiling on the operator-set K, re-applied here rather than trusted from the setting.
     * {@code dsl.query.max_parallel_sub_plans} enforces the same maximum in its own {@code Setting},
     * but this class is handed a plain {@code int}: a caller that ever reads the value from somewhere
     * else must not be able to widen the fan-out past the ceiling.
     *
     * <p><b>Widths above 2 are not yet known-good.</b> The fan-out benchmark measured 0 failures in 80
     * executions at {@code K <= 2} and 6 in 120 (5.0%) at {@code K >= 3}, where a 3-level aggregation
     * intermittently returns HTTP 500; the mechanism was not traced. The shipped default is 1, so no
     * cluster reaches that regime without an operator opting in, and the terms derived from the machine
     * ({@code K_gate} from the fragment-executor budget, {@code K_search} from the SEARCH pool) clamp
     * the width down on smaller hosts regardless. Trace and fix the {@code K >= 3} failures before
     * recommending anything above 2 in an operator runbook.
     */
    static final int MAX_K_SETTING = 5;

    private SubPlanParallelism() {}

    /**
     * Every input the {@code K_eff} decision needs, injected so the formula is testable without a live
     * node. Each field's producer is named in its own comment; this class reads nothing itself.
     *
     * @param n number of plans in the query ({@code QueryPlans.getAll().size()})
     * @param kSetting the operator's {@code dsl.query.max_parallel_sub_plans}, re-clamped here to
     *                 {@code [1, MAX_K_SETTING]}
     * @param vCpu {@code Runtime.getRuntime().availableProcessors()} on the coordinator
     * @param fragmentExecutorMultiplier the backend's concurrency-gate multiplier, unwrapped;
     *                                   <b>unread</b> when {@code gateTermPresent} is false, so the
     *                                   caller passes a deliberately poisonous placeholder there
     * @param targetPartitions the backend's derived {@code target_partitions}, already {@code >= 1}
     * @param gateTermPresent whether a backend declared the multiplier at all; {@code false} DROPS the
     *                        {@code K_gate} term rather than clamping it to 1
     * @param shardsOnBusiestNode S_node, from live coordinator routing
     * @param maxConcurrentShardRequestsPerNode the engine's per-node in-flight shard-request cap
     * @param searchPoolSize live {@code getMaximumPoolSize()} of the SEARCH executor;
     *                       {@link OptionalInt#empty()} DROPS the {@code K_search} term
     */
    public record Inputs(int n, int kSetting, int vCpu, double fragmentExecutorMultiplier, int targetPartitions, boolean gateTermPresent,
        int shardsOnBusiestNode, int maxConcurrentShardRequestsPerNode, OptionalInt searchPoolSize) {
    }

    /**
     * The chosen width together with every intermediate term that produced it, so the one observable
     * {@code K_eff} log line can report the values actually used instead of recomputing them (a second
     * computation could disagree with the first and the line would then describe a run that never
     * happened).
     *
     * @param kEff the effective number of sub-plans that may run concurrently, always {@code >= 1}
     * @param a fragments the concurrency gate admits per node; not meaningful when {@code kGate} is
     *          empty, because there is then no multiplier to derive it from
     * @param f fragments one sub-query costs on the busiest node
     * @param kGate the gate-derived sub-query bound, or empty when that term was dropped
     * @param kSearch the SEARCH-pool-derived sub-query bound, or empty when that term was dropped
     */
    public record Decision(int kEff, int a, int f, OptionalInt kGate, OptionalInt kSearch) {
    }

    /**
     * The effective fan-out width for the given inputs.
     *
     * @param in the injected inputs
     * @return {@code K_eff}, always in {@code [1, max(1, n)]}
     */
    static int computeKEff(Inputs in) {
        return decide(in).kEff();
    }

    /**
     * The same computation as {@link #computeKEff(Inputs)}, keeping the intermediate terms for the
     * observability line. All {@code n} of the query's plans go through the permit gate, so the width is
     * bounded by {@code n} rather than by {@code n - 1}.
     *
     * @param in the injected inputs
     * @return the width plus the terms it was derived from, with {@code kEff} in {@code [1, max(1, n)]}
     */
    public static Decision decide(Inputs in) {
        final int gatedPlans = in.n();
        // Both belts, deliberately: F feeds two divisions, and a shard count of 0 (red index) or a
        // relaxed cap on the declaring plugin's side must not reach them.
        int f = Math.max(1, Math.min(in.shardsOnBusiestNode(), in.maxConcurrentShardRequestsPerNode()));

        int a;
        OptionalInt kGate;
        if (in.gateTermPresent()) {
            // The producer clamps targetPartitions to >= 1; clamped again here because this record is a
            // seam that another reader could fill differently, and a 0 here would throw on the query
            // path — the one thing an advisory input must never do.
            int targetPartitions = Math.max(1, in.targetPartitions());
            a = Math.max(1, (int) Math.floor(in.vCpu() * in.fragmentExecutorMultiplier() / targetPartitions));
            kGate = OptionalInt.of(Math.max(1, ceilDiv(a, f)));
        } else {
            // No multiplier was declared, so there are no gate-admitted fragments to count. The term is
            // dropped below; `a` is reported as 1 only so the log line has a number, and the accompanying
            // `K_gate=absent` is what tells a reader it took no part in the decision. The multiplier
            a = 1;
            kGate = OptionalInt.empty();
        }

        OptionalInt kSearch = in.searchPoolSize().isPresent()
            ? OptionalInt.of(Math.max(1, (in.searchPoolSize().getAsInt() - SEARCH_RESERVE) / f))
            : OptionalInt.empty();

        // A query with one plan has no width decision to make, and clamp(..., 1, gatedPlans) would be
        // malformed at 0 (upper < lower). Returning 1 also keeps the
        // caller from ever constructing a PendingExecutions(0), whose constructor asserts permits > 0.
        if (gatedPlans <= 1) {
            return new Decision(1, a, f, kGate, kSearch);
        }

        int kEff = Math.min(gatedPlans, Math.max(1, Math.min(MAX_K_SETTING, in.kSetting())));
        if (kGate.isPresent()) {
            kEff = Math.min(kEff, kGate.getAsInt());
        }
        if (kSearch.isPresent()) {
            kEff = Math.min(kEff, kSearch.getAsInt());
        }
        // Both ends of the clamp: every term above is >= 1, so this only re-states the invariant the
        // callers rely on (a gate of width 0 or a width above the plan count).
        kEff = Math.max(1, Math.min(kEff, gatedPlans));
        return new Decision(kEff, a, f, kGate, kSearch);
    }

    /** Integer ceiling division for two positive ints. */
    private static int ceilDiv(int dividend, int divisor) {
        return (dividend + divisor - 1) / divisor;
    }
}

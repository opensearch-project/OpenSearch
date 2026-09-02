/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.dsl.settings.DslGateInputs;
import org.opensearch.dsl.settings.DslQuerySettings;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;

/**
 * Executes the plans of one DSL query through the analytics engine's {@link QueryPlanExecutor} and
 * collects their results in plan order.
 */
public class DslQueryPlanExecutor {

    private static final Logger logger = LogManager.getLogger(DslQueryPlanExecutor.class);

    /**
     * The three non-numeric renderings of a width-line term. Part of SC-10's contract and NOT
     * interchangeable — {@link #logKEff} documents what each one tells a reader. Kept as constants so the
     * strings the runbook matches on exist in exactly one place.
     */
    private static final String TERM_ABSENT = "absent";
    private static final String TERM_SKIPPED = "skipped";
    private static final String TERM_UNAVAILABLE = "unavailable";

    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> executor;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final DslQuerySettings dslSettings;
    private final DslGateInputs gateInputs;

    /**
     * Creates an executor backed by the given analytics engine plan executor.
     *
     * @param executor analytics engine executor that runs individual RelNode plans
     * @param clusterService supplies the coordinator's operation routing, for the shard-layout input of
     *                       the fan-out width
     * @param threadPool supplies the live SEARCH executor, for the pool-size input of the fan-out width
     * @param dslSettings holder of {@code dsl.query.max_parallel_sub_plans}, read once per query
     * @param gateInputs reader for the cross-plugin concurrency-gate inputs, read once per query
     */
    public DslQueryPlanExecutor(
        QueryPlanExecutor<RelNode, Iterable<Object[]>> executor,
        ClusterService clusterService,
        ThreadPool threadPool,
        DslQuerySettings dslSettings,
        DslGateInputs gateInputs
    ) {
        this.executor = executor;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.dslSettings = dslSettings;
        this.gateInputs = gateInputs;
    }

    // TODO: add per-plan error handling so a failure in one plan
    // doesn't prevent returning partial results from other plans (e.g. HITS)
    /**
     * Executes all plans of one request and delivers their results, in plan order, to the listener.
     *
     * @param plans the query plans to execute
     * @param state the request's cluster-state snapshot, read only for the shard-layout width input; a
     *              {@code null} drops that input
     * @param concreteIndex the request's single resolved concrete index, used only to read the shard
     *                      layout the fan-out width divides by; a {@code null} drops that input
     * @param listener receives the ordered list of results on success, or the failure
     */
    public void execute(QueryPlans plans, ClusterState state, String concreteIndex, ActionListener<List<ExecutionResult>> listener) {
        List<QueryPlans.QueryPlan> queryPlans = plans.getAll();
        final int n = queryPlans.size();
        if (n == 0) {
            listener.onResponse(List.of());
            return;
        }
        if (n == 1) {
            // Returns before the width decision, so a single-plan query emits NO width line. That absence is
            // part of the line's contract: it means "not a multi-plan query" and must never come to mean "the
            // line was dropped", which is what a K_eff=1 line here would make it ambiguous with.
            executeNext(queryPlans, 0, new ArrayList<>(1), listener);
            return;
        }
        // Read once, here, and threaded on as a boolean so no later frame can ask the question again and get
        // a different answer for the same query. Read from `plans` because that is the last frame holding the
        // typed QueryPlans; every frame below carries the untyped list.
        final boolean hasAggregation = plans.has(QueryPlans.Type.AGGREGATION);
        final int kEff = decideWidth(n, hasAggregation, state, concreteIndex);
        if (kEff == 1) {
            // A width-1 gate is the sequential chain with extra bookkeeping, so take the chain itself. This
            // is what the shipped default (max_parallel_sub_plans = 1) takes, and it is byte-identical to the
            // behaviour before the fan-out existed.
            executeNext(queryPlans, 0, new ArrayList<>(n), listener);
            return;
        }
        dispatchGated(queryPlans, 0, n, new SubPlanResultCollector(n, listener), new PendingExecutions(kEff));
    }

    /**
     * The {@code K_eff} decision: settles the width (emitting the one observability line for this query on
     * every path through here) and never throws. Runs on the calling thread, before any plan is dispatched.
     *
     * @param n number of plans in this query, all of which go through the gate
     * @param hasAggregation whether this query carries an AGGREGATION plan; {@code false} settles the width at
     *                       1 before any input is read
     * @param state the request's snapshot, source of the shard-layout read
     * @param concreteIndex the request's resolved concrete index, or null
     * @return the width to run at, always {@code >= 1}
     */
    private int decideWidth(int n, boolean hasAggregation, ClusterState state, String concreteIndex) {
        // Boxed so "not read yet" is representable: the sentinel renders as the state string below. Today
        // this read cannot throw (DslQuerySettings caches the value in a volatile field and refreshes it
        // from a settings-update consumer), which is exactly why it is the read taken FIRST — but a later
        Integer kSetting = null;
        SubPlanParallelism.Decision decision = null;
        String unread = TERM_SKIPPED;
        try {
            kSetting = dslSettings.maxParallelSubPlans();
            // Bound the inline drain depth: PendingExecutions.finishAndRunNext drains the next queued task
            // on the finishing thread, so a callee that completes inline nests one frame group per plan.
            boolean tooManyPlans = n > SubPlanParallelism.MAX_FANOUT_PLANS;
            // Ordered cheapest-decisive-first, deliberately. With no aggregation plan to overlap, at the
            // shipped default (max_parallel_sub_plans = 1), and above the plan bound, K_eff is 1 whatever
            // every other term says, so reading them would be pure waste on the query hot path:
            if (hasAggregation && kSetting > 1 && tooManyPlans == false) {
                decision = SubPlanParallelism.decide(readInputs(n, kSetting, state, concreteIndex));
            }
        } catch (RuntimeException e) {
            // DEBUG, not WARN: on a node whose registry really did lose a key this fires on every
            // multi-plan query, and the search itself is unaffected — it just runs sequentially, which is
            // the shipped default anyway.
            logger.debug("the fan-out width could not be read; running the sub-plans sequentially", e);
            decision = null;
            unread = TERM_UNAVAILABLE;
        }
        int kEff = decision == null ? 1 : decision.kEff();
        logKEff(kSetting, decision, unread, n, kEff);
        return kEff;
    }

    /**
     * Sends plans {@code [from, n)} through one permit gate, reporting each to the collector by its own
     * plan index. The single copy of the fan-out's permit accounting; today every caller enters at
     * {@code from = 0}, since every plan is gated.
     *
     * @param queryPlans the query's plans
     * @param from the first plan index to gate
     * @param n the query's plan count, i.e. the exclusive upper bound of the dispatch
     * @param collector the collector every gated plan reports to; must be sized for exactly {@code n - from}
     *                  reports, which is checked before the first dispatch — a mismatch fails the request
     *                  through the collector rather than hanging it, and nothing is dispatched
     * @param gate a fresh gate of the settled width, owned by this dispatch alone
     */
    private void dispatchGated(
        List<QueryPlans.QueryPlan> queryPlans,
        int from,
        int n,
        SubPlanResultCollector collector,
        PendingExecutions gate
    ) {
        // The dispatch range and the collector's report count are set independently; a disagreement is a
        // HANG one way and an early terminal with a duplicate query still in flight the other, never a
        // merely wrong value. Checked before the loop, so bailing out leaks no permit and abandons no
        // in-flight plan.
        if (collector.expectGatedRange(from, n) == false) {
            return;
        }
        for (int i = from; i < n; i++) {
            final int idx = i;
            final QueryPlans.QueryPlan plan = queryPlans.get(idx);
            // notifyOnce is OUTERMOST, and that order is load-bearing. runAfter fires its Runnable from a
            // finally on EVERY notification and has no once-only guard of its own, so with notifyOnce on the
            // inside a listener that is notified twice (see the catch below — the engine can complete the
            // listener and *then* throw) would release the permit twice: finishAndRunNext would decrement
            // past its own count and drain an extra queued task, admitting one plan more than K_eff.
            ActionListener<Iterable<Object[]>> perPlan = ActionListener.notifyOnce(
                ActionListener.runAfter(reportTo(collector, idx, plan), gate::finishAndRunNext)
            );
            // PendingExecutions.tryRun takes a BooleanSupplier, and this one returns true on EVERY path
            // — that is the whole contract with the gate, not a stub. PendingExecutions reads true as
            // "this work started and owes exactly one finishAndRunNext()", which perPlan pays on every
            // outcome — engine success, engine failure, and the catch below alike. Returning false would
            // mean "I started nothing", and the gate would then pass this permit to the next queued plan
            // ITSELF; since perPlan still releases the permit through runAfter, the window would be
            // credited twice and admit one plan more than K_eff. There is genuinely no declined path
            // here: this fan-out always has a plan to dispatch by the time its turn comes (a decline
            // models work that became unnecessary while queued, e.g. request cancellation, which is not
            // part of the fan-out).
            gate.tryRun(() -> {
                try {
                    // Same thread, strictly before this plan's dispatch: that program order is the
                    // happens-before edge that keeps this plan's invalidateMetadataQuery() from racing the
                    // engine's own invalidate for the same plan. Never in a completion callback, never
                    // outside tryRun. At the shipped INFO level the whole call costs one isDebugEnabled()
                    // boolean. Inside the try, not above it: at DEBUG it dereferences the plan's metadata
                    // provider and renders the plan, and a throw from there escaping this supplier would
                    // leak the permit it already took and skip the countdown, so the listener would never
                    // fire and the REST channel would hang. On the drain path the throw would also escape
                    // into the engine's completion thread through finishAndRunNext.
                    logPlan(plan.relNode());
                    // Null context, exactly as the sequential path passes it; see executeNext.
                    executor.execute(plan.relNode(), null, perPlan);
                } catch (Throwable t) {
                    // A plan that cannot be dispatched must still release its permit and count down, or the
                    // listener never fires and the REST channel hangs. perPlan does both in one call — so
                    // this path owes a finishAndRunNext exactly like a dispatched plan, hence true below.
                    //
                    // Throwable, not Exception: Calcite throws AssertionError outright where its plan
                    // invariants are violated, and logPlan renders a plan. An Error escaping this supplier
                    // would strand the permit and the countdown just as surely as an exception would, so the
                    // cleanup has to run for both.
                    //
                    // And it is deliberately NOT rethrown afterwards. Rethrowing propagates out of the
                    // dispatch loop, so the plans after this one are never dispatched and never count down —
                    // the collector cannot reach its terminal and the REST channel stays open with nobody to
                    // answer it, which is the one outcome this block exists to prevent. Reporting an Error as
                    // a plan failure is a lesser wrong than hanging the request; it is wrapped so the
                    // listener's Exception contract holds, and the cause carries the original.
                    perPlan.onFailure(t instanceof Exception e ? e : new RuntimeException(t));
                }
                return true;
            });
        }
    }

    /**
     * One fanned-out plan's outcome, reported to the collector exactly once.
     *
     * @param collector the query's result collector
     * @param idx this plan's index in the query's plan list
     * @param plan the plan being dispatched, carried into its result
     * @return the listener to hand to the engine for that plan
     */
    private ActionListener<Iterable<Object[]>> reportTo(SubPlanResultCollector collector, int idx, QueryPlans.QueryPlan plan) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Iterable<Object[]> rows) {
                ExecutionResult result;
                try {
                    logRows(rows);
                    result = new ExecutionResult(plan, rows);
                } catch (Exception e) {
                    // A failure while handling this plan's rows is this plan's failure, and it has to count
                    // down like any other or the request never completes.
                    collector.planFailed(e);
                    return;
                }
                collector.planSucceeded(idx, result);
            }

            @Override
            public void onFailure(Exception e) {
                collector.planFailed(e);
            }
        };
    }

    /**
     * The sequential chain: dispatch plan {@code index}, and only from its success callback dispatch
     * plan {@code index + 1}. The first failure ends the chain — the listener fires {@code onFailure} with
     * that error and the remaining plans do not run.
     */
    private void executeNext(
        List<QueryPlans.QueryPlan> queryPlans,
        int index,
        List<ExecutionResult> results,
        ActionListener<List<ExecutionResult>> outer
    ) {
        if (index >= queryPlans.size()) {
            outer.onResponse(results);
            return;
        }
        QueryPlans.QueryPlan plan = queryPlans.get(index);
        RelNode relNode = plan.relNode();
        logPlan(relNode);
        // Sequential dispatch: a synchronous throw from here propagates out through the caller's
        // completion callback, where ActionListener.wrap routes it to the listener's failure arm.
        // TODO: context param is null, may carry execution hints
        executor.execute(relNode, null, ActionListener.wrap(rows -> {
            logRows(rows);
            results.add(new ExecutionResult(plan, rows));
            executeNext(queryPlans, index + 1, results, outer);
        }, outer::onFailure));
    }

    /**
     * Builds the fan-out width's inputs once per query, at the decision site, from their named producers.
     * Read here and not cached: the gate inputs are all dynamic settings, and an operator sweeping them
     * has to be able to change the width of a running node.
     */
    private SubPlanParallelism.Inputs readInputs(int n, int kSetting, ClusterState state, String concreteIndex) {
        OptionalDouble multiplier = gateInputs.fragmentExecutorMultiplier();
        return new SubPlanParallelism.Inputs(
            n,
            kSetting,
            Runtime.getRuntime().availableProcessors(),
            // Deliberately NaN and not 1.0: when the multiplier is absent the gate term is DROPPED, so this
            // value is never read — and a synthesised 1.0 would be indistinguishable from a genuinely
            // configured 1.0, i.e. it would clamp the width where the contract says drop the term.
            multiplier.orElse(Double.NaN),
            gateInputs.targetPartitions(),
            multiplier.isPresent(),
            shardsOnBusiestNode(state, concreteIndex),
            gateInputs.maxConcurrentShardRequestsPerNode(),
            searchPoolSize(threadPool.executor(ThreadPool.Names.SEARCH))
        );
    }

    /**
     * Shards of this request's index on the busiest node, read from the <em>request's own</em>
     * cluster-state snapshot. Not a second {@code clusterService.state()} read: two reads of one request
     * can straddle a routing change and produce a layout that never existed on any state.
     */
    private int shardsOnBusiestNode(ClusterState state, String concreteIndex) {
        if (state == null || concreteIndex == null) {
            // No snapshot to read routing from. 1 is the neutral value for a count, and F = max(1, ...)
            return 1;
        }
        return CoordinatorShardLayout.shardsOnBusiestNode(state, clusterService.operationRouting(), concreteIndex);
    }

    /**
     * Live maximum size of the SEARCH pool, or empty when it cannot be read.
     *
     * @param searchExecutor the SEARCH executor, as returned by {@code ThreadPool.executor}
     * @return its live maximum pool size, or empty
     */
    static OptionalInt searchPoolSize(ExecutorService searchExecutor) {
        return (searchExecutor instanceof OpenSearchThreadPoolExecutor tpe)
            ? OptionalInt.of(tpe.getMaximumPoolSize())
            : OptionalInt.empty();
    }

    /**
     * The one line per multi-plan query that makes the fan-out width observable — the rollout steps and
     * every benchmark cell attribute against it, and without it a run whose width was pinned to 1 by the
     * gate is indistinguishable from a K=1 baseline.
     *
     * @param kSetting the operator's width setting, or null if even that could not be read
     * @param decision the terms actually computed, or null when they were skipped or unavailable
     * @param unread how to render the terms {@code decision} does not carry
     * @param n the query's plan count
     * @param kEff the width this query runs at
     */
    private void logKEff(Integer kSetting, SubPlanParallelism.Decision decision, String unread, int n, int kEff) {
        logger.info(
            "dsl.fanout.k_eff K_setting={} A={} F={} K_gate={} K_search={} n={} K_eff={}",
            kSetting == null ? unread : String.valueOf(kSetting),
            decision == null ? unread : String.valueOf(decision.a()),
            decision == null ? unread : String.valueOf(decision.f()),
            decision == null ? unread : bound(decision.kGate()),
            decision == null ? unread : bound(decision.kSearch()),
            n,
            kEff
        );
    }

    /** A droppable term of the width line: its value, or {@link #TERM_ABSENT} when it left the min. */
    private static String bound(OptionalInt term) {
        return term.isPresent() ? String.valueOf(term.getAsInt()) : TERM_ABSENT;
    }

    private static void logRows(Iterable<Object[]> rows) {
        if (logger.isDebugEnabled() == false) return;
        List<Object[]> list = (rows instanceof List) ? (List<Object[]>) rows : null;
        int count = list != null ? list.size() : -1;
        logger.debug("Query result rowCount={}", count);
        if (list != null) {
            int preview = Math.min(20, list.size());
            for (int i = 0; i < preview; i++) {
                logger.debug("row[{}]={}", i, Arrays.toString(list.get(i)));
            }
            if (list.size() > preview) {
                logger.debug("... ({} more rows)", list.size() - preview);
            }
        }
    }

    /**
     * Logs a plan's text at DEBUG, and is the reference for how to do that safely.
     */
    private void logPlan(RelNode relNode) {
        if (logger.isDebugEnabled()) {
            org.apache.calcite.rel.metadata.JaninoRelMetadataProvider previous =
                org.apache.calcite.rel.metadata.RelMetadataQueryBase.THREAD_PROVIDERS.get();
            try {
                org.apache.calcite.rel.metadata.RelMetadataQueryBase.THREAD_PROVIDERS.set(
                    org.apache.calcite.rel.metadata.JaninoRelMetadataProvider.of(
                        java.util.Objects.requireNonNull(relNode.getCluster().getMetadataProvider())
                    )
                );
                relNode.getCluster().invalidateMetadataQuery();
                logger.debug("Executing RelNode:\n{}", relNode.explain());
            } finally {
                if (previous == null) {
                    org.apache.calcite.rel.metadata.RelMetadataQueryBase.THREAD_PROVIDERS.remove();
                } else {
                    org.apache.calcite.rel.metadata.RelMetadataQueryBase.THREAD_PROVIDERS.set(previous);
                }
            }
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.golden.CalciteTestInfra;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.dsl.settings.DslGateInputs;
import org.opensearch.dsl.settings.DslQuerySettings;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for the sub-plan fan-out.
 */
public class DslQueryPlanExecutorTests extends OpenSearchTestCase {

    private static final String MULTIPLIER_KEY = "datafusion.concurrency.fragment_executor_multiplier";
    private static final String SHARD_REQUEST_CAP_KEY = "analytics.query.max_concurrent_shard_requests_per_node";
    private static final String MAX_SLICE_COUNT_KEY = SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey();

    /** Same key/type/bounds as the sibling DataFusion plugin's descriptor, which is unreachable from here. */
    private static final Setting<Double> MULTIPLIER_COPY = Setting.doubleSetting(
        MULTIPLIER_KEY,
        1.5,
        0.1,
        10.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Same key/type/bounds as the parent analytics-engine plugin's descriptor. */
    private static final Setting<Integer> SHARD_REQUEST_CAP_COPY = Setting.intSetting(
        SHARD_REQUEST_CAP_KEY,
        5,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** The fixed leading token of the width line — what the runbook and the benchmark cells grep for. */
    private static final String K_EFF_TOKEN = "dsl.fanout.k_eff";

    /** The index the shard-layout tests route against; it exists only in their cluster-state fixture. */
    private static final String ROUTED_INDEX = "products";

    /** One log line per emitted plan, from {@code logPlan}. */
    private static final String PLAN_LOG_MARKER = "Executing RelNode";

    private static final String LOGGER_NAME = "org.opensearch.dsl.executor";
    private static final String DEBUG_LOGGING = LOGGER_NAME + ":DEBUG";
    private static final String DEBUG_REASON = "logPlan and logRows are DEBUG-level, so the level has to be raised to reach them";

    private LogicalTableScan scan;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        scan = TestUtils.createTestRelNode();
    }

    // ── The sequential contract (the shipped default) ───────────────────────

    public void testExecuteDelegatesEachPlanToExecutor() {
        List<Object[]> expectedRows = List.<Object[]>of(new Object[] { "laptop", 1200 });

        DslQueryPlanExecutor executor = executor((plan, ctx, listener) -> listener.onResponse(expectedRows), 1);
        QueryPlans plans = new QueryPlans.Builder().add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, scan)).build();

        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        executor.execute(plans, null, "test-index", future);
        List<ExecutionResult> results = future.actionGet();

        assertEquals(1, results.size());
        ExecutionResult result = results.get(0);
        assertSame(expectedRows, result.getRows());
        assertEquals(QueryPlans.Type.HITS, result.getType());
        assertNotNull(result.getPlan());
        assertSame(scan, result.getPlan().relNode());
        assertEquals(
            List.of(
                "name",
                "price",
                "brand",
                "rating",
                "created_date",
                "is_active",
                "timestamp",
                "location",
                "status",
                "binary_data",
                "event_time",
                "ip_address",
                "event_nanos",
                "scaled_price",
                "unsigned_counter",
                "tiny_val",
                "small_val",
                "float_val"
            ),
            result.getFieldNames()
        );
    }

    /**
     * Every plan of a request — plan 0 and each fanned-out sibling alike — is dispatched with the same
     * engine context the sequential path has always used, which on this branch is {@code null}.
     */
    public void testDispatchesEveryPlanWithANullEngineContext() {
        ClusterState state = ShardLayouts.clusterState(ROUTED_INDEX, List.of("node-0", "node-1"));

        List<Object> seen = Collections.synchronizedList(new ArrayList<>());
        DslQueryPlanExecutor executor = executor((plan, ctx, listener) -> {
            // Recorded rather than asserted inline: an assertion throwing here would be routed to the
            // request's failure arm and reported as a plan failure instead of as a test failure.
            seen.add(ctx);
            listener.onResponse(List.<Object[]>of());
        }, 2);

        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        executor.execute(plans(3), state, ROUTED_INDEX, future);
        assertEquals(3, future.actionGet().size());

        assertEquals("every plan must be dispatched", 3, seen.size());
        for (int i = 0; i < seen.size(); i++) {
            assertNull(
                "plan "
                    + i
                    + " must reach the engine with the same null context the sequential path passes, "
                    + "on the gated route too, got: "
                    + seen.get(i),
                seen.get(i)
            );
        }
    }

    /**
     * Iteration order is part of the contract: results come back in plan order, and this test is the only
     * thing that says so (see the class javadoc).
     */
    public void testExecuteRunsPlansInPlanOrder() {
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);

        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        executor(stub, 1).execute(plans, null, "test-index", future);
        List<ExecutionResult> results = future.actionGet();

        assertEquals(List.of(0, 1, 2), stub.dispatchOrder());
        assertEquals(3, results.size());
        for (int i = 0; i < 3; i++) {
            assertSame("result " + i + " must be plan " + i + "'s", plans.getAll().get(i).relNode(), results.get(i).getPlan().relNode());
        }
    }

    /**
     * The invariant the fan-out deliberately breaks, pinned here at the shipped default: plan {@code i+1}
     * is dispatched only after plan {@code i} has reported, so exactly one plan is ever outstanding.
     */
    public void testExecuteDispatchesPlanNPlusOneOnlyAfterPlanN() {
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        stub.deferAll = true;

        CapturingListener listener = new CapturingListener();
        executor(stub, 1).execute(plans, null, "test-index", listener);

        for (int i = 0; i < 3; i++) {
            assertEquals("plan " + i + " must be the only one outstanding", 1, stub.inFlight.get());
            assertEquals(i + 1, stub.dispatchOrder().size());
            stub.completeParked(i);
        }
        assertEquals(1, stub.highWater.get());
        assertEquals(3, listener.results.size());
    }

    /**
     * The abort arm: the first failure ends the chain — the listener gets that exception, no results, and
     * the plan after the failing one is never dispatched. The fan-out path deliberately differs (it waits
     * for the siblings), which is why this pin is taken at {@code K_setting = 1}.
     */
    public void testExecuteAbortsChainOnFirstPlanFailure() {
        RuntimeException boom = new RuntimeException("plan 1 failed");
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        stub.failInline.put(1, boom);

        CapturingListener listener = new CapturingListener();
        executor(stub, 1).execute(plans, null, "test-index", listener);

        assertSame("the first failure must reach the listener unchanged", boom, listener.failure);
        assertNull("a failed chain must not also deliver results", listener.results);
        assertEquals("the plan after the failing one must not be dispatched", List.of(0, 1), stub.dispatchOrder());
        assertEquals("exactly one terminal callback", 1, listener.terminalCalls);
    }

    /** The same fail-fast contract when it is plan 0 that fails: nothing else is dispatched at all. */
    public void testExecuteFailureOnFirstPlanDispatchesNothingElse() {
        RuntimeException boom = new RuntimeException("plan 0 failed");
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        stub.failInline.put(0, boom);

        CapturingListener listener = new CapturingListener();
        executor(stub, 1).execute(plans, null, "test-index", listener);

        assertSame(boom, listener.failure);
        assertNull(listener.results);
        assertEquals("only plan 0 may be dispatched", List.of(0), stub.dispatchOrder());
        assertEquals("exactly one terminal callback", 1, listener.terminalCalls);
    }

    /** At the shipped default the call sequence is the sequential one, in plan order. */
    public void testDegreeOneIsSequentialAndOrdered() {
        QueryPlans plans = plans(4);
        Stub stub = new Stub(plans);

        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        executor(stub, 1).execute(plans, null, "test-index", future);
        List<ExecutionResult> results = future.actionGet();

        assertEquals(1, stub.highWater.get());
        assertEquals(List.of(0, 1, 2, 3), stub.dispatchOrder());
        assertPlanOrder(plans, results);
    }

    // ── The fan-out ─────────────────────────────────────────────────────────

    /**
     * No short-circuit: a failure must not be reported while sibling sub-plans are still running, because
     * firing the listener early abandons distributed queries that have nobody left to report to.
     */
    public void testAllPlansCompleteBeforeFailureIsReported() {
        QueryPlans plans = plans(3);
        // One stamp source shared by the stub and the listener, so "after" is asserted on a real ordering
        // rather than on a sleep.
        AtomicInteger stamps = new AtomicInteger();
        Stub stub = new Stub(plans, stamps);
        stub.failInline.put(1, new RuntimeException("plan 1 failed"));
        stub.defer.add(2);

        CapturingListener listener = new CapturingListener(stamps);
        executor(stub, 2).execute(plans, null, "test-index", listener);

        assertEquals("plan 1 failed, but plan 2 is still running", 0, listener.terminalCalls);
        assertEquals(List.of(0, 1, 2), stub.dispatchOrder());
        int beforePlanTwo = stamps.get();
        stub.completeParked(2);

        assertEquals(1, listener.terminalCalls);
        assertNotNull(listener.failure);
        assertTrue("the failure must be reported after plan 2 finished", listener.terminalStamp > beforePlanTwo);
    }

    /** A plan that throws out of its dispatch still releases its permit and drives the countdown. */
    public void testSynchronousThrowMidFanOutStillCompletesListener() {
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        stub.throwInline.put(1, new IllegalStateException("dispatch of plan 1 threw"));

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        assertEquals(1, listener.terminalCalls);
        assertNotNull(listener.failure);
        assertTrue("plan 2 must still run", stub.dispatchOrder().contains(2));
    }

    /**
     * Every fan-out plan failing pre-dispatch still ends in exactly <b>one</b> failure reaching the request,
     * carrying nothing else with it.
     */
    public void testPreDispatchFailureDrivesCountdownToZero() {
        QueryPlans plans = plans(4);
        Stub stub = new Stub(plans);
        for (int i = 1; i < 4; i++) {
            stub.throwInline.put(i, new IllegalStateException("dispatch of plan " + i + " threw"));
        }

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        assertEquals(1, listener.terminalCalls);
        assertNotNull(listener.failure);
        assertEquals(
            "one _search must not carry K internal exceptions to the client: " + Arrays.toString(listener.failure.getSuppressed()),
            0,
            listener.failure.getSuppressed().length
        );
    }

    /**
     * The listener-notified-twice path, which nothing else covers: the engine completes the per-plan
     * listener and <em>then</em> throws, so the loop's {@code catch} calls {@code onFailure} on an
     * already-completed listener.
     */
    public void testCompleteThenThrowDoesNotDoubleReleaseThePermit() {
        QueryPlans plans = plans(5);
        Stub stub = new Stub(plans);
        stub.deferAll = true;
        stub.completeThenThrow.add(1);

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);
        stub.completeParked(0);

        // Plan 1 completed and threw, releasing its one permit; plans 2 and 3 hold the two permits and plan
        // 4 is queued behind them. A double release would have admitted plan 4 as well.
        assertEquals("a second permit release would have admitted a third plan at once", 2, stub.highWater.get());
        assertEquals(List.of(0, 1, 2, 3), stub.dispatchOrder());

        while (listener.terminalCalls == 0) {
            stub.completeAnyParked();
            assertTrue("in-flight " + stub.inFlight.get() + " exceeded K_eff", stub.inFlight.get() <= 2);
        }
        assertEquals("exactly one terminal callback", 1, listener.terminalCalls);
        assertNotNull("the fan-out must complete: " + listener.failure, listener.results);
        assertPlanOrder(plans, listener.results);
        assertEquals(2, stub.highWater.get());
    }

    /**
     * A throw out of the <em>request's own</em> listener must not come back as a second report for the plan
     * that happened to be last. That is what {@code ActionListener.wrap} would do: the countdown call sits in
     * its success body, so the terminal's throw would be routed to the same wrapper's failure arm, the plan
     * would count down twice, the countdown would drop below zero — permanently past the terminal it tests
     * for — and the exception would be parked unread in the collector's failure queue with nothing left to
     * fire it at. It is a silent loss, not a hang, which is exactly why nothing else here notices it.
     *
     * <p>The discriminating observation is that the throw <b>propagates to whoever completed the plan</b>.
     * The plans are parked and completed by the test rather than inline, so there is no dispatch
     * {@code catch} in between to absorb it: with the report outside the try the completion throws, and with
     * it inside the try the completion returns normally and the exception disappears.
     */
    public void testAThrowFromTheRequestListenerIsNotReportedAsAPlanFailure() {
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        stub.defer.add(1);
        stub.defer.add(2);

        ThrowingListener listener = new ThrowingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);
        assertEquals("both fan-out plans must be outstanding at width 2", List.of(0, 1, 2), stub.dispatchOrder());

        // Plan 1 only counts down; the terminal is plan 2's, and the request listener throws out of it.
        stub.completeParked(1);
        assertEquals("the terminal must not have fired yet", 0, listener.terminalCalls);

        IllegalStateException thrown = expectThrows(IllegalStateException.class, () -> stub.completeParked(2));
        assertEquals("the request listener's own failure", thrown.getMessage());
        assertEquals("the request listener must still have been notified exactly once", 1, listener.terminalCalls);
        assertEquals("its failure arm must not be reached — it is not the query that failed", 0, listener.failureCalls);
    }

    /**
     * An empty plan set completes with an empty list. It cannot arrive from the converter today, but the
     * branch is explicit code with a real alternative: falling through would index plan 0 of nothing, and
     * failing would turn a query the old sequential chain answered into an error.
     */
    public void testEmptyPlanSetCompletesWithAnEmptyList() {
        QueryPlans plans = new QueryPlans.Builder().build();
        Stub stub = new Stub(plans);

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        assertEquals(1, listener.terminalCalls);
        assertNull("an empty query is not a failure: " + listener.failure, listener.failure);
        assertEquals(List.of(), listener.results);
        assertEquals("nothing may be dispatched for an empty query", List.of(), stub.dispatchOrder());
    }

    /**
     * Above the inline-drain bound the executor takes the sequential path. The stub completes inline, which
     * is the only way to make this assertion mean anything: in production the engine forks onto SEARCH
     * before executing, so the nesting this bounds does not appear there at all.
     */
    public void testLargePlanCountFallsBackToSequential() {
        QueryPlans plans = plans(64);
        Stub stub = new Stub(plans);

        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        executor(stub, 2).execute(plans, null, "test-index", future);
        List<ExecutionResult> results = future.actionGet();

        assertEquals("the fallback must be sequential, not a width-2 gate", 1, stub.highWater.get());
        assertPlanOrder(plans, results);
    }

    // ── The fan-out ─────────────────────────────────────────────────────────

    /**
     * <b>The reason every plan is gated.</b> A 2-plan query — a 2-level nested aggregation with
     * {@code size: 0}, the measured production shape — is the common case. Gating both plans is what makes
     * {@code K_eff} 2 so they are genuinely in flight together; a shape that ran plan 0 alone first would
     * clamp the width to {@code n - 1 == 1} and never overlap anything here.
     */
    public void testTwoPlansRunBothConcurrently() {
        QueryPlans plans = plans(2);
        Stub stub = new Stub(plans);
        stub.deferAll = true;

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        assertEquals("both plans must be dispatched before either completes", List.of(0, 1), stub.dispatchOrder());
        assertEquals("K_eff = 2 at n = 2, the common production shape", 2, stub.inFlight.get());
        assertEquals(2, stub.highWater.get());

        stub.completeParked(1);
        assertNull("the listener must wait for plan 0", listener.results);
        stub.completeParked(0);

        assertEquals("exactly one terminal callback", 1, listener.terminalCalls);
        assertPlanOrder(plans, listener.results);
    }

    /**
     * Slotting by plan index, proved by completing the plans in an order unrelated to their plan order —
     * including plan 0 <b>last</b>, which is only expressible because plan 0 goes through the gate and
     * has already completed before its collector exists.
     */
    public void testResultsInPlanOrderWhenCompletionOrderShuffled() {
        QueryPlans plans = plans(4);
        Stub stub = new Stub(plans);
        stub.deferAll = true;

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        // K_eff = 2, so plans 0 and 1 are in flight and plans 2 and 3 are queued behind them.
        assertEquals(List.of(0, 1), stub.dispatchOrder());
        stub.completeParked(1);
        assertEquals("finishing plan 1 must admit the queued plan 2", List.of(0, 1, 2), stub.dispatchOrder());
        stub.completeParked(2);
        assertEquals("finishing plan 2 must admit the queued plan 3", List.of(0, 1, 2, 3), stub.dispatchOrder());
        stub.completeParked(3);
        assertNull("the listener must wait for plan 0", listener.results);
        stub.completeParked(0);

        assertEquals(1, listener.terminalCalls);
        assertPlanOrder(plans, listener.results);
    }

    /** The gate's whole job. Run with {@code -Dtests.iters=100}. */
    public void testNeverMoreThanKEffInFlight() {
        QueryPlans plans = plans(6);
        Stub stub = new Stub(plans);
        stub.deferAll = true;

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        assertEquals(2, stub.inFlight.get());
        while (listener.terminalCalls == 0) {
            stub.completeAnyParked();
            assertTrue("in-flight " + stub.inFlight.get() + " exceeded K_eff", stub.inFlight.get() <= 2);
        }
        assertEquals("high-water must not exceed K_eff", 2, stub.highWater.get());
        assertPlanOrder(plans, listener.results);
    }

    /**
     * A mid-flight failure: the listener still fires exactly once, still carries the
     * first failure, and still fires only after every sibling has reported — firing early would abandon
     * distributed queries with nobody left to report to.
     */
    public void testCompletesListenerExactlyOnceOnMidFlightFailure() {
        RuntimeException boom = new RuntimeException("plan 1 failed");
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        stub.defer.add(0);
        stub.failInline.put(1, boom);
        stub.defer.add(2);

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        assertEquals("plan 1's failure released its permit, so plan 2 must have been admitted", List.of(0, 1, 2), stub.dispatchOrder());
        assertEquals("plans 0 and 2 are still running", 0, listener.terminalCalls);

        stub.completeParked(2);
        assertEquals("plan 0 is still running", 0, listener.terminalCalls);
        stub.completeParked(0);

        assertEquals("exactly one terminal callback", 1, listener.terminalCalls);
        assertSame("the first failure must reach the listener unchanged", boom, listener.failure);
        assertNull("a failed query must not also deliver results", listener.results);
    }

    /**
     * A plan-0 failure does not suppress the other plans, pinned so it is a decision rather than a
     * surprise. They are already dispatched by the time it reports, so the collector waits for them and
     * reports plan 0's failure exactly once.
     */
    public void testDispatchesEveryPlanEvenWhenPlanZeroFails() {
        RuntimeException boom = new RuntimeException("plan 0 failed");
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        stub.failInline.put(0, boom);

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        assertEquals("plan 0 is dispatched through the gate like any other plan", List.of(0, 1, 2), stub.dispatchOrder());
        assertEquals(1, listener.terminalCalls);
        assertSame(boom, listener.failure);
        assertEquals("no plan may be left in flight", 0, stub.inFlight.get());
    }

    /**
     * Turning the arm on is not by itself a widening: at the shipped {@code max_parallel_sub_plans = 1} the
     * fan-out settles on width 1 and takes the plain sequential chain from plan 0. That is what makes
     * {@code K = 1} a byte-identical baseline rather than a second,
     * untested behaviour.
     */
    public void testWidthOneIsTheSequentialChain() {
        QueryPlans plans = plans(4);
        Stub stub = new Stub(plans);

        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        executor(stub, 1).execute(plans, null, "test-index", future);
        List<ExecutionResult> results = future.actionGet();

        assertEquals("width 1 must never have two plans outstanding", 1, stub.highWater.get());
        assertEquals(List.of(0, 1, 2, 3), stub.dispatchOrder());
        assertPlanOrder(plans, results);
    }

    /**
     * The inline-drain bound counts gated plans, and every plan is gated. At
     * {@code n == MAX_FANOUT_PLANS} the query still fans out; at {@code n == MAX_FANOUT_PLANS + 1} it must
     * fall back to sequential, because the inline drain would otherwise nest one frame group per plan.
     */
    public void testInlineDrainBoundCountsEveryPlan() {
        QueryPlans atBound = plans(SubPlanParallelism.MAX_FANOUT_PLANS);
        Stub atBoundStub = new Stub(atBound);
        atBoundStub.deferAll = true;
        CapturingListener atBoundListener = new CapturingListener();
        executor(atBoundStub, 2).execute(atBound, null, "test-index", atBoundListener);
        assertEquals("exactly MAX_FANOUT_PLANS gated plans must still fan out", 2, atBoundStub.inFlight.get());
        while (atBoundListener.terminalCalls == 0) {
            atBoundStub.completeAnyParked();
        }
        assertPlanOrder(atBound, atBoundListener.results);

        QueryPlans pastBound = plans(SubPlanParallelism.MAX_FANOUT_PLANS + 1);
        Stub pastBoundStub = new Stub(pastBound);
        pastBoundStub.deferAll = true;
        CapturingListener pastBoundListener = new CapturingListener();
        executor(pastBoundStub, 2).execute(pastBound, null, "test-index", pastBoundListener);
        assertEquals("one gated plan past the bound must be sequential", 1, pastBoundStub.inFlight.get());
        assertEquals(List.of(0), pastBoundStub.dispatchOrder());
        while (pastBoundListener.terminalCalls == 0) {
            pastBoundStub.completeAnyParked();
            assertTrue("the sequential path must never have two plans outstanding", pastBoundStub.inFlight.get() <= 1);
        }
        assertEquals(1, pastBoundStub.highWater.get());
        assertPlanOrder(pastBound, pastBoundListener.results);
    }

    /**
     * The payload contract under <b>real</b> concurrency, and the one that matters most for a measurement
     * tool: a racy fan-out would produce fast numbers that are quietly wrong, which is worse than no
     * fan-out at all. Every other fan-out test here completes the parked plans from the test thread, so the
     * gate's release and the collector's slotting are never actually concurrent there.
     *
     * <p>Run at the widest fan-out the inline-drain bound allows ({@code n == MAX_FANOUT_PLANS}, all of them
     * gated), with more drainers than permits so two releases overlap inside the gate. It pins that every
     * plan's payload lands in its own slot (nothing lost, duplicated or reordered), that the terminal fires
     * exactly once, and that concurrent releases cannot admit a plan past {@code K_eff} — including for
     * plan 0, which is a gated plan like any other. Run with {@code -Dtests.iters=100}.
     */
    public void testConcurrentCompletionsSlotEveryPlanExactlyOnce() throws Exception {
        QueryPlans plans = plans(SubPlanParallelism.MAX_FANOUT_PLANS);
        final int gatedPlans = plans.getAll().size();
        Stub stub = new Stub(plans);
        stub.deferAll = true;

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);
        // The dispatch loop runs on this thread and parks plans 0 and 1 before any drainer starts, so the
        // high-water mark of K_eff is reached deterministically and the assertion at the end is about the
        // gate never going *above* it under contention.
        assertEquals(2, stub.highWater.get());

        AtomicInteger completed = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        List<Thread> drainers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Thread drainer = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                while (completed.get() < gatedPlans && System.nanoTime() < deadlineNanos) {
                    if (stub.completeAnyParkedIfPresent()) {
                        completed.incrementAndGet();
                    } else {
                        // Nothing parked yet: a permit is held by a plan another drainer is still dispatching.
                        Thread.yield();
                    }
                }
            }, "fan-out-drainer-" + i);
            drainers.add(drainer);
            drainer.start();
        }
        start.countDown();
        for (Thread drainer : drainers) {
            drainer.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse("drainer " + drainer.getName() + " did not finish", drainer.isAlive());
        }

        // Asserted on the test thread, not inside the drainers: a bare assert in a spawned thread depends on
        // the runner's uncaught-exception handling to be seen, and a deadlocked fan-out has to fail on this
        // count rather than hang the suite.
        assertEquals("every gated plan must have completed", gatedPlans, completed.get());
        assertEquals("exactly one terminal callback", 1, listener.terminalCalls);
        assertNull("no plan failed: " + listener.failure, listener.failure);
        assertPlanOrder(plans, listener.results);
        assertEquals("concurrent releases must not admit a plan past K_eff", 2, stub.highWater.get());
    }

    // ── The aggregation-only gate ───────────────────────────────────────────

    /**
     * <b>A request with no aggregation plan must never fan out.</b> The overlap exists to run AGGREGATION
     * sub-plans at the same time, so a batch that carries none takes the sequential chain however wide the
     * gate is set.
     */
    public void testNonAggregationQueryNeverFansOut() {
        QueryPlans plans = hitsAndCountPlans(1);
        Stub stub = new Stub(plans);
        stub.deferAll = true;

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        // Before any completion: every gated plan is dispatched up front, so one dispatched plan
        // is the whole claim. A fanned-out run cannot produce this state at any width above 1.
        assertEquals("a non-aggregation query must dispatch plan 0 alone", List.of(0), stub.dispatchOrder());
        assertEquals(1, stub.inFlight.get());

        while (listener.terminalCalls == 0) {
            stub.completeAnyParked();
            assertTrue("the sequential path must never have two plans outstanding", stub.inFlight.get() <= 1);
        }
        assertEquals(1, stub.highWater.get());
        assertEquals("every plan must still run, just one at a time", List.of(0, 1), stub.dispatchOrder());
        assertEquals("exactly one terminal callback", 1, listener.terminalCalls);
        assertPlanOrder(plans, listener.results);

        // The control: the same n, the same setting, the same arm — one AGGREGATION plan instead of the
        // COUNT plan — must still overlap. The gate closes a request shape, it does not disable the feature.
        QueryPlans aggregation = plans(2);
        Stub aggregationStub = new Stub(aggregation);
        aggregationStub.deferAll = true;
        executor(aggregationStub, 2).execute(aggregation, null, "test-index", new CapturingListener());
        assertEquals("an aggregation query at the same settings must still fan out", List.of(0, 1), aggregationStub.dispatchOrder());
        assertEquals(2, aggregationStub.highWater.get());
    }

    /**
     * The two real request shapes, from the real converter, at one set of settings: the fan-out's reason for
     * existing still overlaps, and a plain search does not. {@code size: 0} plus a 2-level nested aggregation
     * is the measured production shape and emits no HITS plan at all, so this also pins that the eligibility
     * predicate asks whether <em>an</em> AGGREGATION plan is present rather than whether every plan is one —
     * the aggregation batch here carries the request-totals COUNT plan alongside its two aggregation plans,
     * and a stricter predicate would have disqualified the very shape the feature was built for.
     */
    public void testMeasuredAggregationShapeFansOutButAPlainSearchDoesNot() throws Exception {
        QueryPlans aggregation = sizeZeroNestedAggregationPlans();
        assertFalse("size: 0 emits no hits plan", aggregation.has(QueryPlans.Type.HITS));
        assertTrue("the fixture must be an aggregation request", aggregation.has(QueryPlans.Type.AGGREGATION));
        assertTrue("and a multi-plan one, or there is nothing to overlap", aggregation.getAll().size() >= 2);

        Stub aggregationStub = new Stub(aggregation);
        aggregationStub.deferAll = true;
        CapturingListener aggregationListener = new CapturingListener();
        executor(aggregationStub, 2).execute(aggregation, null, "test-index", aggregationListener);

        assertEquals("the measured production shape must still overlap", 2, aggregationStub.inFlight.get());
        assertEquals(2, aggregationStub.highWater.get());
        while (aggregationListener.terminalCalls == 0) {
            aggregationStub.completeAnyParked();
        }
        assertEquals(1, aggregationListener.terminalCalls);
        assertNull("no plan failed: " + aggregationListener.failure, aggregationListener.failure);

        QueryPlans plain = convert(new SearchSourceBuilder());
        assertFalse("a plain search converts to no aggregation plan", plain.has(QueryPlans.Type.AGGREGATION));
        assertEquals("the hits plan plus the COUNT plan that supplies hits.total", 2, plain.getAll().size());

        Stub plainStub = new Stub(plain);
        plainStub.deferAll = true;
        CapturingListener plainListener = new CapturingListener();
        executor(plainStub, 2).execute(plain, null, "test-index", plainListener);

        assertEquals("a plain search's two engine calls must not overlap", 1, plainStub.inFlight.get());
        while (plainListener.terminalCalls == 0) {
            plainStub.completeAnyParked();
            assertTrue("the sequential path must never have two plans outstanding", plainStub.inFlight.get() <= 1);
        }
        assertEquals(1, plainStub.highWater.get());
        assertEquals(1, plainListener.terminalCalls);
    }

    /**
     * SC-10 survives the gate: an ineligible multi-plan query is still measured exactly once. The line's
     * <em>absence</em> means "not a multi-plan query" and must keep meaning only that, so the gate sits at
     * the width decision rather than short-circuiting in front of it — a whole class of multi-plan queries
     * silently dropping out of the log would make every scraped cell unattributable.
     */
    public void testNonAggregationQueryIsStillMeasured() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey(), 2)
            .put(SHARD_REQUEST_CAP_KEY, 5)
            .build();
        ClusterSettings clusterSettings = registry(nodeSettings, SHARD_REQUEST_CAP_COPY);
        ClusterService clusterService = clusterService(nodeSettings, clusterSettings);

        QueryPlans plans = hitsAndCountPlans(1);
        Stub stub = new Stub(plans);
        DslQueryPlanExecutor executor = executor(stub, clusterService, new DslGateInputs(clusterSettings), mockThreadPool());
        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        ClusterState placed = ShardLayouts.clusterState(ROUTED_INDEX, List.of("node-0", "node-0", "node-1"));

        List<String> lines = capturingLogs(K_EFF_TOKEN, () -> executor.execute(plans, placed, ROUTED_INDEX, future));

        assertPlanOrder(plans, future.actionGet());
        assertEquals(1, stub.highWater.get());
        verify(clusterService, never()).operationRouting();

        assertEquals("an ineligible multi-plan query is still measured", 1, lines.size());
        Map<String, String> fields = parseKEffLine(lines.get(0));
        assertEquals("the operator's setting is reported as it is, not as the gate made it", "2", fields.get("K_setting"));
        assertEquals("2", fields.get("n"));
        assertEquals("1", fields.get("K_eff"));
        for (String term : List.of("A", "F", "K_gate", "K_search")) {
            assertEquals("a term the gate never reached must render as skipped, not as a number", "skipped", fields.get(term));
        }
    }

    // ── D2.2: the live SEARCH pool-size read ────────────────────────────────

    /**
     * The size has to come off the executor, live. {@code ThreadPool.info} is built once at node start and
     * never rebuilt, so it goes stale the moment the pool is resized — the second assertion here is what
     * makes this a regression guard for that rather than a tautology.
     */
    public void testSearchPoolSizeTracksLiveMaximumPoolSize() throws Exception {
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            ExecutorService searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
            OptionalInt before = DslQueryPlanExecutor.searchPoolSize(searchExecutor);
            assertTrue("SEARCH is always an OpenSearchThreadPoolExecutor on a real node", before.isPresent());
            int original = before.getAsInt();

            ((OpenSearchThreadPoolExecutor) searchExecutor).setMaximumPoolSize(original + 3);

            assertEquals(original + 3, DslQueryPlanExecutor.searchPoolSize(searchExecutor).getAsInt());
            assertEquals(
                "threadPool.info() is the stale reading this method exists to avoid",
                original,
                threadPool.info(ThreadPool.Names.SEARCH).getMax()
            );
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * The absent case, which cannot be provoked on a real node — SEARCH is always built as a resizable
     * {@code OpenSearchThreadPoolExecutor} — so an in-process injection is the only way to reach it. The
     * load-bearing half is that the term is then <b>dropped</b> from the width rather than replaced with a
     * guessed pool size; the paired assertion lives in
     * {@link SubPlanParallelismTests#testKEffDropsSearchTermWhenPoolSizeAbsent}. Neither half may be
     * deleted alone.
     */
    public void testSearchPoolSizeAbsentWhenExecutorIsNotOpenSearchThreadPoolExecutor() {
        ExecutorService plain = Executors.newFixedThreadPool(4);
        try {
            assertEquals(OptionalInt.empty(), DslQueryPlanExecutor.searchPoolSize(plain));
        } finally {
            plain.shutdownNow();
        }
    }

    // ── D2.4: the SC-9 mapping, observed through the width line ─────────────

    /**
     * Every gate input reaches the formula: with the multiplier at 3.0 and {@code target_partitions} at 1,
     * the reported fragment count is {@code vCpu * 3} and the reported sub-query bound is that over
     * {@code F}. Asserting through the width line rather than through a test-only accessor keeps the
     * production surface unchanged — and the line is the contract anyway.
     */
    public void testInputsPopulatedFromGateInputs() throws Exception {
        // max_slice_count = 1 pins target_partitions at 1 whatever this machine's core count is (the
        // mirror is min(sliceCount, vCpu), and the "none" mode forces 1 too), so the expected A below is a
        // function of the multiplier alone.
        Settings nodeSettings = Settings.builder()
            .put(DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey(), 2)
            .put(MULTIPLIER_KEY, 3.0)
            .put(SHARD_REQUEST_CAP_KEY, 5)
            .put(MAX_SLICE_COUNT_KEY, 1)
            .build();
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);

        List<String> lines = runCapturingLogs(executor(stub, nodeSettings, MULTIPLIER_COPY, SHARD_REQUEST_CAP_COPY), plans, K_EFF_TOKEN);

        assertEquals(1, lines.size());
        Map<String, String> fields = parseKEffLine(lines.get(0));
        int vCpu = Runtime.getRuntime().availableProcessors();
        int expectedA = Math.max(1, (int) Math.floor(vCpu * 3.0 / 1));
        // No cluster-state snapshot on this request, so the shard layout is the neutral 1 and F is 1.
        assertEquals("2", fields.get("K_setting"));
        assertEquals(String.valueOf(expectedA), fields.get("A"));
        assertEquals("1", fields.get("F"));
        assertEquals("the gate term must be present, not absent", String.valueOf(expectedA), fields.get("K_gate"));
        assertEquals("the mock SEARCH executor has no readable size", "absent", fields.get("K_search"));
        assertEquals("3", fields.get("n"));
        assertEquals(String.valueOf(Math.min(2, expectedA)), fields.get("K_eff"));
    }

    /**
     * The D-side half of the "absent is not 1.0" contract: with no backend declaring the multiplier the
     * gate term is <b>dropped</b>, which the line reports as {@code absent}. A synthesised 1.0 would be
     * indistinguishable from a configured 1.0 and would clamp the width instead.
     */
    public void testEmptyMultiplierSetsGateTermAbsentNotOne() throws Exception {
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);

        List<String> lines = runCapturingLogs(executor(stub, 2), plans, K_EFF_TOKEN);

        assertEquals(1, lines.size());
        Map<String, String> fields = parseKEffLine(lines.get(0));
        assertEquals("absent", fields.get("K_gate"));
        assertNotEquals("a dropped term must never render as a number", "1", fields.get("K_gate"));
        assertEquals("dropping the term must not narrow the width", "2", fields.get("K_eff"));
    }

    /**
     * Read once per query, not once per plan: the gate inputs are dynamic settings, so they are read at the
     * decision site — but a read inside the fan-out loop would multiply the cost by the plan count and make
     * the reported width disagree with the one that ran.
     */
    public void testGateInputsReadOncePerQueryNotPerPlan() throws Exception {
        AtomicInteger parses = new AtomicInteger();
        Setting<Double> countingMultiplier = new Setting<>(MULTIPLIER_KEY, "1.5", raw -> {
            parses.incrementAndGet();
            return Double.parseDouble(raw);
        }, Setting.Property.NodeScope, Setting.Property.Dynamic);

        Settings nodeSettings = Settings.builder().put(DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey(), 2).build();
        ClusterSettings registry = registry(nodeSettings, countingMultiplier, SHARD_REQUEST_CAP_COPY);
        DslGateInputs gateInputs = new DslGateInputs(registry);

        // Calibrated rather than hardcoded: the cost of ONE accessor call on this registry, whatever the
        // settings machinery does internally.
        parses.set(0);
        gateInputs.fragmentExecutorMultiplier();
        int perRead = parses.get();
        assertTrue("the counting parser must actually be exercised", perRead > 0);

        QueryPlans plans = plans(4);
        Stub stub = new Stub(plans);
        parses.set(0);
        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        executor(stub, nodeSettings, registry, gateInputs).execute(plans, null, "test-index", future);
        assertEquals(4, future.actionGet().size());

        assertEquals("the multiplier must be read once for the whole query, not once per plan", perRead, parses.get());
    }

    // ── D2.5: the shard-layout input, wired ─────────────────────────────────

    /**
     * The wiring of the busiest-node shard count into the width, which {@link CoordinatorShardLayoutTests}
     * covers only as a standalone function. Without a test that gets <em>here</em>, replacing the whole
     * routing read with {@code return 1} keeps every other test in this class green, because they all hand
     * the executor a null cluster-state snapshot and therefore take the neutral fallback.
     */
    public void testShardLayoutTermIsReadFromLiveRoutingNotAConstant() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey(), 2)
            .put(SHARD_REQUEST_CAP_KEY, 5)
            .build();
        ClusterState skewed = ShardLayouts.clusterState(ROUTED_INDEX, List.of("node-0", "node-0", "node-0", "node-0", "node-1", "node-1"));

        QueryPlans plans = plans(3);
        List<String> lines = runCapturingLogs(
            executor(new Stub(plans), nodeSettings, SHARD_REQUEST_CAP_COPY),
            plans,
            K_EFF_TOKEN,
            skewed,
            ROUTED_INDEX
        );
        assertEquals(1, lines.size());
        assertEquals(
            "F must be the busiest node's shard count from live routing — not 1 (the no-snapshot fallback), "
                + "not 6 (the index's shard count) and not 2 (the other node's share)",
            "4",
            parseKEffLine(lines.get(0)).get("F")
        );

        QueryPlans control = plans(3);
        List<String> withoutSnapshot = runCapturingLogs(
            executor(new Stub(control), nodeSettings, SHARD_REQUEST_CAP_COPY),
            control,
            K_EFF_TOKEN,
            null,
            ROUTED_INDEX
        );
        assertEquals(1, withoutSnapshot.size());
        assertEquals(
            "with no snapshot to read there is nothing to divide by, so the neutral 1 stands",
            "1",
            parseKEffLine(withoutSnapshot.get(0)).get("F")
        );
    }

    /**
     * A resolved index name is one of the two things the routing read needs, so a query that arrives without
     * one must degrade to the neutral fallback rather than fail the search on an advisory input.
     */
    public void testShardLayoutFallsBackToOneWhenTheIndexNameIsAbsent() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey(), 2)
            .put(SHARD_REQUEST_CAP_KEY, 5)
            .build();
        ClusterState skewed = ShardLayouts.clusterState(ROUTED_INDEX, List.of("node-0", "node-0", "node-0", "node-0"));

        QueryPlans plans = plans(3);
        List<String> lines = runCapturingLogs(
            executor(new Stub(plans), nodeSettings, SHARD_REQUEST_CAP_COPY),
            plans,
            K_EFF_TOKEN,
            skewed,
            null
        );

        assertEquals(1, lines.size());
        assertEquals("1", parseKEffLine(lines.get(0)).get("F"));
    }

    // ── The width decision never fails a search, and reads nothing it cannot use ──

    /**
     * The tenet, at the <em>composition</em> site rather than at one input: <b>a wrong fan-out width must
     * never fail a search.</b> Each input is already fail-secure on its own — {@code DslGateInputs} catches
     * per key, {@code CoordinatorShardLayout} degrades to 1 — but the composition runs inside plan 0's
     * success callback, where {@code ActionListener.wrap} routes a throw to the request's failure arm. So
     * before the catch, a read that threw here turned a search whose plan 0 had already succeeded into a
     * 500.
     *
     * <p>The throw is provoked the way it would really arrive: {@code targetPartitions()} resolves two
     * {@code :server} settings <b>typed</b>, and {@code ClusterSettings.get(Setting)} throws
     * {@code SettingsException("setting ... has not been registered")} when a key stops being registered (an
     * upgrade that moves or renames it). Unlike the multiplier read, that one is not wrapped by its own
     * producer. Note the shape the production catch has to be written for: {@code SettingsException} is an
     * {@code OpenSearchException}, i.e. a {@code RuntimeException} but <b>not</b> an
     * {@code IllegalArgumentException}.
     *
     * <p>What makes this test discriminating is what it asserts <em>past</em> the absence of a failure: the
     * full ordered result list is delivered, and the query ran sequentially. Without the catch,
     * {@code actionGet()} rethrows the {@code IllegalArgumentException} and the test fails there.
     */
    public void testAThrowingWidthReadRunsSequentiallyInsteadOfFailingTheSearch() throws Exception {
        Settings nodeSettings = Settings.builder().put(DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey(), 2).build();
        // Registers the DSL settings only, so DslQuerySettings still works while the two :server settings
        // targetPartitions() reads typed are missing.
        ClusterSettings partial = new ClusterSettings(nodeSettings, new HashSet<>(DslQuerySettings.all()));
        DslGateInputs unreadable = new DslGateInputs(partial);
        // The premise of the test, asserted rather than assumed: this really is a throwing read.
        expectThrows(SettingsException.class, unreadable::targetPartitions);

        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        DslQueryPlanExecutor executor = executor(stub, nodeSettings, partial, unreadable);

        List<String> lines = capturingLogs(K_EFF_TOKEN, () -> executor.execute(plans, null, "test-index", future));

        assertPlanOrder(plans, future.actionGet());
        assertEquals("the degraded path is the sequential one", 1, stub.highWater.get());
        assertEquals(List.of(0, 1, 2), stub.dispatchOrder());

        assertEquals("a query that degraded is still measured", 1, lines.size());
        Map<String, String> fields = parseKEffLine(lines.get(0));
        assertEquals("the setting is read before the terms, so it is still reportable", "2", fields.get("K_setting"));
        for (String term : List.of("A", "F", "K_gate", "K_search")) {
            assertEquals("a term whose read threw must not render as a number or as absent", "unavailable", fields.get(term));
        }
        assertEquals("1", fields.get("K_eff"));
    }

    /**
     * The same degrade for the other unguarded read at that site: {@code threadPool.executor(SEARCH)} throws
     * if that pool is not registered.
     */
    public void testAThrowingSearchPoolReadRunsSequentially() {
        Settings nodeSettings = Settings.builder().put(DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey(), 2).build();
        ClusterSettings clusterSettings = registry(nodeSettings);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any())).thenThrow(new IllegalArgumentException("no executor service found for [search]"));

        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        executor(stub, clusterService(nodeSettings, clusterSettings), new DslGateInputs(clusterSettings), threadPool).execute(
            plans,
            null,
            "test-index",
            future
        );

        assertPlanOrder(plans, future.actionGet());
        assertEquals(1, stub.highWater.get());
    }

    /**
     * At the shipped default the expensive inputs are <b>not read at all</b>. {@code K_eff} is 1 whatever
     * they say once {@code K_setting} is 1, and reading them costs an {@code OperationRouting.searchShards}
     * over every shard of the index plus four settings lookups on the query hot path, all discarded.
     */
    public void testWidthTermsAreNotReadAtTheShippedDefault() throws Exception {
        AtomicInteger parses = new AtomicInteger();
        Setting<Double> countingMultiplier = new Setting<>(MULTIPLIER_KEY, "1.5", raw -> {
            parses.incrementAndGet();
            return Double.parseDouble(raw);
        }, Setting.Property.NodeScope, Setting.Property.Dynamic);
        Settings nodeSettings = Settings.builder()
            .put(DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey(), 1)
            .put(SHARD_REQUEST_CAP_KEY, 5)
            .build();
        ClusterSettings clusterSettings = registry(nodeSettings, countingMultiplier, SHARD_REQUEST_CAP_COPY);
        ClusterService clusterService = clusterService(nodeSettings, clusterSettings);

        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        DslQueryPlanExecutor executor = executor(stub, clusterService, new DslGateInputs(clusterSettings), mockThreadPool());
        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        ClusterState placed = ShardLayouts.clusterState(ROUTED_INDEX, List.of("node-0", "node-0", "node-1"));

        parses.set(0);
        List<String> lines = capturingLogs(K_EFF_TOKEN, () -> executor.execute(plans, placed, ROUTED_INDEX, future));

        assertPlanOrder(plans, future.actionGet());
        verify(clusterService, never()).operationRouting();
        assertEquals("no gate setting may be read once K_setting has already settled the width", 0, parses.get());

        assertEquals(1, lines.size());
        Map<String, String> fields = parseKEffLine(lines.get(0));
        assertEquals("1", fields.get("K_setting"));
        for (String term : List.of("A", "F", "K_gate", "K_search")) {
            assertEquals(
                "an unread term must render as skipped — `absent` means READ AND DROPPED, the opposite claim",
                "skipped",
                fields.get(term)
            );
        }
        assertEquals("1", fields.get("K_eff"));
    }

    /** The other early-out: above the inline-drain bound the width is 1, so the terms are not read either. */
    public void testWidthTermsAreNotReadAboveThePlanBound() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey(), 2)
            .put(SHARD_REQUEST_CAP_KEY, 5)
            .build();
        ClusterSettings clusterSettings = registry(nodeSettings, SHARD_REQUEST_CAP_COPY);
        ClusterService clusterService = clusterService(nodeSettings, clusterSettings);

        QueryPlans plans = plans(SubPlanParallelism.MAX_FANOUT_PLANS + 2);
        Stub stub = new Stub(plans);
        DslQueryPlanExecutor executor = executor(stub, clusterService, new DslGateInputs(clusterSettings), mockThreadPool());
        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        ClusterState placed = ShardLayouts.clusterState(ROUTED_INDEX, List.of("node-0", "node-0", "node-1"));

        List<String> lines = capturingLogs(K_EFF_TOKEN, () -> executor.execute(plans, placed, ROUTED_INDEX, future));

        assertPlanOrder(plans, future.actionGet());
        verify(clusterService, never()).operationRouting();
        assertEquals(1, lines.size());
        Map<String, String> fields = parseKEffLine(lines.get(0));
        assertEquals("2", fields.get("K_setting"));
        assertEquals("skipped", fields.get("F"));
        assertEquals("1", fields.get("K_eff"));
    }

    // ── D4.4: SC-10, the observable width ───────────────────────────────────

    public void testKEffLineEmittedExactlyOncePerQuery() throws Exception {
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);

        List<String> lines = runCapturingLogs(executor(stub, 2), plans, K_EFF_TOKEN);

        assertEquals("exactly one width line per multi-plan query", 1, lines.size());
    }

    /**
     * The line's field set and order, pinned as a contract: seven fields, these names, these positions, and
     * the line <em>ends</em> after {@code K_eff}. A scraper anchored to the leading
     * {@code dsl.fanout.k_eff} token depends on all of it, so a reordering or an appended field fails here.
     */
    public void testKEffLineCarriesEveryContractFieldInOrder() throws Exception {
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);

        List<String> lines = runCapturingLogs(executor(stub, 2), plans, K_EFF_TOKEN);

        assertEquals(1, lines.size());
        String line = lines.get(0);
        assertTrue(
            "the eight fields must appear in contract order, got: " + line,
            Pattern.compile(
                "^"
                    + Pattern.quote(K_EFF_TOKEN)
                    + " K_setting=(\\S+) A=(\\S+) F=(\\S+) K_gate=(\\S+) K_search=(\\S+) n=(\\S+) K_eff=(\\S+)$"
            ).matcher(line).matches()
        );
        Map<String, String> fields = parseKEffLine(line);
        assertEquals("2", fields.get("K_setting"));
        assertEquals("1", fields.get("F"));
        assertEquals("3", fields.get("n"));
        assertEquals("2", fields.get("K_eff"));
    }

    public void testKEffLineRendersDroppedTermsAsAbsent() throws Exception {
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);

        List<String> lines = runCapturingLogs(executor(stub, 2), plans, K_EFF_TOKEN);

        assertEquals(1, lines.size());
        Map<String, String> fields = parseKEffLine(lines.get(0));
        assertEquals("absent", fields.get("K_gate"));
        assertEquals("absent", fields.get("K_search"));
        assertNotEquals("1", fields.get("K_gate"));
        assertNotEquals("1", fields.get("K_search"));
    }

    /** A query too wide to fan out is still measured — otherwise it looks unmeasured, not unfanned. */
    public void testKEffLineEmittedOnSequentialFallback() throws Exception {
        QueryPlans plans = plans(64);
        Stub stub = new Stub(plans);

        List<String> lines = runCapturingLogs(executor(stub, 2), plans, K_EFF_TOKEN);

        assertEquals(1, lines.size());
        Map<String, String> fields = parseKEffLine(lines.get(0));
        assertEquals("64", fields.get("n"));
        assertEquals("1", fields.get("K_eff"));
    }

    /**
     * A single-plan query never reaches the fan-out decision, so it emits nothing. An absent line means
     * "not a multi-plan query", never "the line was dropped" — the rollout steps read it that way.
     */
    public void testNoKEffLineForSinglePlanQuery() throws Exception {
        QueryPlans plans = plans(1);
        Stub stub = new Stub(plans);

        List<String> lines = runCapturingLogs(executor(stub, 2), plans, K_EFF_TOKEN);

        assertTrue("a single-plan query must emit no width line, got: " + lines, lines.isEmpty());
    }

    // ── D4.3: logPlan's kept set -> invalidate -> explain sequence ───────────

    /**
     * The regression pin for the NPE class this whole plugin keeps warning about: {@code RelWriterImpl}
     * asks the cluster for a metadata query while rendering, and one built on a thread whose
     * {@code THREAD_PROVIDERS} ThreadLocal is unset NPEs. {@code logPlan} is safe only because it sets that
     * ThreadLocal itself, on the dispatching thread, before it explains.
     */
    @TestLogging(reason = DEBUG_REASON, value = DEBUG_LOGGING)
    public void testLogPlanOnColdThreadDoesNotNpe() throws Exception {
        QueryPlans plans = nestedAggregationPlans();
        assertTrue("the fixture must produce a fan-out, got " + plans.getAll().size() + " plan(s)", plans.getAll().size() >= 2);

        Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
        Queue<Thread> threads = new ConcurrentLinkedQueue<>();
        DslQueryPlanExecutor executor = executor((plan, ctx, listener) -> {
            // A genuinely fresh thread, never a pooled one: a pooled thread may already be primed.
            Thread thread = new Thread(() -> {
                try {
                    RelMetadataQueryBase.THREAD_PROVIDERS.remove();
                    assertNull("the dispatching thread must start cold", RelMetadataQueryBase.THREAD_PROVIDERS.get());
                    listener.onResponse(List.<Object[]>of());
                } catch (Throwable t) {
                    failures.add(t);
                }
            }, "cold-dispatch");
            threads.add(thread);
            thread.start();
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }, 2);

        CapturingListener listener = new CapturingListener();
        RelMetadataQueryBase.THREAD_PROVIDERS.remove();
        assertNull(RelMetadataQueryBase.THREAD_PROVIDERS.get());
        executor.execute(plans, null, "test-index", listener);

        for (Thread thread : threads) {
            thread.join();
        }
        if (failures.isEmpty() == false) {
            throw new AssertionError("plan logging failed on a cold thread", failures.peek());
        }
        assertNull("no failure may reach the listener: " + failures, listener.failure);
        assertNotNull(listener.results);
        assertEquals(plans.getAll().size(), listener.results.size());
    }

    /**
     * The guard-regression pin, and the only mechanical protection for the cost decision: at INFO the block
     * must not execute at all, so the ThreadLocal it would have written must still be unset afterwards.
     * <b>This is the test that fails if the guard is reverted to {@code isInfoEnabled()}.</b>
     */
    public void testLogPlanIsOffAtInfoLevel() throws Exception {
        QueryPlans plans = nestedAggregationPlans();
        Stub stub = new Stub(plans);

        RelMetadataQueryBase.THREAD_PROVIDERS.remove();
        assertNull(RelMetadataQueryBase.THREAD_PROVIDERS.get());
        List<String> planLines = runCapturingLogs(executor(stub, 2), plans, PLAN_LOG_MARKER);

        assertNull("at INFO the plan-log block must not run", RelMetadataQueryBase.THREAD_PROVIDERS.get());
        assertTrue("at INFO nothing may be logged, got: " + planLines, planLines.isEmpty());
    }

    /**
     * The ThreadLocal must not outlive the call. {@code fanOut} is reached from plan 0's completion
     * callback, so {@code logPlan} runs on a pooled ENGINE/TRANSPORT thread: a bare
     * {@code THREAD_PROVIDERS.set} leaves a {@code JaninoRelMetadataProvider} — and transitively this
     * query's {@code RelOptCluster}, {@code SchemaPlus} and {@code ClusterState} — pinned to a shared thread
     * after the request ends, where a later unrelated query can observe it.
     *
     * <p>The stub completes inline, so every plan of this query is dispatched on the test thread and the
     * assertion is about the thread that really ran {@code logPlan}. It is genuinely failable: with the
     * {@code set} unpaired the ThreadLocal is still populated here.
     */
    @TestLogging(reason = DEBUG_REASON, value = DEBUG_LOGGING)
    public void testLogPlanLeavesNoProviderPinnedToTheDispatchingThread() throws Exception {
        QueryPlans plans = nestedAggregationPlans();
        Stub stub = new Stub(plans);

        RelMetadataQueryBase.THREAD_PROVIDERS.remove();
        assertNull(RelMetadataQueryBase.THREAD_PROVIDERS.get());
        List<String> planLines = runCapturingLogs(executor(stub, 2), plans, PLAN_LOG_MARKER);

        // Without this the assertion below could pass on a query that never logged a plan at all.
        assertFalse("the plan-log block must have run, or this proves nothing", planLines.isEmpty());
        assertNull("logPlan must not leave a metadata provider pinned to a pooled thread", RelMetadataQueryBase.THREAD_PROVIDERS.get());
    }

    /**
     * The other half of the restore: a thread that arrives with its own provider set — the engine's own
     * planning threads do — must leave with the same one, not with this query's and not with none.
     */
    @TestLogging(reason = DEBUG_REASON, value = DEBUG_LOGGING)
    public void testLogPlanRestoresAProviderTheThreadAlreadyHad() throws Exception {
        QueryPlans plans = nestedAggregationPlans();
        Stub stub = new Stub(plans);
        JaninoRelMetadataProvider caller = JaninoRelMetadataProvider.of(TestUtils.createTestRelNode().getCluster().getMetadataProvider());

        JaninoRelMetadataProvider after;
        RelMetadataQueryBase.THREAD_PROVIDERS.set(caller);
        try {
            List<String> planLines = runCapturingLogs(executor(stub, 2), plans, PLAN_LOG_MARKER);
            assertFalse("the plan-log block must have run, or this proves nothing", planLines.isEmpty());
            after = RelMetadataQueryBase.THREAD_PROVIDERS.get();
        } finally {
            RelMetadataQueryBase.THREAD_PROVIDERS.remove();
        }
        assertSame("the caller's own provider must be restored, not removed and not replaced", caller, after);
    }

    /**
     * The hard invariant: {@code logPlan(plan)} runs on the same thread as, and strictly before, the
     * dispatch of that <em>same</em> plan. Asserted per plan rather than "does it log at all" — a later
     * refactor that moved the call into a completion callback, or out of the gate's runnable, would still
     * log the same number of lines.
     */
    @TestLogging(reason = DEBUG_REASON, value = DEBUG_LOGGING)
    public void testLogPlanPrecedesDispatchForEachPlan() throws Exception {
        QueryPlans plans = nestedAggregationPlans();
        int n = plans.getAll().size();
        Stub stub = new Stub(plans);
        AtomicInteger planLogsSeen = new AtomicInteger();
        stub.planLogsSeen = planLogsSeen;

        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(DslQueryPlanExecutor.class))) {
            appender.addExpectation(new CountingExpectation(PLAN_LOG_MARKER, planLogsSeen));
            CapturingListener listener = new CapturingListener();
            executor(stub, 2).execute(plans, null, "test-index", listener);
            assertNotNull("the fan-out must complete: " + listener.failure, listener.results);
        }

        assertEquals("every plan must have been dispatched", n, stub.logsSeenAtEntry.size());
        for (Map.Entry<Integer, Integer> entry : stub.logsSeenAtEntry.entrySet()) {
            assertEquals(
                "plan " + entry.getKey() + " must be dispatched after its own plan-log line and before the next plan's",
                Integer.valueOf(stub.dispatchOrder().indexOf(entry.getKey()) + 1),
                entry.getValue()
            );
        }
        assertEquals("one plan-log line per plan", n, planLogsSeen.get());
    }

    /**
     * {@code logPlan} is a throwing call — at DEBUG it dereferences the plan's metadata provider and
     * renders the plan — so it has to sit <em>inside</em> the dispatch {@code try}, next to
     * {@code executor.execute}. Outside it, a throw escapes the gate's runnable after the permit was
     * taken: nothing releases the permit, nothing counts the plan down, the collector's terminal can never
     * fire and the request hangs; on the drain path the same throw escapes into an engine completion
     * thread through {@code finishAndRunNext}.
     *
     * <p>The discriminating assertion is that the <em>other</em> fan-out plans still ran. With the call
     * outside the {@code try} the throw unwinds the dispatch loop itself, so plans 2..4 are never
     * dispatched at all — and the terminal callback count alone would not show it, because the escape is
     * caught one frame up by plan 0's listener and reported as a failure either way.
     *
     * <p>No other test reaches this path: the stub's {@code throwInline} / {@code failInline} fire from
     * {@code execute}, which is already inside the {@code try}, and
     * {@link #testLogPlanOnColdThreadDoesNotNpe()} only asserts the happy path.
     */
    @TestLogging(reason = DEBUG_REASON, value = DEBUG_LOGGING)
    public void testPlanLogFailureIsReportedAndDoesNotStrandTheFanOut() {
        // A real plan (Mockito cannot mock Calcite classes here — see TestUtils) whose own cluster has no
        // metadata provider, so logPlan's requireNonNull throws: the same NullPointerException shape the
        // fan-out IT scrapes the node log for. createTestRelNode builds a fresh cluster per call, so this
        // cripples this plan only.
        RelNode unloggable = TestUtils.createTestRelNode();
        unloggable.getCluster().setMetadataProvider(null);
        QueryPlans.Builder builder = new QueryPlans.Builder();
        builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, scan));
        builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, unloggable));
        for (int i = 2; i < 5; i++) {
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, TestUtils.createTestRelNode()));
        }
        QueryPlans plans = builder.build();
        Stub stub = new Stub(plans);

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        assertEquals("exactly one terminal callback", 1, listener.terminalCalls);
        assertNotNull("the plan-log failure must be reported, not swallowed", listener.failure);
        assertFalse("the plan whose logging threw cannot have been dispatched", stub.dispatchOrder().contains(1));
        assertEquals("every other fan-out plan must still have been dispatched", List.of(0, 2, 3, 4), stub.dispatchOrder());
        assertEquals("no plan may be left in flight", 0, stub.inFlight.get());
    }

    /**
     * An {@code Error} out of a dispatch must still release the permit and drive the countdown. Calcite
     * throws {@code AssertionError} outright where its plan invariants are violated, so this is reachable
     * from {@code logPlan} and from the engine call itself, and a {@code catch (Exception)} would let it
     * past — stranding the permit and the countdown and leaving the REST channel open with nobody to answer
     * it. That hang is the one failure this class exists to make impossible, so it must not depend on the
     * throwable being an {@code Exception}.
     *
     * <p>The {@code Error} is deliberately not rethrown: rethrowing would abandon the dispatch loop, so the
     * plans after this one would never be dispatched and never count down — reintroducing the very hang the
     * catch prevents. The assertions below pin that trade: the request completes, and the siblings still ran.
     */
    public void testAnErrorDuringDispatchStillReleasesThePermitAndCountsDown() {
        QueryPlans plans = plans(3);
        Stub stub = new Stub(plans);
        AssertionError boom = new AssertionError("Calcite plan invariant violated");
        stub.errorInline.put(0, boom);

        CapturingListener listener = new CapturingListener();
        executor(stub, 2).execute(plans, null, "test-index", listener);

        assertEquals("the request must be completed exactly once, not hung", 1, listener.terminalCalls);
        assertNotNull("the failing plan must be reported, not swallowed", listener.failure);
        assertSame("the Error must be carried as the cause rather than lost", boom, listener.failure.getCause());
        assertEquals("no plan may be left in flight", 0, stub.inFlight.get());
        // The discriminating assertion: had the permit leaked, or had the Error escaped the loop, the
        // siblings would never have been admitted and dispatchOrder would stop at 0.
        assertEquals("the surviving plans must still have been dispatched", List.of(0, 1, 2), stub.dispatchOrder());
    }

    // ── Harness ─────────────────────────────────────────────────────────────

    /**
     * Asserts the whole slotting contract: slot {@code i} carries plan {@code i}'s <em>plan</em> and plan
     * {@code i}'s <em>payload</em>.
     */
    private static void assertPlanOrder(QueryPlans plans, List<ExecutionResult> results) {
        assertNotNull(results);
        assertEquals(plans.getAll().size(), results.size());
        for (int i = 0; i < results.size(); i++) {
            assertSame("result " + i + " must be plan " + i + "'s", plans.getAll().get(i).relNode(), results.get(i).getPlan().relNode());
            assertEquals("result " + i + " must carry plan " + i + "'s rows", Stub.tag(i), Stub.tagOf(results.get(i)));
        }
    }

    /**
     * One HITS plan plus {@code n - 1} AGGREGATION plans, each with its own distinct RelNode — which is what
     * every assertion here identifies a plan by (the stub's dispatch index and its payload tag both derive
     * from it). The plans carry no {@code AggregationMetadata}: the fan-out never reads it, and a plan is
     * only required to carry a type and a RelNode.
     */
    private QueryPlans plans(int n) {
        QueryPlans.Builder builder = new QueryPlans.Builder();
        builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, scan));
        for (int i = 1; i < n; i++) {
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, TestUtils.createTestRelNode()));
        }
        return builder.build();
    }

    /**
     * A batch with <b>no</b> aggregation plan: one HITS plan plus {@code countPlans} COUNT plans, each with
     * its own distinct RelNode, exactly as {@link #plans(int)} builds its aggregation batches. This is the
     * shape the fan-out must refuse — {@code countPlans == 1} is what a plain search converts to.
     */
    private QueryPlans hitsAndCountPlans(int countPlans) {
        QueryPlans.Builder builder = new QueryPlans.Builder();
        builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, scan));
        for (int i = 0; i < countPlans; i++) {
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.COUNT, TestUtils.createTestRelNode()));
        }
        QueryPlans plans = builder.build();
        assertFalse("the fixture must carry no aggregation plan", plans.has(QueryPlans.Type.AGGREGATION));
        assertEquals(1 + countPlans, plans.getAll().size());
        return plans;
    }

    /**
     * The multi-plan fixture the plan-logging tests need: real converter output for a nested aggregation,
     * so the plans have the shape whose {@code explain()} actually reaches Calcite's metadata handlers.
     */
    private static QueryPlans nestedAggregationPlans() throws Exception {
        return convert(new SearchSourceBuilder().size(10).aggregation(nestedTermsAggregation()));
    }

    /**
     * The measured production shape: {@code size: 0} plus a 2-level nested aggregation. Emits no HITS plan,
     * so it is also the fixture that proves the eligibility predicate is not accidentally keyed on plan 0.
     */
    private static QueryPlans sizeZeroNestedAggregationPlans() throws Exception {
        return convert(new SearchSourceBuilder().size(0).aggregation(nestedTermsAggregation()));
    }

    private static TermsAggregationBuilder nestedTermsAggregation() {
        return new TermsAggregationBuilder("by_brand").field("brand")
            .subAggregation(
                new TermsAggregationBuilder("by_name").field("name").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            );
    }

    /**
     * Real converter output for one request against the shared three-field test mapping. Used where the
     * request <em>shape</em> is the thing under test, so that the plan set is the one a real search produces
     * rather than one this class asserted into existence.
     */
    private static QueryPlans convert(SearchSourceBuilder source) throws Exception {
        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put("name", "VARCHAR");
        mapping.put("price", "INTEGER");
        mapping.put("brand", "VARCHAR");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping("test-index", mapping);
        return new SearchSourceConverter(infra.schema()).convert(source, "test-index");
    }

    // Most tests hand the executor a null cluster state, which is the no-snapshot shape: the shard-layout
    // input then degrades to the neutral 1 rather than reading a second, unrelated snapshot, so the width
    // does not depend on the machine running the test. The tests that care about the routing read build a
    // real snapshot with ShardLayouts.clusterState and pass it directly.

    /** The executor at a given width, over a real settings registry. */
    private DslQueryPlanExecutor executor(QueryPlanExecutor<RelNode, Iterable<Object[]>> stub, int kSetting) {
        return executor(stub, Settings.builder().put(DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey(), kSetting).build());
    }

    /**
     * Builds the executor over a real settings registry. The concurrency-gate multiplier is registered only
     * when a test passes its descriptor, so by default that term is absent and the width is
     * {@code min(n - 1, K_setting)} on every host.
     */
    private DslQueryPlanExecutor executor(
        QueryPlanExecutor<RelNode, Iterable<Object[]>> stub,
        Settings nodeSettings,
        Setting<?>... extras
    ) {
        ClusterSettings clusterSettings = registry(nodeSettings, extras);
        return executor(stub, nodeSettings, clusterSettings, new DslGateInputs(clusterSettings));
    }

    private DslQueryPlanExecutor executor(
        QueryPlanExecutor<RelNode, Iterable<Object[]>> stub,
        Settings nodeSettings,
        ClusterSettings clusterSettings,
        DslGateInputs gateInputs
    ) {
        return executor(stub, clusterService(nodeSettings, clusterSettings), gateInputs, mockThreadPool());
    }

    /** The widest seam: every collaborator injected, for the tests that assert on what was NOT read. */
    private DslQueryPlanExecutor executor(
        QueryPlanExecutor<RelNode, Iterable<Object[]>> stub,
        ClusterService clusterService,
        DslGateInputs gateInputs,
        ThreadPool threadPool
    ) {
        return new DslQueryPlanExecutor(stub, clusterService, threadPool, new DslQuerySettings(clusterService), gateInputs);
    }

    /**
     * The mock coordinator, returned rather than hidden so a test can {@code verify} which of its reads the
     * width decision performed.
     */
    private static ClusterService clusterService(Settings nodeSettings, ClusterSettings clusterSettings) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        // A real routing service, not a mock: the shard-layout read is only reached when the request carries a
        // cluster-state snapshot, and the test that does that has to reach the production read rather than a
        // stubbed answer. On the null-snapshot requests the rest of the class uses, this is never consulted.
        when(clusterService.operationRouting()).thenReturn(ShardLayouts.routing());
        return clusterService;
    }

    /** The server's built-in settings plus any local descriptor copies standing in for another plugin's. */
    private static ClusterSettings registry(Settings nodeSettings, Setting<?>... extras) {
        Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.addAll(DslQuerySettings.all());
        registered.addAll(Set.of(extras));
        return new ClusterSettings(nodeSettings, registered);
    }

    /**
     * A thread pool whose SEARCH executor is deliberately <em>not</em> an
     * {@code OpenSearchThreadPoolExecutor}, so the pool-size term is absent and the fan-out width does not
     * depend on the machine running the test.
     */
    private static ThreadPool mockThreadPool() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any())).thenReturn(mock(ExecutorService.class));
        return threadPool;
    }

    /** Runs one query with the class's logger captured, returning the messages carrying {@code marker}. */
    private static List<String> runCapturingLogs(DslQueryPlanExecutor executor, QueryPlans plans, String marker) throws Exception {
        return runCapturingLogs(executor, plans, marker, null, "test-index");
    }

    /**
     * The same capture around an arbitrary run, for the tests that assert on the delivered result list as
     * well as on the line — {@link #runCapturingLogs} owns its listener, so it cannot hand one back.
     */
    private static List<String> capturingLogs(String marker, CheckedRunnable<Exception> query) throws Exception {
        List<String> captured = Collections.synchronizedList(new ArrayList<>());
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(DslQueryPlanExecutor.class))) {
            appender.addExpectation(new CapturingExpectation(marker, captured));
            query.run();
        }
        return captured;
    }

    /** The same, for a query whose cluster-state snapshot and index name matter (the shard-layout read). */
    private static List<String> runCapturingLogs(
        DslQueryPlanExecutor executor,
        QueryPlans plans,
        String marker,
        ClusterState state,
        String concreteIndex
    ) throws Exception {
        List<String> captured = Collections.synchronizedList(new ArrayList<>());
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(DslQueryPlanExecutor.class))) {
            appender.addExpectation(new CapturingExpectation(marker, captured));
            PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
            executor.execute(plans, state, concreteIndex, future);
            assertEquals(plans.getAll().size(), future.actionGet().size());
        }
        return captured;
    }

    private static Map<String, String> parseKEffLine(String line) {
        Map<String, String> fields = new LinkedHashMap<>();
        Matcher matcher = Pattern.compile("(\\w+)=(\\S+)").matcher(line);
        while (matcher.find()) {
            fields.put(matcher.group(1), matcher.group(2));
        }
        // Seven fields, in the contract order testKEffLineCarriesEveryContractFieldInOrder pins.
        // Pinned as a count so a field cannot be silently dropped or added without a test saying so.
        assertEquals("the line must carry exactly the seven contract fields: " + line, 7, fields.size());
        return fields;
    }

    /**
     * Captures every message carrying a marker. A counting/capturing expectation is required because the
     * framework's built-in {@code SeenEventExpectation} records only <em>whether</em> an event was seen, so
     * it would pass on three width lines as happily as on one.
     */
    private static class CapturingExpectation implements MockLogAppender.LoggingExpectation {

        private final String marker;
        private final List<String> captured;

        CapturingExpectation(String marker, List<String> captured) {
            this.marker = marker;
            this.captured = captured;
        }

        @Override
        public void match(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            if (message.contains(marker)) {
                captured.add(message);
            }
        }

        @Override
        public void assertMatched() {
            // Nothing to assert here: the count and the contents are what the tests assert, and an
            // expectation that failed on "no events" would make the INFO-level test unwritable.
        }
    }

    /** Counts messages carrying a marker, so the count can be read while the query is still running. */
    private static class CountingExpectation implements MockLogAppender.LoggingExpectation {

        private final String marker;
        private final AtomicInteger seen;

        CountingExpectation(String marker, AtomicInteger seen) {
            this.marker = marker;
            this.seen = seen;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getMessage().getFormattedMessage().contains(marker)) {
                seen.incrementAndGet();
            }
        }

        @Override
        public void assertMatched() {}
    }

    /**
     * Stub engine with in-flight accounting. Per-plan behaviour is configured by index before the run:
     * complete inline (the default), park the listener for the test to complete later, fail inline, throw
     * out of the dispatch, or complete <em>and then</em> throw.
     */
    private static class Stub implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

        private final List<RelNode> planOrder;
        private final List<Integer> dispatched = Collections.synchronizedList(new ArrayList<>());
        private final Map<Integer, ActionListener<Iterable<Object[]>>> parked = new ConcurrentHashMap<>();

        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger highWater = new AtomicInteger();
        /** Monotonic stamp source, so orderings can be asserted without sleeping. */
        final AtomicInteger sequence;
        /** Plan-log lines already emitted when each plan's dispatch was entered. */
        final Map<Integer, Integer> logsSeenAtEntry = new ConcurrentHashMap<>();

        /** Park every plan's listener rather than completing it. */
        boolean deferAll;
        /** Park only these plans' listeners. */
        final Set<Integer> defer = Collections.synchronizedSet(new HashSet<>());
        final Map<Integer, RuntimeException> failInline = new ConcurrentHashMap<>();
        final Map<Integer, RuntimeException> throwInline = new ConcurrentHashMap<>();
        /** Like {@code throwInline}, but an {@code Error} — the dispatch path must clean up for those too. */
        final Map<Integer, Error> errorInline = new ConcurrentHashMap<>();
        final Set<Integer> completeThenThrow = Collections.synchronizedSet(new HashSet<>());
        AtomicInteger planLogsSeen;

        Stub(QueryPlans plans) {
            this(plans, new AtomicInteger());
        }

        Stub(QueryPlans plans, AtomicInteger sequence) {
            this.planOrder = plans.getAll().stream().map(QueryPlans.QueryPlan::relNode).toList();
            this.sequence = sequence;
        }

        List<Integer> dispatchOrder() {
            return List.copyOf(dispatched);
        }

        @Override
        public void execute(RelNode plan, QueryRequestContext ctx, ActionListener<Iterable<Object[]>> listener) {
            int index = planOrder.indexOf(plan);
            assertTrue("the stub was handed a RelNode that is not one of the query's plans", index >= 0);
            dispatched.add(index);
            sequence.incrementAndGet();
            if (planLogsSeen != null) {
                logsSeenAtEntry.put(index, planLogsSeen.get());
            }
            int now = inFlight.incrementAndGet();
            highWater.updateAndGet(previous -> Math.max(previous, now));

            RuntimeException thrown = throwInline.get(index);
            if (thrown != null) {
                inFlight.decrementAndGet();
                throw thrown;
            }
            Error error = errorInline.get(index);
            if (error != null) {
                inFlight.decrementAndGet();
                throw error;
            }
            if (completeThenThrow.contains(index)) {
                // The shape only this configuration produces, and it overrides deferral because the point is
                // that the throw lands on an ALREADY-completed listener: the loop's catch then notifies it a
                // second time.
                inFlight.decrementAndGet();
                listener.onResponse(rows(index));
                throw new IllegalStateException("plan " + index + " completed and then threw");
            }
            RuntimeException failure = failInline.get(index);
            if (failure != null) {
                inFlight.decrementAndGet();
                listener.onFailure(failure);
                return;
            }
            if (deferAll || defer.contains(index)) {
                parked.put(index, listener);
                return;
            }
            inFlight.decrementAndGet();
            listener.onResponse(rows(index));
        }

        void completeParked(int index) {
            ActionListener<Iterable<Object[]>> listener = parked.remove(index);
            assertNotNull("plan " + index + " was not parked", listener);
            inFlight.decrementAndGet();
            sequence.incrementAndGet();
            listener.onResponse(rows(index));
        }

        void completeAnyParked() {
            Optional<Integer> next = parked.keySet().stream().findFirst();
            assertTrue("nothing left to complete", next.isPresent());
            completeParked(next.get());
        }

        /**
         * Takes one parked plan and completes it, or reports that none is parked — the multi-threaded form of
         * {@link #completeAnyParked()}. The take has to be the {@code remove}, not the {@code keySet} read:
         * two drainers that both saw the same key would otherwise both complete that plan's listener, which
         * would inject the very double-notification the fan-out is supposed to survive and make the test
         *
         * @return true if a plan was completed, false if nothing was parked at that moment
         */
        boolean completeAnyParkedIfPresent() {
            for (Integer index : parked.keySet()) {
                ActionListener<Iterable<Object[]>> listener = parked.remove(index);
                if (listener == null) {
                    continue;   // another drainer took this one first
                }
                inFlight.decrementAndGet();
                sequence.incrementAndGet();
                listener.onResponse(rows(index));
                return true;
            }
            return false;
        }

        private static List<Object[]> rows(int index) {
            return List.<Object[]>of(new Object[] { tag(index) });
        }

        /** The payload tag this stub produces for a plan — unique per plan, so a mis-slot is visible. */
        static String tag(int index) {
            return String.format(Locale.ROOT, "plan-%d", index);
        }

        /** The payload tag carried by a delivered result, i.e. which plan's rows actually landed in a slot. */
        static String tagOf(ExecutionResult result) {
            Iterator<Object[]> rows = result.getRows().iterator();
            assertTrue("a delivered result carried no rows at all", rows.hasNext());
            Object[] row = rows.next();
            assertFalse("the fixture emits exactly one row per plan", rows.hasNext());
            return (String) row[0];
        }
    }

    /**
     * A request listener that throws out of {@code onResponse} — the shape a downstream response builder
     * takes when it fails on results it has just been handed. It counts its own notifications so a
     * conversion of that throw into a plan failure is visible as a second one.
     */
    private static class ThrowingListener implements ActionListener<List<ExecutionResult>> {

        private int terminalCalls;
        private int failureCalls;

        @Override
        public void onResponse(List<ExecutionResult> results) {
            terminalCalls++;
            throw new IllegalStateException("the request listener's own failure");
        }

        @Override
        public void onFailure(Exception e) {
            terminalCalls++;
            failureCalls++;
        }
    }

    /** Records the single terminal callback, so a double completion or a silent success is visible. */
    private static class CapturingListener implements ActionListener<List<ExecutionResult>> {

        /** Shared with the stub when a test needs to order the terminal against a plan's completion. */
        private final AtomicInteger stamps;

        private List<ExecutionResult> results;
        private Exception failure;
        private int terminalCalls;
        private int terminalStamp;

        CapturingListener() {
            this(new AtomicInteger());
        }

        CapturingListener(AtomicInteger stamps) {
            this.stamps = stamps;
        }

        @Override
        public void onResponse(List<ExecutionResult> executionResults) {
            terminalCalls++;
            terminalStamp = stamps.incrementAndGet();
            this.results = executionResults;
        }

        @Override
        public void onFailure(Exception e) {
            terminalCalls++;
            terminalStamp = stamps.incrementAndGet();
            this.failure = e;
        }
    }
}

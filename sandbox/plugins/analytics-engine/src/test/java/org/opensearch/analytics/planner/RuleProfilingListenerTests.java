/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;

import java.util.List;
import java.util.Map;

/**
 * Drives {@link PlannerImpl#runAllOptimizations} with profiling enabled across multiple
 * SQL query shapes and asserts on the resulting {@link RuleProfilingListener.PlannerProfile}:
 * phases ran in order, durations are non-negative, the rule-set matches exactly, and each
 * rule's production count matches the expected value.
 *
 * <p>Plan-shape correctness is intentionally <strong>not</strong> asserted here — that's
 * {@code *PlanShapeTests}' job. These tests only validate the profiling listener.
 *
 * <p>Productions (count of successful {@code transformTo} calls) are deterministic enough
 * to assert exactly. Attempts and elapsed time depend on Volcano's exploration order and
 * are only sanity-checked ({@code productions <= attempts}, {@code totalNanos >= 0}).
 */
public class RuleProfilingListenerTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(RuleProfilingListenerTests.class);

    private static final List<String> EXPECTED_PHASES = List.of("calcite-core-rules", "aggregate-decompose", "marking", "cbo");

    public void testProfilePureScan() {
        runAndAssertRules(
            1,
            "SELECT URL FROM hits",
            Map.of(
                "ReduceExpressionsRule(Project)",
                0L,
                "OpenSearchProjectRule",
                1L,
                "OpenSearchTableScanRule",
                1L,
                "ExpandConversionRule",
                1L
            )
        );
    }

    public void testProfileFilterOverScan() {
        runAndAssertRules(
            1,
            "SELECT URL FROM hits WHERE CounterID = 100",
            Map.of(
                "ReduceExpressionsRule(Filter)",
                0L,
                "ReduceExpressionsRule(Project)",
                0L,
                "OpenSearchFilterRule",
                1L,
                "OpenSearchProjectRule",
                1L,
                "OpenSearchTableScanRule",
                1L,
                "ExpandConversionRule",
                1L
            )
        );
    }

    public void testProfileAggregateOverFilterMultiShard() {
        runAndAssertRules(
            5,
            "SELECT CounterID, SUM(ParamPrice) AS total FROM hits WHERE AdvEngineID = 5 GROUP BY CounterID",
            Map.ofEntries(
                Map.entry("ReduceExpressionsRule(Filter)", 0L),
                Map.entry("ReduceExpressionsRule(Project)", 0L),
                Map.entry("OpenSearchFilterRule", 1L),
                Map.entry("OpenSearchProjectRule", 1L),
                Map.entry("OpenSearchTableScanRule", 1L),
                Map.entry("OpenSearchAggregateRule", 1L),
                Map.entry("OpenSearchAggregateSplitRule", 4L),
                Map.entry("OpenSearchDistributionDeriveRule", 3L),
                Map.entry("ExpandConversionRule", 5L)
            )
        );
    }

    /** Self-join with aggregate on top — exercises {@code OpenSearchJoinRule} + {@code OpenSearchJoinSplitRule}. */
    public void testProfileJoinWithAggregateMultiShard() {
        runAndAssertRules(
            5,
            "SELECT l.CounterID, COUNT(*) AS cnt FROM hits l JOIN hits r ON l.CounterID = r.CounterID GROUP BY l.CounterID",
            Map.of(
                "ReduceExpressionsRule(Project)",
                0L,
                "OpenSearchTableScanRule",
                1L,
                "OpenSearchProjectRule",
                1L,
                "OpenSearchJoinRule",
                1L,
                "OpenSearchAggregateRule",
                1L,
                "OpenSearchAggregateSplitRule",
                1L,
                "OpenSearchJoinSplitRule",
                1L,
                "ExpandConversionRule",
                2L
            )
        );
    }

    public void testProfilingDisabledLeavesContextNull() {
        ClusterState state = clickBenchClusterState(1);
        PlannerContext context = context(state, false);
        RelNode parsed = SqlPlannerTestFixture.parseSql("SELECT URL FROM hits", state);

        PlannerImpl.runAllOptimizations(parsed, context);

        assertNull("Profiling disabled — getProfilingResults() must return null", context.getProfilingResults());
    }

    // ---- Shared assertion helper ----

    /**
     * Runs the SQL through {@link PlannerImpl#runAllOptimizations} with profiling enabled,
     * then asserts:
     * <ul>
     *   <li>All four optimization phases ran in declared order with non-negative durations.</li>
     *   <li>The set of fired rules matches {@code expectedProductionsByRule.keySet()} exactly.</li>
     *   <li>Each rule's {@code productions} equals the expected value in the map.</li>
     *   <li>Per-rule sanity: {@code attempts > 0}, {@code productions <= attempts},
     *       {@code totalNanos >= 0}.</li>
     * </ul>
     */
    private void runAndAssertRules(int shardCount, String sql, Map<String, Long> expectedProductionsByRule) {
        ClusterState state = clickBenchClusterState(shardCount);
        PlannerContext context = context(state, true);
        RelNode parsed = SqlPlannerTestFixture.parseSql(sql, state);

        PlannerImpl.runAllOptimizations(parsed, context);

        RuleProfilingListener.PlannerProfile profile = context.getProfilingResults();
        assertNotNull("Profiling enabled — profile must be recorded", profile);

        assertEquals("All four optimization phases must run in declared order", EXPECTED_PHASES, profile.phases());

        for (String phase : EXPECTED_PHASES) {
            Long durationNs = profile.phaseDurationsNs().get(phase);
            assertNotNull("Phase '" + phase + "' must have a duration recorded", durationNs);
            assertTrue("Phase '" + phase + "' duration must be non-negative", durationNs >= 0);
        }

        // Rule-set match.
        assertEquals("Profile rule-set must match expected rule-set exactly", expectedProductionsByRule.keySet(), profile.rules().keySet());

        // Per-rule productions + sanity.
        profile.rules().forEach((ruleName, metrics) -> {
            long expectedProductions = expectedProductionsByRule.get(ruleName);
            assertEquals("Rule '" + ruleName + "' production count must match expected", expectedProductions, metrics.productions());
            assertTrue("Rule '" + ruleName + "' attempts must be > 0", metrics.attempts() > 0);
            assertTrue("Rule '" + ruleName + "' productions must be <= attempts", metrics.productions() <= metrics.attempts());
            assertTrue("Rule '" + ruleName + "' totalNanos must be >= 0", metrics.totalNanos() >= 0);
        });

        LOGGER.info("Profile:\n{}", profile.format());
    }

    // ---- Context wiring ----

    private static ClusterState clickBenchClusterState(int shardCount) {
        return SqlPlannerTestFixture.clusterStateWith(ClickBench.INDEX, ClickBench.BASIC_FIELDS, "parquet", shardCount);
    }

    private PlannerContext context(ClusterState state, boolean profilingEnabled) {
        return new PlannerContext(new CapabilityRegistry(List.of(DATAFUSION, LUCENE), FieldStorageResolver::new), state, profilingEnabled);
    }
}

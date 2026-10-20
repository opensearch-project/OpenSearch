/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats;

import org.opensearch.analytics.exec.ExecutionGraph;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageMetrics;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Node-local rollup of analytics-engine query, stage, and fragment timings.
 * {@code DefaultPlanExecutor} calls {@link #recordExecution} once per query
 * terminal — feeding the raw counters straight off the {@link ExecutionGraph}
 * without allocating an intermediate
 * {@link org.opensearch.analytics.exec.profile.QueryProfile}. The {@code _explain}
 * payload still uses {@code QueryProfileBuilder.snapshot()}; the stats path
 * intentionally avoids it so the hot path stays allocation-free.
 *
 * <p>{@link #snapshot()} returns an immutable {@link AnalyticsStats} for the
 * REST endpoint to render. Recording is wait-free: counters and latency totals
 * are {@link LongAdder}s, the per-stage-type bucket map is a
 * {@link ConcurrentHashMap}.
 *
 * <p>We intentionally don't compute averages, percentiles, or histograms here
 * — the metrics backend derives those from interval diffs of the cumulative
 * {@code count} / {@code sum_ms} totals. Mirrors the contract of the
 * analytics DataFusion backend's stats endpoint.
 */
public final class AnalyticsStatsCollector {

    private final LatencyTotals queriesElapsed = new LatencyTotals();
    private final LatencyTotals queriesPlanning = new LatencyTotals();

    private final ConcurrentMap<String, StageCounters> byStageType = new ConcurrentHashMap<>();

    private final LongAdder fragmentsTotal = new LongAdder();
    private final LongAdder fragmentsSucceeded = new LongAdder();
    private final LongAdder fragmentsFailed = new LongAdder();
    private final LatencyTotals fragmentsElapsed = new LatencyTotals();

    /**
     * Folds a completed query's execution into the rollup. Called once per query
     * from {@code DefaultPlanExecutor} at terminal — success, failure, or cancellation.
     * Walks the {@link ExecutionGraph} directly and feeds {@link LongAdder}s; no
     * intermediate {@code QueryProfile}, no plan stringification, no per-stage
     * stream filters.
     *
     * <p>Tolerates a null graph (e.g. failures upstream of execution) by
     * recording only the planning time.
     *
     * @param graph the post-execution graph; may be {@code null} if the query
     *              failed before execution started
     * @param dag   the DAG that produced the graph; used to resolve each
     *              {@link StageExecution} to its {@link Stage#getExecutionType()}
     *              for per-stage-type bucketing. May be {@code null} when graph is null.
     * @param planningTimeMs Calcite planning duration in ms
     */
    public void recordExecution(ExecutionGraph graph, QueryDAG dag, long planningTimeMs) {
        if (planningTimeMs > 0) {
            queriesPlanning.record(planningTimeMs);
        }
        if (graph == null) return;

        // Build a stageId -> executionType lookup once. The DAG is small (handful of stages)
        // so a flat HashMap is faster than the recursive findStageById walk QueryProfileBuilder
        // does per-stage.
        Map<Integer, String> stageTypeById = dag != null ? buildStageTypeIndex(dag) : Map.of();

        // Track the earliest start / latest end across all stages to derive end-to-end
        // execution wall time without a second pass over the graph.
        long earliestStart = Long.MAX_VALUE;
        long latestEnd = 0L;

        for (StageExecution exec : graph.allExecutions()) {
            StageMetrics m = exec.getMetrics();
            long start = m.getStartTimeMs();
            long end = m.getEndTimeMs();
            long elapsed = (start > 0 && end > 0) ? end - start : 0L;
            if (start > 0) earliestStart = Math.min(earliestStart, start);
            if (end > 0) latestEnd = Math.max(latestEnd, end);

            String execType = stageTypeById.getOrDefault(exec.getStageId(), exec.getClass().getSimpleName());
            StageCounters c = bucket(execType);
            switch (exec.getState()) {
                case SUCCEEDED -> c.succeeded.increment();
                case FAILED -> c.failed.increment();
                case CANCELLED -> c.cancelled.increment();
                default -> {
                    // CREATED / RUNNING — stage didn't reach terminal; don't count toward terminal buckets.
                }
            }
            c.started.increment();
            c.rowsProcessedTotal.add(m.getRowsProcessed());
            if (elapsed > 0) {
                c.elapsed.record(elapsed);
            }

            for (StageTask task : exec.tasks()) {
                fragmentsTotal.increment();
                switch (task.state()) {
                    case FINISHED -> fragmentsSucceeded.increment();
                    case FAILED -> fragmentsFailed.increment();
                    default -> {
                        // CREATED / RUNNING / CANCELLED — counted in total only.
                    }
                }
                long taskStart = task.startedAtMs();
                long taskEnd = task.finishedAtMs();
                long taskElapsed = (taskStart > 0 && taskEnd > 0) ? taskEnd - taskStart : 0L;
                if (taskElapsed > 0) {
                    fragmentsElapsed.record(taskElapsed);
                }
            }
        }

        long executionTimeMs = (earliestStart != Long.MAX_VALUE && latestEnd > 0) ? latestEnd - earliestStart : 0L;
        if (executionTimeMs > 0) {
            queriesElapsed.record(executionTimeMs);
        }
    }

    /** Builds an immutable snapshot of all counters at this instant. */
    public AnalyticsStats snapshot() {
        AnalyticsStats.Queries queries = new AnalyticsStats.Queries(queriesElapsed.snapshot(), queriesPlanning.snapshot());
        Map<String, AnalyticsStats.StageBucket> stageMap = new TreeMap<>();
        for (Map.Entry<String, StageCounters> e : byStageType.entrySet()) {
            StageCounters c = e.getValue();
            stageMap.put(
                e.getKey(),
                new AnalyticsStats.StageBucket(
                    c.started.sum(),
                    c.succeeded.sum(),
                    c.failed.sum(),
                    c.cancelled.sum(),
                    c.rowsProcessedTotal.sum(),
                    c.elapsed.snapshot()
                )
            );
        }
        AnalyticsStats.Fragments fragments = new AnalyticsStats.Fragments(
            fragmentsTotal.sum(),
            fragmentsSucceeded.sum(),
            fragmentsFailed.sum(),
            fragmentsElapsed.snapshot()
        );
        return new AnalyticsStats(queries, stageMap, fragments);
    }

    private StageCounters bucket(String stageType) {
        return byStageType.computeIfAbsent(stageType != null ? stageType : "unknown", k -> new StageCounters());
    }

    private static Map<Integer, String> buildStageTypeIndex(QueryDAG dag) {
        Map<Integer, String> out = new HashMap<>();
        addStage(out, dag.rootStage());
        return out;
    }

    private static void addStage(Map<Integer, String> out, Stage stage) {
        out.put(stage.getStageId(), stage.getExecutionType().name());
        for (Stage child : stage.getChildStages()) {
            addStage(out, child);
        }
    }

    /**
     * Wait-free count + sum of millisecond elapsed times. Negative recordings
     * are dropped. Mirrors the cumulative {@code total_*_duration_ms} /
     * {@code total_*_count} pattern used by the DataFusion backend's stats.
     */
    private static final class LatencyTotals {
        private final LongAdder count = new LongAdder();
        private final LongAdder sumMs = new LongAdder();

        void record(long valueMs) {
            if (valueMs < 0) return;
            count.increment();
            sumMs.add(valueMs);
        }

        AnalyticsStats.LatencyStats snapshot() {
            return new AnalyticsStats.LatencyStats(count.sum(), sumMs.sum());
        }
    }

    private static final class StageCounters {
        final LongAdder started = new LongAdder();
        final LongAdder succeeded = new LongAdder();
        final LongAdder failed = new LongAdder();
        final LongAdder cancelled = new LongAdder();
        final LongAdder rowsProcessedTotal = new LongAdder();
        final LatencyTotals elapsed = new LatencyTotals();
    }
}

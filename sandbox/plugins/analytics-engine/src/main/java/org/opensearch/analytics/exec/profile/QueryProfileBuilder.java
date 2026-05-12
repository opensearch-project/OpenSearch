/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.profile;

import org.apache.calcite.plan.RelOptUtil;
import org.opensearch.analytics.exec.ExecutionGraph;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageMetrics;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.TaskTracker;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;

import java.util.ArrayList;
import java.util.List;

/**
 * Snapshots an {@link ExecutionGraph} plus the per-query {@link TaskTracker} into a
 * {@link QueryProfile}. Pure read — no mutation of the graph or tracker. Safe to call
 * on success, failure, or cancellation paths: whatever state each stage has reached by
 * the snapshot point is captured verbatim.
 *
 * @opensearch.internal
 */
public final class QueryProfileBuilder {

    private QueryProfileBuilder() {}

    public static QueryProfile snapshot(ExecutionGraph graph, QueryContext config) {
        return snapshot(graph, config, "");
    }

    public static QueryProfile snapshot(ExecutionGraph graph, QueryContext config, String fullPlan) {
        TaskTracker tracker = config.taskTracker();
        List<String> fullPlanLines = splitPlanLines(fullPlan);
        List<StageProfile> stageProfiles = new ArrayList<>();
        long earliestStart = Long.MAX_VALUE;
        long latestEnd = 0L;

        for (StageExecution exec : graph.allExecutions()) {
            StageMetrics m = exec.getMetrics();
            long start = m.getStartTimeMs();
            long end = m.getEndTimeMs();
            long elapsed = (start > 0 && end > 0) ? end - start : 0L;
            if (start > 0) earliestStart = Math.min(earliestStart, start);
            if (end > 0) latestEnd = Math.max(latestEnd, end);

            Stage stage = findStageById(config.dag().rootStage(), exec.getStageId());
            // Stage#getExchangeInfo() is null for the root stage (no parent) and non-null
            // for each cut edge. ExchangeInfo#distributionType() is a Calcite
            // RelDistribution.Type enum.
            String distribution = (stage != null && stage.getExchangeInfo() != null)
                ? stage.getExchangeInfo().distributionType().name()
                : null;
            List<String> fragment = stage != null && stage.getFragment() != null
                ? splitPlanLines(RelOptUtil.toString(stage.getFragment()))
                : List.of();

            List<TaskProfile> taskProfiles = buildTaskProfiles(tracker, exec.getStageId());

            stageProfiles.add(
                new StageProfile(
                    exec.getStageId(),
                    stage != null ? stage.getExecutionType().name() : exec.getClass().getSimpleName(),
                    distribution,
                    exec.getState().name(),
                    start,
                    end,
                    elapsed,
                    m.getRowsProcessed(),
                    m.getTasksCompleted(),
                    m.getTasksFailed(),
                    fragment,
                    taskProfiles
                )
            );
        }

        long totalElapsed = (earliestStart != Long.MAX_VALUE && latestEnd > 0) ? latestEnd - earliestStart : 0L;
        return new QueryProfile(graph.queryId(), fullPlanLines, totalElapsed, stageProfiles);
    }

    /**
     * Splits a Calcite {@code RelOptUtil.toString} output into one entry per line.
     * Empty trailing lines from Calcite's rendering are dropped. Returns an empty list
     * for null or empty input so the caller doesn't have to null-check downstream.
     */
    private static List<String> splitPlanLines(String text) {
        if (text == null || text.isEmpty()) return List.of();
        String[] raw = text.split("\n");
        List<String> out = new ArrayList<>(raw.length);
        for (String line : raw) {
            if (line.isEmpty() == false) out.add(line);
        }
        return out;
    }

    private static List<TaskProfile> buildTaskProfiles(TaskTracker tracker, int stageId) {
        List<StageTask> tasks = tracker.tasksForStage(stageId);
        List<TaskProfile> out = new ArrayList<>(tasks.size());
        for (StageTask t : tasks) {
            long start = t.startedAtMs();
            long end = t.finishedAtMs();
            long elapsed = (start > 0 && end > 0) ? end - start : 0L;
            out.add(
                new TaskProfile(
                    t.id().stageId(),
                    t.id().partitionId(),
                    describeTarget(t.target()),
                    t.state().name(),
                    start,
                    end,
                    elapsed
                )
            );
        }
        return out;
    }

    /**
     * Human-readable target label for the profile output. Includes the node id and,
     * for shard-routed targets, the shard ordinal so the profile identifies which
     * shard a task ran against.
     */
    private static String describeTarget(ExecutionTarget target) {
        if (target == null) return "(unresolved)";
        String nodeId = target.node() != null ? target.node().getId() : "(unknown)";
        if (target instanceof ShardExecutionTarget shard) {
            return nodeId + "/shard[" + shard.shardId().getId() + "]";
        }
        return nodeId;
    }

    private static Stage findStageById(Stage root, int stageId) {
        if (root.getStageId() == stageId) return root;
        for (Stage child : root.getChildStages()) {
            Stage found = findStageById(child, stageId);
            if (found != null) return found;
        }
        return null;
    }
}

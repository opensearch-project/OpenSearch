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
import org.opensearch.analytics.exec.stage.shard.ShardStageTask;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.FilterDelegationInstructionNode;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Snapshots an {@link ExecutionGraph} into a {@link QueryProfile}. Pure read — no
 * mutation of the graph. Safe to call on success, failure, or cancellation paths.
 *
 * @opensearch.internal
 */
public final class QueryProfileBuilder {

    private QueryProfileBuilder() {}

    public static QueryProfile snapshot(ExecutionGraph graph, QueryContext config, String fullPlan, long planningTimeMs) {
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
            String distribution = (stage != null && stage.getExchangeInfo() != null)
                ? stage.getExchangeInfo().distributionType().name()
                : null;
            // TODO(plan-shape): also expose ALL post-fork alternatives
            // (stage.getPlanAlternatives() -> each .resolvedFragment()) as `alternatives`.
            List<String> fragment = stage != null && stage.getFragment() != null
                ? splitPlanLines(RelOptUtil.toString(stage.getFragment()))
                : List.of();
            // PlanAlternativeSelector collapses each stage to a single chosen backend (or it
            // was already singular post-fork). The first alternative IS the chosen one — the
            // data node never re-selects when the list is size 1, which is the post-selector
            // invariant DefaultPlanExecutor depends on.
            String chosenBackend = stage != null && stage.getPlanAlternatives().isEmpty() == false
                ? stage.getPlanAlternatives().getFirst().backendId()
                : null;
            String treeShape = stage != null ? extractTreeShape(stage) : null;

            List<TaskProfile> taskProfiles = buildTaskProfiles(exec);
            long tasksCompleted = taskProfiles.stream().filter(t -> "FINISHED".equals(t.state())).count();
            long tasksFailed = taskProfiles.stream().filter(t -> "FAILED".equals(t.state())).count();

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
                    tasksCompleted,
                    tasksFailed,
                    fragment,
                    chosenBackend,
                    treeShape,
                    taskProfiles
                )
            );
        }

        long executionTimeMs = (earliestStart != Long.MAX_VALUE && latestEnd > 0) ? latestEnd - earliestStart : 0L;
        return new QueryProfile(graph.queryId(), fullPlanLines, planningTimeMs, executionTimeMs, stageProfiles);
    }

    private static List<TaskProfile> buildTaskProfiles(StageExecution exec) {
        List<StageTask> tasks = exec.tasks();
        List<TaskProfile> out = new ArrayList<>(tasks.size());
        for (StageTask t : tasks) {
            long start = t.startedAtMs();
            long end = t.finishedAtMs();
            long elapsed = (start > 0 && end > 0) ? end - start : 0L;
            DataNodePayload payload = parseDataNodePayload(t.dataNodeMetrics());
            out.add(new TaskProfile(describeTarget(t), t.state().name(), elapsed, payload.metrics, payload.physicalPlan));
        }
        return out;
    }

    private record DataNodePayload(Map<String, Long> metrics, String physicalPlan) {
        static final DataNodePayload EMPTY = new DataNodePayload(null, null);
    }

    @SuppressWarnings("unchecked")
    private static DataNodePayload parseDataNodePayload(byte[] json) {
        if (json == null || json.length == 0) return DataNodePayload.EMPTY;
        try {
            var parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, json);
            Map<String, Object> raw = parser.map();
            String physicalPlan = null;
            Map<String, Long> metrics = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                if ("physical_plan".equals(entry.getKey()) && entry.getValue() instanceof String s) {
                    physicalPlan = s;
                } else if (entry.getValue() instanceof Number n) {
                    metrics.put(entry.getKey(), n.longValue());
                }
            }
            return new DataNodePayload(metrics.isEmpty() ? null : metrics, physicalPlan);
        } catch (Exception e) {
            return DataNodePayload.EMPTY;
        }
    }

    private static String describeTarget(StageTask task) {
        if (task instanceof ShardStageTask shardTask) {
            var target = shardTask.target();
            if (target instanceof ShardExecutionTarget shardTarget) {
                String nodeId = shardTarget.node() != null ? shardTarget.node().getId() : "(unknown)";
                return nodeId + "/shard[" + shardTarget.shardId().getId() + "]";
            }
            return target.node() != null ? target.node().getId() : "(unknown)";
        }
        return task.id().toString();
    }

    private static List<String> splitPlanLines(String text) {
        if (text == null || text.isEmpty()) return List.of();
        String[] raw = text.split("\n");
        List<String> out = new ArrayList<>(raw.length);
        for (String line : raw) {
            if (line.isEmpty() == false) out.add(line);
        }
        return out;
    }

    /**
     * Returns the {@code FilterTreeShape} carried by the stage's chosen plan's delegation
     * instruction (filter or shard-scan-with-delegation). Stages without a delegation-bearing
     * instruction (no filter, or a coord stage with no shard scan) return {@code null} — the
     * shape concept doesn't apply to them. This reads from the post-selector first plan.
     */
    private static String extractTreeShape(Stage stage) {
        for (InstructionNode node : stage.getPlanAlternatives().getFirst().instructions()) {
            if (node instanceof FilterDelegationInstructionNode f) return f.getTreeShape().name();
            if (node instanceof ShardScanWithDelegationInstructionNode s) return s.getTreeShape().name();
        }
        return null;
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.profile;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Per-stage profile snapshot. Combines stage-level metadata (id, execution type,
 * distribution), terminal state, wall-clock timing from {@code StageMetrics}, and
 * the list of {@link TaskProfile}s for the stage's dispatched tasks.
 *
 * @param stageId              stage identifier from the DAG
 * @param executionType        value of {@code StageExecutionType} as string (SHARD_FRAGMENT, COORDINATOR_REDUCE, LOCAL_PASSTHROUGH)
 * @param distribution         Calcite distribution type this stage emits to its parent — e.g. SINGLETON, HASH_DISTRIBUTED; null for root
 * @param state                terminal {@code StageExecution.State}
 * @param startMs              wall-clock millis from {@code StageMetrics.recordStart()}, 0 if never started
 * @param endMs                wall-clock millis from {@code StageMetrics.recordEnd()}, 0 if still running
 * @param elapsedMs            {@code endMs - startMs}, or 0 if either stamp is missing
 * @param rowsProcessed        counter from {@code StageMetrics.addRowsProcessed}
 * @param tasksCompleted       counter from {@code StageMetrics.incrementTasksCompleted}
 * @param tasksFailed          counter from {@code StageMetrics.incrementTasksFailed}
 * @param fragment             Calcite {@code RelOptUtil.toString(stage.getFragment())} rendered as an
 *                             array of lines (one element per level of indent) — much easier to read in
 *                             raw JSON than a single multi-line escaped string
 * @param tasks                per-partition task profiles registered with the TaskTracker
 */
public record StageProfile(int stageId, String executionType, String distribution, String state, long startMs, long endMs, long elapsedMs,
    long rowsProcessed, long tasksCompleted, long tasksFailed, List<String> fragment, List<TaskProfile> tasks) implements ToXContentObject {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("stage_id", stageId);
        builder.field("execution_type", executionType);
        if (distribution != null) builder.field("distribution", distribution);
        builder.field("state", state);
        if (startMs > 0) builder.field("start_ms", startMs);
        if (endMs > 0) builder.field("end_ms", endMs);
        builder.field("elapsed_ms", elapsedMs);
        builder.field("rows_processed", rowsProcessed);
        builder.field("tasks_completed", tasksCompleted);
        builder.field("tasks_failed", tasksFailed);
        if (fragment != null && fragment.isEmpty() == false) {
            builder.startArray("fragment");
            for (String line : fragment)
                builder.value(line);
            builder.endArray();
        }
        builder.startArray("tasks");
        for (TaskProfile t : tasks) {
            t.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}

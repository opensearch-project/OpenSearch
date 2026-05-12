/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-query registry of every {@link StageTask} across all stages. Owned by
 * {@code PlanWalker}; populated as stages materialise their task lists at dispatch
 * time. Exists to answer questions like "is this stage finished?" and "which tasks
 * are still running?" without walking every stage execution.
 *
 * <p>The registry is not a replacement for {@link StageExecution}'s own state — it's a
 * lookup index. Stage readiness is still computed from task states here, then driven
 * through the stage's CAS transitions.
 *
 * @opensearch.internal
 */
public final class TaskTracker {

    private final Map<StageTaskId, StageTask> tasks = new ConcurrentHashMap<>();

    /** Register a newly-created task. Idempotent — double-registers overwrite, which should not happen. */
    public void register(StageTask task) {
        tasks.put(task.id(), task);
    }

    /** Returns the task for {@code id}, or null if unknown. */
    public StageTask get(StageTaskId id) {
        return tasks.get(id);
    }

    /**
     * Returns true when every task registered for {@code stageId} has reached a terminal
     * state ({@link StageTaskState#FINISHED}, {@link StageTaskState#FAILED},
     * {@link StageTaskState#CANCELLED}).
     */
    public boolean allTasksTerminalForStage(int stageId) {
        for (StageTask t : tasks.values()) {
            if (t.id().stageId() == stageId && t.state().isTerminal() == false) return false;
        }
        return true;
    }

    /** Returns the subset of tasks registered for {@code stageId}. */
    public List<StageTask> tasksForStage(int stageId) {
        return tasks.values().stream().filter(t -> t.id().stageId() == stageId).toList();
    }
}

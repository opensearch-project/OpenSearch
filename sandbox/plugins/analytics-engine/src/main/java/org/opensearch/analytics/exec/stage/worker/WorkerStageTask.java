/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.worker;

import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.WorkerExecutionTarget;

/**
 * {@link StageTask} variant for hash-shuffle workers. Each task is bound to a single
 * {@link WorkerExecutionTarget} carrying the destination node + partition index; the runner
 * builds the per-task fragment request from the target's partition.
 *
 * @opensearch.internal
 */
public final class WorkerStageTask extends StageTask {

    private final WorkerExecutionTarget target;

    public WorkerStageTask(StageTaskId id, WorkerExecutionTarget target) {
        super(id);
        this.target = target;
    }

    public WorkerExecutionTarget target() {
        return target;
    }
}

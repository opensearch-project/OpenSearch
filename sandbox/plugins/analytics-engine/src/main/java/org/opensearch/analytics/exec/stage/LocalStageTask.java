/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

/**
 * {@link StageTask} variant for coordinator-local execution. Carries the {@link Runnable}
 * body the {@link LocalTaskRunner} runs on the per-query virtual-thread executor — no
 * remote routing, no {@link org.opensearch.analytics.planner.dag.ExecutionTarget}.
 *
 * @opensearch.internal
 */
public final class LocalStageTask extends StageTask {

    private final Runnable body;

    public LocalStageTask(StageTaskId id, Runnable body) {
        super(id);
        this.body = body;
    }

    public Runnable body() {
        return body;
    }
}

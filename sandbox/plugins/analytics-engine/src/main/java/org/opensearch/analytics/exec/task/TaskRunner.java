/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.core.action.ActionListener;

/**
 * Per-stage transport mechanism for a {@link StageTask} variant. Arrow Flight stream
 * for shard fragments, virtual-thread executor for local work, shuffle / broadcast
 * variants in the future. A stage owns both its task variant and its runner, so
 * dispatch is statically typed end-to-end.
 *
 * @opensearch.internal
 */
public interface TaskRunner<T extends StageTask> {

    /**
     * Run {@code task}. Invoke exactly one of {@link ActionListener#onResponse} (signalling
     * task completion — payload is unused) or {@link ActionListener#onFailure}.
     */
    void run(T task, ActionListener<Void> listener);

    /** Sentinel for stages with empty {@code tasks()}; throws if invoked. */
    TaskRunner<StageTask> NONE = (task, listener) -> {
        throw new IllegalStateException("TaskRunner.NONE invoked for task " + task.id());
    };
}

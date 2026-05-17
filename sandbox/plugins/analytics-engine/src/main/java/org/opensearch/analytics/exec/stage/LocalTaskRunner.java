/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.Executor;

/**
 * LOCAL-kind task runner: submits {@link LocalStageTask#body()} to a per-query
 * virtual-thread executor. Virtual threads keep blocking reduce drains off SEARCH workers.
 *
 * @opensearch.internal
 */
public final class LocalTaskRunner implements TaskRunner<LocalStageTask> {

    private final Executor executor;

    public LocalTaskRunner(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void run(LocalStageTask task, ActionListener<Void> listener) {
        executor.execute(() -> {
            try {
                task.body().run();
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.coordinator;

import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.NotifyOnceListener;

import java.util.concurrent.Executor;

/**
 * LOCAL-kind task runner: submits the {@link LocalStageTask#body()} to a per-query
 * virtual-thread executor. The body owns the listener — wrapped in a
 * {@link NotifyOnceListener} so a body that both fires and throws can't double-notify.
 * Virtual threads keep blocking reduce drains off SEARCH workers.
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
        ActionListener<Void> once = new NotifyOnceListener<>() {
            @Override
            protected void innerOnResponse(Void unused) {
                listener.onResponse(unused);
            }

            @Override
            protected void innerOnFailure(Exception cause) {
                listener.onFailure(cause);
            }
        };
        executor.execute(() -> {
            try {
                task.body().accept(once);
            } catch (Exception e) {
                once.onFailure(e);
            }
        });
    }
}

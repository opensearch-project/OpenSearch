/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.coordinator;

import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.core.action.ActionListener;

import java.util.function.Consumer;

/**
 * {@link StageTask} variant for coordinator-local execution. The body is a
 * {@link Consumer} of the scheduler's per-task {@link ActionListener}: the body
 * is responsible for firing the listener (success or failure) when its work
 * completes. {@link LocalTaskRunner} runs the body on the per-query virtual-thread
 * executor and wraps the listener single-fire so a body that fires-then-throws
 * doesn't double-fire.
 *
 * <p>Sync work: body does the work, then calls {@code listener.onResponse(null)}.
 * <p>Async work: body registers a callback that will fire the listener later,
 * then returns immediately. The runner's catch handles body-throws-before-fire.
 *
 * @opensearch.internal
 */
public final class LocalStageTask extends StageTask {

    private final Consumer<ActionListener<Void>> body;

    public LocalStageTask(StageTaskId id, Consumer<ActionListener<Void>> body) {
        super(id);
        this.body = body;
    }

    public Consumer<ActionListener<Void>> body() {
        return body;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.planner.dag.Stage;

/**
 * Sentinel {@link StageExecution} for LOCAL pass-through (root gather)
 * stages. Owns an {@link ExchangeSink} and transitions synchronously through
 * {@code CREATED → RUNNING → SUCCEEDED} on {@link #start()} — there is
 * no real dispatch work. The driver calls {@code start()} only after all
 * children have reached {@code SUCCEEDED}, which is the normal driver rule.
 *
 * <p>Keeps the registry fully populated so cancellation, EXPLAIN, and
 * metrics traversal all work uniformly without pass-through special cases.
 *
 * @opensearch.internal
 */
final class PassThroughStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private final ExchangeSink sink;

    public PassThroughStageExecution(Stage stage, ExchangeSink sink) {
        super(stage);
        this.sink = sink;
    }

    @Override
    public void start() {
        if (transitionTo(State.RUNNING) == false) return;
        transitionTo(State.SUCCEEDED);
    }

    @Override
    public void cancel(String reason) {
        transitionTo(State.CANCELLED);
    }

    @Override
    public ExchangeSink sink() {
        return sink;
    }
}

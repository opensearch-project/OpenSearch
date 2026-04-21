/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.planner.dag.Stage;

/**
 * Sentinel {@link StageExecution} for LOCAL pass-through (root gather)
 * stages. Owns a {@link RowProducingSink} and transitions synchronously
 * through {@code CREATED → RUNNING → SUCCEEDED} on {@link #start()}.
 *
 * <p>Implements both {@link DataConsumer} and {@link DataProducer} via
 * {@link SinkProvidingStageExecution}: children write into the same sink
 * that the root reads from.
 *
 * @opensearch.internal
 */
final class PassThroughStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private final RowProducingSink ownedSink;

    public PassThroughStageExecution(Stage stage, ExchangeSink sink) {
        super(stage);
        if ((sink instanceof RowProducingSink) == false) {
            throw new IllegalArgumentException("PassThroughStageExecution requires a RowProducingSink");
        }
        this.ownedSink = (RowProducingSink) sink;
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
    public ExchangeSink inputSink(int childStageId) {
        return ownedSink;
    }

    @Override
    public ExchangeSink outputSink() {
        return ownedSink;
    }

    @Override
    public ExchangeSource outputSource() {
        return ownedSink;
    }
}

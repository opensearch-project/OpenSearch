/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;

import java.util.List;

/**
 * LOCAL pass-through (root gather) stage. Owns a {@link RowProducingSink} that
 * children write into and the root reads from. Runs a no-op LOCAL task so the
 * scheduler-driven dispatch path stays uniform.
 *
 * @opensearch.internal
 */
final class PassThroughStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private final RowProducingSink ownedSink;

    public PassThroughStageExecution(Stage stage, QueryContext config, ExchangeSink sink) {
        super(stage, config.queryId(), config.operationListeners());
        if ((sink instanceof RowProducingSink) == false) {
            throw new IllegalArgumentException("PassThroughStageExecution requires a RowProducingSink");
        }
        this.ownedSink = (RowProducingSink) sink;
        this.runner = new LocalTaskRunner(config.localTaskExecutor());
    }

    @Override
    public void start() {
        LocalStageTask task = new LocalStageTask(new StageTaskId(getStageId(), 0), () -> {});
        publishTasksAndStart(List.of(task));
    }

    @Override
    public ExchangeSink inputSink(int childStageId) {
        return ownedSink;
    }

    @Override
    public ExchangeSource outputSource() {
        return ownedSink;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;

/**
 * Creates a {@link LateMaterializationStageExecution} for the QTF fetch + assembly phase.
 */
final class LateMaterializationStageScheduler implements StageScheduler {

    private final AnalyticsSearchTransportService transport;

    LateMaterializationStageScheduler(AnalyticsSearchTransportService transport) {
        this.transport = transport;
    }

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        return new LateMaterializationStageExecution(stage, config, sink, transport);
    }
}

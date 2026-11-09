/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;

/**
 * Factory for creating {@link StageExecution} instances for a specific
 * stage execution type. Registered in {@link StageExecutionBuilder}'s factory
 * registry keyed by {@link org.opensearch.analytics.planner.dag.StageExecutionType}.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface StageExecutionFactory {

    /**
     * Creates a stage execution for the given stage, writing output into
     * the provided sink.
     */
    StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config);
}

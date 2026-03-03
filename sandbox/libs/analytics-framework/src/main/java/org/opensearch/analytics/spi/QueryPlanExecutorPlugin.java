/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.analytics.exec.QueryPlanExecutor;

import java.util.List;

/**
 * SPI extension point for query plan execution.
 *
 * <p>Implementations provide a {@link QueryPlanExecutor} to the analytics
 * engine hub. The hub discovers implementations via
 * {@code ExtensiblePlugin.loadExtensions} and calls {@link #createExecutor}
 * during {@code createComponents}, binding the result into Guice for
 * consumption by front-end plugins.
 *
 * @opensearch.internal
 */
public interface QueryPlanExecutorPlugin {

    /**
     * Creates a {@link QueryPlanExecutor} using the discovered back-end plugins.
     * Called by the hub during {@code createComponents}.
     *
     * @param backEnds the list of discovered {@link AnalyticsBackEndPlugin} instances
     * @return a fully initialized {@link QueryPlanExecutor}
     */
    QueryPlanExecutor createExecutor(List<AnalyticsBackEndPlugin> backEnds);
}

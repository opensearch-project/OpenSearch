/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.analytics.spi.QueryPlanExecutorPlugin;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.plugins.Plugin;

import java.util.List;

/**
 * Default query executor plugin.
 * Implements {@link QueryPlanExecutorPlugin} to provide a
 * {@link QueryPlanExecutor} to the analytics engine hub.
 */
public class AnalyticsExecutorPlugin extends Plugin implements QueryPlanExecutorPlugin {

    @Override
    public QueryPlanExecutor createExecutor(List<AnalyticsBackEndPlugin> backEnds) {
        return new DefaultPlanExecutor(backEnds);
    }
}

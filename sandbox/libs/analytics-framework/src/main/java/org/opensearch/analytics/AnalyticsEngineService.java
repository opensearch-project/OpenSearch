/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.exec.QueryPlanExecutor;

/**
 * Static singleton providing the analytics engine's executor and context
 * to front-end plugins (PPL, SQL) that live in separate classloaders.
 *
 * <p>Set by {@code AnalyticsPlugin.createComponents()} at node startup.
 * Consumed by transport actions in front-end plugins via {@link #getInstance()}.
 *
 * @opensearch.internal
 */
public class AnalyticsEngineService {

    private static volatile AnalyticsEngineService INSTANCE;

    private final EngineContext engineContext;
    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;

    public AnalyticsEngineService(EngineContext engineContext,
                                  QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor) {
        this.engineContext = engineContext;
        this.planExecutor = planExecutor;
    }

    public static void setInstance(AnalyticsEngineService instance) {
        INSTANCE = instance;
    }

    public static AnalyticsEngineService getInstance() {
        return INSTANCE;
    }

    public EngineContext getEngineContext() {
        return engineContext;
    }

    public QueryPlanExecutor<RelNode, Iterable<Object[]>> getPlanExecutor() {
        return planExecutor;
    }
}

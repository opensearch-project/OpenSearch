/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Coordinator-level plan executor. Plans the query via the capability-aware planner.
 * Shard-level execution will be handled by a separate data-node transport action.
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final Map<String, AnalyticsSearchBackendPlugin> backEnds;
    private final CapabilityRegistry capabilityRegistry;
    private final ClusterService clusterService;

    public DefaultPlanExecutor(List<AnalyticsSearchBackendPlugin> providers,
                               CapabilityRegistry capabilityRegistry,
                               ClusterService clusterService) {
        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin provider : providers) {
            this.backEnds.put(provider.name(), provider);
        }
        this.capabilityRegistry = capabilityRegistry;
        this.clusterService = clusterService;
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        logicalFragment = PlannerImpl.createPlan(logicalFragment,
            new PlannerContext(capabilityRegistry, clusterService.state()));

        logger.info("[DefaultPlanExecutor] Planned:\n{}", logicalFragment.explain());

        // TODO: dispatch to data-node transport action for shard-level execution
        // For now, return empty results — the plan is logged for validation
        return new ArrayList<>();
    }
}

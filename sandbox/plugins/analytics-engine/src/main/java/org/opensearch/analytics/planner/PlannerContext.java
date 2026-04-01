/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterState;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared context available to all planner rules.
 * Holds cluster state and backend plugins. Rules consult this
 * to extract index metadata, mappings, and backend capabilities
 * for whichever tables they encounter.
 *
 * @opensearch.internal
 */
public class PlannerContext {

    private final Map<String, AnalyticsSearchBackendPlugin> backends;
    private final ClusterState clusterState;
    private final OpenSearchDistributionTraitDef distributionTraitDef;

    public PlannerContext(List<AnalyticsSearchBackendPlugin> backends, ClusterState clusterState) {
        this.backends = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin b : backends) {
            this.backends.put(b.name(), b);
        }
        this.clusterState = clusterState;
        this.distributionTraitDef = new OpenSearchDistributionTraitDef(this);
    }

    public Map<String, AnalyticsSearchBackendPlugin> getBackends() {
        return backends;
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public OpenSearchDistributionTraitDef getDistributionTraitDef() {
        return distributionTraitDef;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.cluster.ClusterState;

/**
 * Shared context available to all planner rules.
 * Holds capability registry (singleton, built at plugin startup) and
 * per-query cluster state.
 *
 * @opensearch.internal
 */
public class PlannerContext {

    private final CapabilityRegistry capabilityRegistry;
    private final ClusterState clusterState;
    private final OpenSearchDistributionTraitDef distributionTraitDef;
    private int annotationIdCounter;

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState) {
        this.capabilityRegistry = capabilityRegistry;
        this.clusterState = clusterState;
        this.distributionTraitDef = new OpenSearchDistributionTraitDef(this);
        this.annotationIdCounter = 0;
    }

    /** Returns a unique annotation ID for marking phase. */
    public int nextAnnotationId() {
        return annotationIdCounter++;
    }

    public CapabilityRegistry getCapabilityRegistry() {
        return capabilityRegistry;
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public OpenSearchDistributionTraitDef getDistributionTraitDef() {
        return distributionTraitDef;
    }
}

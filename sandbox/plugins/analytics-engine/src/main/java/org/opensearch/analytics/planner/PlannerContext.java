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
    private final boolean profilingEnabled;
    private int annotationIdCounter;
    private RuleProfilingListener.PlannerProfile lastProfile;

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState) {
        this(capabilityRegistry, clusterState, false);
    }

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState, boolean profilingEnabled) {
        this.capabilityRegistry = capabilityRegistry;
        this.clusterState = clusterState;
        this.distributionTraitDef = new OpenSearchDistributionTraitDef(this);
        this.profilingEnabled = profilingEnabled;
        this.annotationIdCounter = 0;
    }

    /** True when {@link PlannerImpl#runAllOptimizations} should attach a {@link RuleProfilingListener}. */
    public boolean isProfilingEnabled() {
        return profilingEnabled;
    }

    /** Stash the snapshot taken at the end of {@code runAllOptimizations}. Null when profiling was disabled. */
    public void recordProfilingResults(RuleProfilingListener.PlannerProfile profile) {
        this.lastProfile = profile;
    }

    public RuleProfilingListener.PlannerProfile getProfilingResults() {
        return lastProfile;
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

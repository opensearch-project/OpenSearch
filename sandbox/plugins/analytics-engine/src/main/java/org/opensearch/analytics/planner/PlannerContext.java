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
import org.opensearch.common.settings.Settings;

/**
 * Shared context available to all planner rules.
 * Holds capability registry (singleton, built at plugin startup), per-query cluster state,
 * and the cluster settings snapshot used by hash-shuffle partition-count resolution.
 *
 * @opensearch.internal
 */
public class PlannerContext {

    private final CapabilityRegistry capabilityRegistry;
    private final ClusterState clusterState;
    private final Settings settings;
    private final OpenSearchDistributionTraitDef distributionTraitDef;
    private final boolean profilingEnabled;
    private int annotationIdCounter;
    private RuleProfilingListener.PlannerProfile lastProfile;

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState) {
        this(capabilityRegistry, clusterState, Settings.EMPTY, false);
    }

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState, boolean profilingEnabled) {
        this(capabilityRegistry, clusterState, Settings.EMPTY, profilingEnabled);
    }

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState, Settings settings) {
        this(capabilityRegistry, clusterState, settings, false);
    }

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState, Settings settings, boolean profilingEnabled) {
        this.capabilityRegistry = capabilityRegistry;
        this.clusterState = clusterState;
        this.settings = settings;
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

    public Settings getSettings() {
        return settings;
    }

    public OpenSearchDistributionTraitDef getDistributionTraitDef() {
        return distributionTraitDef;
    }
}

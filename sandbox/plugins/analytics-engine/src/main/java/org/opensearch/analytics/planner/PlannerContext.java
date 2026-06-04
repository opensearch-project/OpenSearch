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
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.Nullable;

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
    @Nullable
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final OpenSearchDistributionTraitDef distributionTraitDef;
    private final boolean profilingEnabled;
    private final boolean preferMetadataDriver;
    private int annotationIdCounter;
    private RuleProfilingListener.PlannerProfile lastProfile;

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState) {
        this(capabilityRegistry, clusterState, null, false, true);
    }

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState, boolean profilingEnabled) {
        this(capabilityRegistry, clusterState, null, profilingEnabled, true);
    }

    public PlannerContext(
        CapabilityRegistry capabilityRegistry,
        ClusterState clusterState,
        @Nullable IndexNameExpressionResolver indexNameExpressionResolver,
        boolean profilingEnabled
    ) {
        this(capabilityRegistry, clusterState, indexNameExpressionResolver, profilingEnabled, true);
    }

    public PlannerContext(
        CapabilityRegistry capabilityRegistry,
        ClusterState clusterState,
        @Nullable IndexNameExpressionResolver indexNameExpressionResolver,
        boolean profilingEnabled,
        boolean preferMetadataDriver
    ) {
        this.capabilityRegistry = capabilityRegistry;
        this.clusterState = clusterState;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.distributionTraitDef = new OpenSearchDistributionTraitDef(this);
        this.profilingEnabled = profilingEnabled;
        this.preferMetadataDriver = preferMetadataDriver;
        this.annotationIdCounter = 0;
    }

    /**
     * The cluster-level index name expression resolver, when available. Null in unit tests
     * that don't exercise alias/wildcard expansion; callers that need it for production
     * paths should fail fast on null. The resolver belongs to the OpenSearch server lifecycle
     * and is provided to {@code DefaultPlanExecutor} via Guice.
     */
    @Nullable
    public IndexNameExpressionResolver getIndexNameExpressionResolver() {
        return indexNameExpressionResolver;
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

    /**
     * Mirrors the {@code analytics.planner.prefer_metadata_driver} cluster setting at planning
     * time. When {@code false}, {@code OpenSearchTableScanRule} skips the permissive
     * metadata-only-driver gate, so the metadata backend (Lucene today) is never admitted as a
     * scan alternative — value-producing peers handle every shape, no late-stage alternative
     * pruning needed.
     */
    public boolean preferMetadataDriver() {
        return preferMetadataDriver;
    }
}

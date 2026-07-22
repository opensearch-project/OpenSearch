/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.settings.DelegationBlockList;
import org.opensearch.analytics.settings.PlannerSettings;
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
    // Base-table columns the query actually references, computed once before marking. Null means
    // "validate all fields" — the pre-existing behavior — used for unanalyzable plan shapes.
    // Cross-index type-conflict validation ({@link IndexResolution}) is scoped to this set so a
    // query that never touches a conflicting field is not rejected because of it.
    @Nullable
    private java.util.Set<String> referencedFields;
    // Cluster settings the planner consults at planning time (oversampling factor + delegation
    // block-list). Defaults to planner defaults; DefaultPlanExecutor injects the live, settings-backed
    // instance via setPlannerSettings before planning.
    private PlannerSettings plannerSettings = PlannerSettings.defaults();

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

    /** Per-backend delegation block-list consulted at marking time. Never null (defaults to empty). */
    public DelegationBlockList getDelegationBlockList() {
        return plannerSettings.getDelegationBlockList();
    }

    /** Inject the live, settings-backed planner settings. Called by {@code DefaultPlanExecutor} before planning. */
    public void setPlannerSettings(PlannerSettings plannerSettings) {
        this.plannerSettings = plannerSettings;
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    /**
     * The base-table columns the query references, or {@code null} to validate all fields. Set once
     * by {@link PlannerImpl} before marking; consulted by {@code OpenSearchTableScanRule} to scope
     * cross-index type-conflict validation. Null (the default) preserves validate-everything.
     */
    @Nullable
    public java.util.Set<String> getReferencedFields() {
        return referencedFields;
    }

    /** Sets the referenced-field set computed from the raw plan before marking. */
    public void setReferencedFields(@Nullable java.util.Set<String> referencedFields) {
        this.referencedFields = referencedFields;
    }

    public double getOversamplingFactor() {
        return plannerSettings.getOversamplingFactor();
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

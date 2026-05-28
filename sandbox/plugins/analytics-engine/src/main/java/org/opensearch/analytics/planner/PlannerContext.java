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
import org.opensearch.common.settings.Settings;

import java.util.function.ToLongFunction;

/**
 * Shared context available to all planner rules.
 * Holds capability registry (singleton, built at plugin startup), per-query cluster state,
 * and the cluster settings snapshot used by hash-shuffle partition-count resolution.
 *
 * @opensearch.internal
 */
public class PlannerContext {

    /**
     * Sentinel returned by {@link #DEFAULT_TABLE_ROW_COUNTS} for indices we have no statistics
     * for. The cost model treats this as "use Calcite's default" — in practice
     * {@code RelOptAbstractTable.getRowCount()} returns 100.0 in that case.
     */
    public static final long UNKNOWN_ROW_COUNT = -1L;

    /** Default lookup that always returns {@link #UNKNOWN_ROW_COUNT} — used when no statistics
     *  source has been wired (e.g. unit tests that don't exercise cost-aware planning). */
    public static final ToLongFunction<String> DEFAULT_TABLE_ROW_COUNTS = name -> UNKNOWN_ROW_COUNT;

    private final CapabilityRegistry capabilityRegistry;
    private final ClusterState clusterState;
    private final Settings settings;
    private final ToLongFunction<String> tableRowCounts;
    @Nullable
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final OpenSearchDistributionTraitDef distributionTraitDef;
    private final boolean profilingEnabled;
    private int annotationIdCounter;
    private RuleProfilingListener.PlannerProfile lastProfile;

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState) {
        this(capabilityRegistry, clusterState, Settings.EMPTY, DEFAULT_TABLE_ROW_COUNTS, null, false);
    }

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState, boolean profilingEnabled) {
        this(capabilityRegistry, clusterState, Settings.EMPTY, DEFAULT_TABLE_ROW_COUNTS, null, profilingEnabled);
    }

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState, Settings settings) {
        this(capabilityRegistry, clusterState, settings, DEFAULT_TABLE_ROW_COUNTS, null, false);
    }

    public PlannerContext(CapabilityRegistry capabilityRegistry, ClusterState clusterState, Settings settings, boolean profilingEnabled) {
        this(capabilityRegistry, clusterState, settings, DEFAULT_TABLE_ROW_COUNTS, null, profilingEnabled);
    }

    public PlannerContext(
        CapabilityRegistry capabilityRegistry,
        ClusterState clusterState,
        @Nullable IndexNameExpressionResolver indexNameExpressionResolver,
        boolean profilingEnabled
    ) {
        this(capabilityRegistry, clusterState, Settings.EMPTY, DEFAULT_TABLE_ROW_COUNTS, indexNameExpressionResolver, profilingEnabled);
    }

    public PlannerContext(
        CapabilityRegistry capabilityRegistry,
        ClusterState clusterState,
        Settings settings,
        ToLongFunction<String> tableRowCounts,
        boolean profilingEnabled
    ) {
        this(capabilityRegistry, clusterState, settings, tableRowCounts, null, profilingEnabled);
    }

    public PlannerContext(
        CapabilityRegistry capabilityRegistry,
        ClusterState clusterState,
        Settings settings,
        ToLongFunction<String> tableRowCounts,
        @Nullable IndexNameExpressionResolver indexNameExpressionResolver,
        boolean profilingEnabled
    ) {
        this.capabilityRegistry = capabilityRegistry;
        this.clusterState = clusterState;
        this.settings = settings;
        this.tableRowCounts = tableRowCounts;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.distributionTraitDef = new OpenSearchDistributionTraitDef(this);
        this.profilingEnabled = profilingEnabled;
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

    public Settings getSettings() {
        return settings;
    }

    /**
     * Per-index row count lookup used by {@code OpenSearchTableScanRule} to seed
     * {@code IndexNameTable.getRowCount()}. Returns {@link #UNKNOWN_ROW_COUNT} for
     * indices we have no statistics for; callers fall back to Calcite's default.
     */
    public ToLongFunction<String> getTableRowCounts() {
        return tableRowCounts;
    }

    public OpenSearchDistributionTraitDef getDistributionTraitDef() {
        return distributionTraitDef;
    }
}

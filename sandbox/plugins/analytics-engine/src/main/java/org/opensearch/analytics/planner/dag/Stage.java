/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.common.Nullable;

import java.util.List;

/**
 * A stage in the query DAG. Each stage holds a marked plan fragment (annotations
 * intact, multiple viableBackends per operator/expression), a {@link TargetResolver}
 * for the Scheduler to resolve execution targets lazily, and references to child stages.
 *
 * <p>The Scheduler determines how to execute a stage from two fields:
 * <ul>
 *   <li>{@link #getTargetResolver()} non-null → dispatch fragment to data nodes</li>
 *   <li>{@link #getExchangeSinkProvider()} non-null → create a backend compute sink at the coordinator</li>
 *   <li>Both null → single-stage query, use a simple {@code RowProducingSink} at the coordinator</li>
 * </ul>
 *
 * <p>After plan forking, {@code planAlternatives} contains resolved variants
 * where every viableBackends is narrowed to exactly one backend.
 *
 * @opensearch.internal
 */
public class Stage {

    private final int stageId;
    private final RelNode fragment;
    private final List<Stage> childStages;
    private final ExchangeInfo exchangeInfo;
    private final ExchangeSinkProvider exchangeSinkProvider;
    private final TargetResolver targetResolver;
    private List<StagePlan> planAlternatives;

    public Stage(
        int stageId,
        RelNode fragment,
        List<Stage> childStages,
        ExchangeInfo exchangeInfo,
        ExchangeSinkProvider exchangeSinkProvider,
        TargetResolver targetResolver
    ) {
        this.stageId = stageId;
        this.fragment = fragment;
        this.childStages = List.copyOf(childStages);
        this.exchangeInfo = exchangeInfo;
        this.exchangeSinkProvider = exchangeSinkProvider;
        this.targetResolver = targetResolver;
        this.planAlternatives = List.of();
    }

    public int getStageId() { return stageId; }

    /** Marked plan fragment with annotations intact. */
    public RelNode getFragment() { return fragment; }

    public List<Stage> getChildStages() { return childStages; }

    /** How this stage connects to its parent. Null for the root stage. */
    @Nullable
    public ExchangeInfo getExchangeInfo() { return exchangeInfo; }

    /**
     * Non-null for coordinator stages with backend computation (final aggregate, sort).
     * Null for simple gather stages — Scheduler uses a {@code RowProducingSink} instead.
     */
    @Nullable
    public ExchangeSinkProvider getExchangeSinkProvider() { return exchangeSinkProvider; }

    /**
     * Non-null for DATA_NODE stages. Null for coordinator/gather stages.
     * Scheduler calls {@code targetResolver.resolve(clusterState, childManifest)} lazily
     * just before dispatch.
     */
    @Nullable
    public TargetResolver getTargetResolver() { return targetResolver; }

    public List<StagePlan> getPlanAlternatives() { return planAlternatives; }

    public void setPlanAlternatives(List<StagePlan> planAlternatives) {
        this.planAlternatives = planAlternatives;
    }
}

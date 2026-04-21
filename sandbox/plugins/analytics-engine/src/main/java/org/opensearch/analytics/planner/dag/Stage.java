/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.common.Nullable;

import java.util.List;

/**
 * A stage in the query DAG. Each stage holds a marked plan fragment (annotations
 * intact, multiple viableBackends per operator/expression), a {@link TargetResolver}
 * for the Scheduler to resolve execution targets lazily, and references to child stages.
 *
 * <p>Execution shape is surfaced explicitly via {@link #getExecutionType()}, derived
 * at construction in priority order:
 * <ol>
 *   <li>{@link ExchangeInfo#distributionType()} == {@code HASH_DISTRIBUTED}
 *       → {@link StageExecutionType#SHUFFLE_WRITE}.</li>
 *   <li>{@link ExchangeInfo#distributionType()} == {@code BROADCAST_DISTRIBUTED}
 *       → {@link StageExecutionType#BROADCAST_WRITE}.</li>
 *   <li>{@link #getTargetResolver()} non-null → {@link StageExecutionType#SHARD_FRAGMENT}
 *       — dispatch fragment per-shard to data nodes.</li>
 *   <li>{@link #getExchangeSinkProvider()} non-null → {@link StageExecutionType#COORDINATOR_REDUCE}
 *       — coordinator-side reduction via backend sink.</li>
 *   <li>Otherwise → {@link StageExecutionType#LOCAL_PASSTHROUGH} — coordinator gather
 *       via {@code RowProducingSink}.</li>
 * </ol>
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
    private final StageExecutionType executionType;
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
        this.executionType = setStageExecutionType(exchangeInfo, exchangeSinkProvider, targetResolver);
        this.planAlternatives = List.of();
    }

    public int getStageId() {
        return stageId;
    }

    /** Marked plan fragment with annotations intact. */
    public RelNode getFragment() {
        return fragment;
    }

    public List<Stage> getChildStages() {
        return childStages;
    }

    /** How this stage connects to its parent. Null for the root stage. */
    @Nullable
    public ExchangeInfo getExchangeInfo() {
        return exchangeInfo;
    }

    /**
     * Non-null for coordinator stages with backend computation (final aggregate, sort).
     * Null for simple gather stages — Scheduler uses a {@code RowProducingSink} instead.
     */
    @Nullable
    public ExchangeSinkProvider getExchangeSinkProvider() {
        return exchangeSinkProvider;
    }

    /**
     * Non-null for DATA_NODE stages. Null for coordinator/gather stages.
     * Scheduler calls {@code targetResolver.resolve(clusterState, childManifest)} lazily
     * just before dispatch.
     */
    @Nullable
    public TargetResolver getTargetResolver() {
        return targetResolver;
    }

    /**
     * Returns where this stage's compute runs. Derived at construction from the
     * target resolver / sink provider pair — see the class-level javadoc.
     */
    public StageExecutionType getExecutionType() {
        return executionType;
    }

    public List<StagePlan> getPlanAlternatives() {
        return planAlternatives;
    }

    public void setPlanAlternatives(List<StagePlan> planAlternatives) {
        this.planAlternatives = planAlternatives;
    }

    private StageExecutionType setStageExecutionType(ExchangeInfo exchangeInfo, ExchangeSinkProvider exchangeSinkProvider, TargetResolver targetResolver) {
        if (exchangeInfo != null && exchangeInfo.distributionType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            return StageExecutionType.SHUFFLE_WRITE;
        } else if (exchangeInfo != null && exchangeInfo.distributionType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
            return StageExecutionType.BROADCAST_WRITE;
        } else if (targetResolver != null) {
            return StageExecutionType.SHARD_FRAGMENT;
        } else if (exchangeSinkProvider != null) {
            return StageExecutionType.COORDINATOR_REDUCE;
        } else {
            return StageExecutionType.LOCAL_PASSTHROUGH;
        }
    }
}

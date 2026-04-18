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
import org.opensearch.analytics.exec.ShardFilterPhase;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.common.Nullable;

import java.util.List;

/**
 * A stage in the query DAG. Each stage holds a marked plan fragment (annotations
 * intact, multiple viableBackends per operator/expression) and references to
 * child stages.
 *
 * <p>After plan forking, {@code planAlternatives} contains resolved variants
 * where every viableBackends is narrowed to exactly one backend.
 *
 * <p>Stage properties like {@code tableName} and {@code shuffleWrite} are
 * computed once during DAG construction and avoid repeated RelNode tree walking
 * at execution time.
 *
 * @opensearch.internal
 */
public class Stage {

    private final int stageId;
    private final RelNode fragment;
    private final List<Stage> childStages;
    private final ExchangeInfo exchangeInfo;
    private final String tableName;
    private final StageExecutionType executionType;
    private List<StagePlan> planAlternatives;
    private ShardFilterPhase shardFilterPhase = ShardFilterPhase.IDENTITY;

    /**
     * Creates a stage with an inferred execution type. The type defaults to
     * {@link StageExecutionType#LOCAL} when there is no exchange and no table
     * scan, and {@link StageExecutionType#DATA_NODE} otherwise.
     */
    public Stage(int stageId, RelNode fragment, List<Stage> childStages, ExchangeInfo exchangeInfo) {
        this(stageId, fragment, childStages, exchangeInfo, null);
    }

    /**
     * Creates a stage with an explicit execution type. When {@code executionType}
     * is null the type is inferred from the fragment.
     */
    public Stage(int stageId, RelNode fragment, List<Stage> childStages, ExchangeInfo exchangeInfo, StageExecutionType executionType) {
        this.stageId = stageId;
        this.fragment = fragment;
        this.childStages = List.copyOf(childStages);
        this.exchangeInfo = exchangeInfo;
        this.tableName = findTableName(fragment);
        this.planAlternatives = List.of();
        if (executionType != null) {
            this.executionType = executionType;
        } else {
            // Infer: no exchange + no table scan → LOCAL
            this.executionType = (exchangeInfo == null && this.tableName == null) ? StageExecutionType.LOCAL : StageExecutionType.DATA_NODE;
        }
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
    public ExchangeInfo getExchangeInfo() {
        return exchangeInfo;
    }

    /**
     * The table name scanned by this stage, or null if the stage has no TableScan
     * (e.g., LOCAL stage with StageInputScan).
     */
    @Nullable
    public String getTableName() {
        return tableName;
    }

    /** Returns the execution type assigned to this stage during DAG construction. */
    public StageExecutionType getExecutionType() {
        return executionType;
    }

    /**
     * Returns true if this stage writes shuffle output (exchange is HASH_DISTRIBUTED).
     * Responses carry metadata (partition manifests) instead of rows.
     */
    public boolean isShuffleWrite() {
        return exchangeInfo != null && exchangeInfo.distributionType() == RelDistribution.Type.HASH_DISTRIBUTED;
    }

    /**
     * Returns true if this stage is a shuffle-read stage: a LOCAL stage that has
     * at least one child whose {@link #isShuffleWrite()} is true.
     */
    public boolean isShuffleRead() {
        return executionType == StageExecutionType.LOCAL && childStages.stream().anyMatch(Stage::isShuffleWrite);
    }

    /**
     * Returns true if this stage writes broadcast output (exchange is BROADCAST_DISTRIBUTED).
     * Responses carry a broadcast manifest (one opaque handle per producer shard) instead of rows.
     */
    public boolean isBroadcastWrite() {
        return exchangeInfo != null && exchangeInfo.distributionType() == RelDistribution.Type.BROADCAST_DISTRIBUTED;
    }

    /**
     * Returns true if this stage has at least one child whose {@link #isBroadcastWrite()} is true.
     * No {@code executionType} constraint — scope is enforced at the scheduler selection layer.
     */
    public boolean isBroadcastRead() {
        return childStages.stream().anyMatch(Stage::isBroadcastWrite);
    }

    public List<StagePlan> getPlanAlternatives() {
        return planAlternatives;
    }

    public void setPlanAlternatives(List<StagePlan> planAlternatives) {
        this.planAlternatives = planAlternatives;
    }

    public ShardFilterPhase getShardFilterPhase() {
        return shardFilterPhase;
    }

    public void setShardFilterPhase(ShardFilterPhase shardFilterPhase) {
        this.shardFilterPhase = shardFilterPhase;
    }

    /** Walks the fragment tree to find OpenSearchTableScan and extract the table name. */
    private static String findTableName(RelNode node) {
        if (node == null) return null;
        if (node instanceof OpenSearchTableScan scan) {
            List<String> qn = scan.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = findTableName(input);
            if (name != null) return name;
        }
        return null;
    }
}

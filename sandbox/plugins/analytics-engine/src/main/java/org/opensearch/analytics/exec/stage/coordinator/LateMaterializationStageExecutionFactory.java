/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.coordinator;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.stage.LateMaterializationStageExecution;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionFactory;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;

/**
 * Builds executions for {@link StageExecutionType#LATE_MATERIALIZATION} (QTF Scatter-Gather)
 * stages. Pulls the {@code OpenSearchLateMaterialization} marker out of the stage's fragment
 * for fetch-list metadata and hands the resulting context to {@link LateMaterializationStageExecution}.
 *
 * <p>This stage has no Substrait fragment ({@link
 * org.opensearch.analytics.planner.dag.FragmentConversionDriver} skips it), so unlike
 * {@link ReduceStageExecutionFactory} we do not pull plan bytes / sink providers / instructions
 * from the stage. The wrapper RelNode itself carries everything we need: the fetch list
 * (which columns to fetch) and per-column storage info (how the data-node should read them).
 *
 * @opensearch.internal
 */
public final class LateMaterializationStageExecutionFactory implements StageExecutionFactory {

    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService transport;

    public LateMaterializationStageExecutionFactory(ClusterService clusterService, AnalyticsSearchTransportService transport) {
        this.clusterService = clusterService;
        this.transport = transport;
    }

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        Stage shardStage = findShardFragmentDescendant(stage);
        if (shardStage == null) {
            throw new IllegalStateException("LATE_MATERIALIZATION stage " + stage.getStageId() + " has no SHARD_FRAGMENT descendant");
        }
        return new LateMaterializationStageExecution(
            stage,
            config,
            sink,
            clusterService,
            transport,
            shardStage.getStageId(),
            shardStage.getPlanAlternatives().get(0).backendId()
        );
    }

    /** DFS for the SHARD_FRAGMENT descendant; null if none. */
    private static Stage findShardFragmentDescendant(Stage stage) {
        for (Stage child : stage.getChildStages()) {
            if (child.getExecutionType() == StageExecutionType.SHARD_FRAGMENT) return child;
            Stage deeper = findShardFragmentDescendant(child);
            if (deeper != null) return deeper;
        }
        return null;
    }
}

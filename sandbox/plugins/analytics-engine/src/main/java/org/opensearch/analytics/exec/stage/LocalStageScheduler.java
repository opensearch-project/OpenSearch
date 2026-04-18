/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.backend.LocalStageRequest;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds executions for LOCAL stages. Handles both pass-through (root gather)
 * stages and compute LOCAL stages backed by an
 * {@link AnalyticsSearchBackendPlugin}. Takes a pre-resolved
 * {@link ExchangeSink} and doesn't care whether it is a root sink or a
 * parent-provided child sink — {@link StageExecutionBuilder} resolves that
 * distinction before calling.
 *
 * <p>For compute LOCAL stages, the scheduler selects the backend by matching
 * the stage's plan alternatives against the map of registered backends.
 * The first plan alternative whose {@code backendId} matches a registered
 * backend wins — the same "first-match" strategy used by
 * {@code AnalyticsSearchService} on data nodes.
 *
 * <p>Pass-through stages return a {@link PassThroughStageExecution};
 * compute LOCAL stages return a {@link LocalStageExecution}.
 *
 * @opensearch.internal
 */
final class LocalStageScheduler {

    private static final Logger logger = LogManager.getLogger(LocalStageScheduler.class);

    private final Map<String, AnalyticsSearchBackendPlugin> backends;

    LocalStageScheduler(Map<String, AnalyticsSearchBackendPlugin> backends) {
        this.backends = backends != null ? backends : Map.of();
    }

    StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        if (isPassThrough(stage)) {
            return new PassThroughStageExecution(stage, sink);
        }
        return buildComputeLocal(stage, sink, config);
    }

    private StageExecution buildComputeLocal(Stage stage, ExchangeSink sink, QueryContext config) {
        if (backends.isEmpty()) {
            throw new IllegalStateException(
                "No analytics backends registered — cannot dispatch compute LOCAL stage "
                    + "(stageId="
                    + stage.getStageId()
                    + ")"
            );
        }

        // Select the first plan alternative whose backendId matches a registered backend.
        StagePlan chosenPlan = null;
        AnalyticsSearchBackendPlugin backend = null;
        for (StagePlan plan : stage.getPlanAlternatives()) {
            AnalyticsSearchBackendPlugin candidate = backends.get(plan.backendId());
            if (candidate != null) {
                chosenPlan = plan;
                backend = candidate;
                break;
            }
        }
        if (chosenPlan == null) {
            throw new IllegalStateException(
                "No StagePlan alternative matches a registered analytics backend "
                    + "(stageId="
                    + stage.getStageId()
                    + ", plan backends="
                    + stage.getPlanAlternatives().stream().map(StagePlan::backendId).toList()
                    + ", available="
                    + backends.keySet()
                    + ")"
            );
        }

        Map<Integer, Schema> childSchemas = buildChildSchemas(stage);

        LocalStageRequest req = new LocalStageRequest(
            config.queryId(),
            stage.getStageId(),
            chosenPlan.convertedBytes(),
            config.bufferAllocator(),
            sink,
            childSchemas
        );

        LocalStageContext ctx;
        try {
            ctx = backend.createLocalStage(req);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create local stage context for stageId=" + stage.getStageId(), e);
        }
        return new LocalStageExecution(stage, ctx, sink);
    }

    /**
     * Builds a map of child stage id → Arrow {@link Schema} from each child
     * stage's fragment row type. Used to construct
     * {@link LocalStageRequest}.
     */
    static Map<Integer, Schema> buildChildSchemas(Stage stage) {
        Map<Integer, Schema> childSchemas = new HashMap<>();
        for (Stage child : stage.getChildStages()) {
            childSchemas.put(child.getStageId(), ArrowSchemaFromCalcite.arrowSchemaFromRowType(child.getFragment().getRowType()));
        }
        return childSchemas;
    }

    /**
     * Returns true if the stage is a LOCAL pass-through (root gather) stage.
     * A pass-through stage has no real fragment — it just gathers children's
     * output into the parent's output chain.
     */
    static boolean isPassThrough(Stage stage) {
        if (stage.getExecutionType() != StageExecutionType.LOCAL) {
            return false;
        }
        return stage.getFragment() == null || stage.getFragment() instanceof OpenSearchStageInputScan;
    }
}

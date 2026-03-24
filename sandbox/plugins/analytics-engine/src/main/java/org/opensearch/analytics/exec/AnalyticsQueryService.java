/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultBatchIterator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.plan.ResolvedPlan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.shard.IndexShard;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data-node service for analytics query execution. Manages the lifecycle of
 * query execution contexts and dispatches resolved plans to the appropriate
 * backend engines.
 *
 * <p>Handles: shard engine resolution, reader snapshot acquisition, delegation
 * setup, engine execution, result collection, and context tracking.
 */
@ExperimentalApi
public class AnalyticsQueryService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(AnalyticsQueryService.class);

    private final AtomicLong nextContextId = new AtomicLong(1);
    private final ConcurrentMapLong<ExecutionContext> activeContexts = ConcurrentCollections
        .newConcurrentMapLongWithAggressiveConcurrency();

    private final Map<String, AnalyticsSearchBackendPlugin> backEnds;

    public AnalyticsQueryService(Map<String, AnalyticsSearchBackendPlugin> backEnds) {
        this.backEnds = backEnds;
    }

    /**
     * Executes a resolved plan against a local shard.
     *
     * @param plan  the resolved plan with backend assignments and delegation predicates
     * @param shard the local index shard
     * @return rows as list of Object arrays
     */
    public Iterable<Object[]> execute(ResolvedPlan plan, IndexShard shard, SearchShardTask task) {
        DataFormatAwareEngine dataFormatAwareEngine = shard.getCompositeEngine();
        if (dataFormatAwareEngine == null) {
            throw new IllegalStateException("No CompositeEngine on shard [" + shard.shardId() + "]");
        }

        AnalyticsSearchBackendPlugin plugin = backEnds.get(plan.getPrimaryBackend());
        if (plugin == null) {
            throw new IllegalStateException("No plugin registered for backend [" + plan.getPrimaryBackend() + "]");
        }

        String tableName = plan.getRoot().getTable() != null
            ? plan.getRoot().getTable().getQualifiedName().get(plan.getRoot().getTable().getQualifiedName().size() - 1)
            : "unknown";

        long ctxId = -1;

        try (DataFormatAwareEngine.DataFormatAwareReader dataFormatAwareReader = dataFormatAwareEngine.acquireReader()) {

            ExecutionContext ctx = new ExecutionContext(plan, tableName, task, dataFormatAwareReader);
            ctxId = putContext(ctx);
            List<Object[]> rows = new ArrayList<>();
            // Create primary engine and execute
            try (SearchExecEngine engine = plugin.searcher(ctx)) {
                logger.info("[AnalyticsQueryService] Executing via [{}], ctxId={}", plugin.name(), ctxId);
                try (EngineResultStream resultStream = engine.execute(ctx)) {
                    EngineResultBatchIterator batchIterator = resultStream.iterator();
                    while (batchIterator.hasNext()) {
                        EngineResultBatch batch = batchIterator.next();
                        List<String> fieldNames = batch.getFieldNames();
                        for (int row = 0; row < batch.getRowCount(); row++) {
                            Object[] rowValues = new Object[fieldNames.size()];
                            for (int col = 0; col < fieldNames.size(); col++) {
                                rowValues[col] = batch.getFieldValue(fieldNames.get(col), row);
                            }
                            rows.add(rowValues);
                        }
                    }

                }
            }
            logger.info("[AnalyticsQueryService] Completed via [{}], {} rows, ctxId={}", plugin.name(), rows.size(), ctxId);
            return rows;
        } catch (Exception e) {
            throw new RuntimeException("Execution failed for [" + plugin.name() + "]", e);
        } finally {
            removeContext(ctxId);
        }
    }

    public long putContext(ExecutionContext context) {
        long id = nextContextId.getAndIncrement();
        activeContexts.put(id, context);
        return id;
    }

    public ExecutionContext getContext(long id) {
        return activeContexts.get(id);
    }

    public ExecutionContext removeContext(long id) {
        return activeContexts.remove(id);
    }

    public int getActiveContextCount() {
        return activeContexts.size();
    }

    @Override
    protected void doStart() {
        logger.info("[AnalyticsQueryService] Started");
    }

    @Override
    protected void doStop() {
        logger.info("[AnalyticsQueryService] Stopping, clearing {} active contexts", activeContexts.size());
        activeContexts.clear();
    }

    @Override
    protected void doClose() {}
}

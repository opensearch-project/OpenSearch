/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.common.Nullable;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.shard.IndexShard;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Data-node service that executes plan fragments against local shards.
 * Acquires a reader from the shard's composite engine, builds an
 * {@link ExecutionContext}, and invokes the backend's {@link SearchExecEngine}
 * to produce results.
 *
 * <p>Does NOT hold {@code IndicesService} — receives an already-resolved
 * {@link IndexShard} from the transport action.
 *
 * @opensearch.internal
 */
public class AnalyticsSearchService {

    private final Map<String, AnalyticsSearchBackendPlugin> backends;

    public AnalyticsSearchService(Map<String, AnalyticsSearchBackendPlugin> backends) {
        this.backends = backends;
    }

    /**
     * Executes a plan fragment against the given shard and returns the collected results.
     *
     * @param request the fragment execution request
     * @param shard   the already-resolved index shard
     * @return a response containing field names and result rows
     */
    public ScanResponse executeFragment(FragmentExecutionRequest request, IndexShard shard) {
        return executeFragment(request, shard, null);
    }

    /**
     * Executes a plan fragment against the given shard and returns the collected results,
     * polling the shard task for cancellation between batches.
     *
     * @param request the fragment execution request
     * @param shard   the already-resolved index shard
     * @param task    the shard task to poll for cancellation (nullable)
     * @return a response containing field names and result rows
     */
    public ScanResponse executeFragment(FragmentExecutionRequest request, IndexShard shard, AnalyticsShardTask task) {
        DataFormatAwareEngine compositeEngine = shard.getCompositeEngine();
        if (compositeEngine == null) {
            throw new IllegalStateException("No CompositeEngine on " + shard.shardId());
        }

        // Select the first available plan alternative whose backend is registered on this node.
        // TODO: smarter selection based on data node capabilities/load
        FragmentExecutionRequest.PlanAlternative selectedPlan = null;
        for (FragmentExecutionRequest.PlanAlternative alt : request.getPlanAlternatives()) {
            if (backends.containsKey(alt.getBackendId())) {
                selectedPlan = alt;
                break;
            }
        }
        if (selectedPlan == null) {
            throw new IllegalArgumentException(
                "No plan alternative matches available backends. Alternatives: "
                    + request.getPlanAlternatives().stream().map(FragmentExecutionRequest.PlanAlternative::getBackendId).toList()
                    + ". Available: "
                    + backends.keySet()
            );
        }

        try (GatedCloseable<Reader> gatedReader = compositeEngine.acquireReader()) {
            SearchShardTask searchShardTask = null; // TODO: real task for cancellation
            ExecutionContext ctx = new ExecutionContext(request.getShardId().getIndexName(), searchShardTask, gatedReader.get());
            ctx.setFragmentBytes(selectedPlan.getFragmentBytes());

            AnalyticsSearchBackendPlugin backend = backends.get(selectedPlan.getBackendId());

            // createSearchExecEngine calls prepare() internally — do NOT call prepare() again
            try (SearchExecEngine<ExecutionContext, EngineResultStream> engine = backend.createSearchExecEngine(ctx)) {
                try (EngineResultStream stream = engine.execute(ctx)) {
                    return collectResponse(stream, task);
                }
            }
        } catch (TaskCancelledException e) {
            throw e; // do NOT wrap — preserve type
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute fragment on " + shard.shardId(), e);
        }
    }

    /**
     * Collects all batches from the result stream into a single {@link ScanResponse}.
     * Field names are captured from the first batch.
     */
    ScanResponse collectResponse(EngineResultStream stream) {
        return collectResponse(stream, null);
    }

    /**
     * Collects all batches from the result stream into a single {@link ScanResponse}.
     * Field names are captured from the first batch. Polls the shard task for cancellation
     * at each batch boundary.
     *
     * @param stream the result stream to drain
     * @param task   the shard task to poll for cancellation (nullable)
     */
    ScanResponse collectResponse(EngineResultStream stream, @Nullable AnalyticsShardTask task) {
        List<Object[]> rows = new ArrayList<>();
        List<String> fieldNames = null;
        Iterator<EngineResultBatch> it = stream.iterator();
        while (it.hasNext()) {
            if (task != null && task.isCancelled()) {
                throw new TaskCancelledException("task cancelled: " + task.getReasonCancelled());
            }
            EngineResultBatch batch = it.next();
            if (fieldNames == null) {
                fieldNames = batch.getFieldNames();
            }
            for (int row = 0; row < batch.getRowCount(); row++) {
                Object[] vals = new Object[fieldNames.size()];
                for (int col = 0; col < fieldNames.size(); col++) {
                    vals[col] = batch.getFieldValue(fieldNames.get(col), row);
                }
                rows.add(vals);
            }
        }
        return new ScanResponse(fieldNames != null ? fieldNames : List.of(), rows);
    }
}

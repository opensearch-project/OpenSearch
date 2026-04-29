/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.RootAllocator;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.common.Nullable;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.shard.IndexShard;

import java.io.IOException;
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
 * <p>Owns a service-lifetime {@link RootAllocator} shared by every fragment —
 * both the row-path {@link #executeFragment} and the streaming
 * {@link #executeFragmentStreaming}. One allocator per service means memory
 * accounting is reported at the service level, and — because the allocator
 * outlives any single request — no per-request close-time leak check fires.
 * For the streaming path, Arrow Flight's outbound handler co-locates its
 * transfer target on this same allocator
 * (see {@code FlightOutboundHandler#processBatchTask}), keeping transfers
 * same-allocator and avoiding the known cross-allocator bug with
 * foreign-backed buffers from the C Data Interface.
 *
 * @opensearch.internal
 */
public class AnalyticsSearchService implements AutoCloseable {

    private final Map<String, AnalyticsSearchBackendPlugin> backends;
    private final AnalyticsOperationListener listener;
    private final RootAllocator allocator;

    public AnalyticsSearchService(Map<String, AnalyticsSearchBackendPlugin> backends) {
        this(backends, List.of());
    }

    public AnalyticsSearchService(Map<String, AnalyticsSearchBackendPlugin> backends, List<AnalyticsOperationListener> listeners) {
        this.backends = backends;
        this.listener = new AnalyticsOperationListener.CompositeListener(listeners);
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void close() {
        allocator.close();
    }

    public FragmentExecutionResponse executeFragment(FragmentExecutionRequest request, IndexShard shard) {
        return executeFragment(request, shard, null);
    }

    public FragmentExecutionResponse executeFragment(FragmentExecutionRequest request, IndexShard shard, AnalyticsShardTask task) {
        ResolvedFragment resolved = resolveFragment(request, shard);
        long startNanos = System.nanoTime();
        try (FragmentResources ctx = startFragment(request, resolved)) {
            FragmentExecutionResponse response = collectResponse(ctx.stream(), task);
            long tookNanos = System.nanoTime() - startNanos;
            listener.onFragmentSuccess(resolved.queryId, resolved.stageId, resolved.shardIdStr, tookNanos, response.getRows().size());
            return response;
        } catch (TaskCancelledException | IllegalStateException | IllegalArgumentException e) {
            listener.onFragmentFailure(resolved.queryId, resolved.stageId, resolved.shardIdStr, e);
            throw e;
        } catch (Exception e) {
            listener.onFragmentFailure(resolved.queryId, resolved.stageId, resolved.shardIdStr, e);
            throw new RuntimeException("Failed to execute fragment on " + shard.shardId(), e);
        }
    }

    public FragmentResources executeFragmentStreaming(FragmentExecutionRequest request, IndexShard shard) {
        ResolvedFragment resolved = resolveFragment(request, shard);
        try {
            return startFragment(request, resolved);
        } catch (TaskCancelledException | IllegalStateException | IllegalArgumentException e) {
            listener.onFragmentFailure(resolved.queryId, resolved.stageId, resolved.shardIdStr, e);
            throw e;
        } catch (Exception e) {
            listener.onFragmentFailure(resolved.queryId, resolved.stageId, resolved.shardIdStr, e);
            throw new RuntimeException("Failed to start streaming fragment on " + shard.shardId(), e);
        }
    }

    private FragmentResources startFragment(FragmentExecutionRequest request, ResolvedFragment resolved) throws IOException {
        GatedCloseable<Reader> gatedReader = resolved.readerProvider.acquireReader();
        SearchExecEngine<ExecutionContext, EngineResultStream> engine = null;
        EngineResultStream stream = null;
        try {
            ExecutionContext ctx = buildContext(request, gatedReader.get(), resolved.plan);
            AnalyticsSearchBackendPlugin backend = backends.get(resolved.plan.getBackendId());
            engine = backend.getSearchExecEngineProvider().createSearchExecEngine(ctx);
            stream = engine.execute(ctx);
            return new FragmentResources(gatedReader, engine, stream);
        } catch (Exception e) {
            try {
                new FragmentResources(gatedReader, engine, stream).close();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private record ResolvedFragment(IndexReaderProvider readerProvider, FragmentExecutionRequest.PlanAlternative plan, String queryId,
        int stageId, String shardIdStr) {
    }

    private ResolvedFragment resolveFragment(FragmentExecutionRequest request, IndexShard shard) {
        IndexReaderProvider readerProvider = shard.getReaderProvider();
        if (readerProvider == null) {
            throw new IllegalStateException("No ReaderProvider on " + shard.shardId());
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

        String shardIdStr = shard.shardId().toString();
        listener.onPreFragmentExecution(request.getQueryId(), request.getStageId(), shardIdStr);
        return new ResolvedFragment(readerProvider, selectedPlan, request.getQueryId(), request.getStageId(), shardIdStr);
    }

    private ExecutionContext buildContext(FragmentExecutionRequest request, Reader reader, FragmentExecutionRequest.PlanAlternative plan) {
        SearchShardTask searchShardTask = null; // TODO: real task for cancellation
        ExecutionContext ctx = new ExecutionContext(request.getShardId().getIndexName(), searchShardTask, reader);
        ctx.setFragmentBytes(plan.getFragmentBytes());
        ctx.setAllocator(allocator);
        return ctx;
    }

    FragmentExecutionResponse collectResponse(EngineResultStream stream) {
        return collectResponse(stream, null);
    }

    FragmentExecutionResponse collectResponse(EngineResultStream stream, @Nullable AnalyticsShardTask task) {
        List<Object[]> rows = new ArrayList<>();
        List<String> fieldNames = null;
        Iterator<EngineResultBatch> it = stream.iterator();
        while (it.hasNext()) {
            if (task != null && task.isCancelled()) {
                throw new TaskCancelledException("task cancelled: " + task.getReasonCancelled());
            }
            EngineResultBatch batch = it.next();
            try {
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
            } finally {
                batch.getArrowRoot().close();
            }
        }
        return new FragmentExecutionResponse(fieldNames != null ? fieldNames : List.of(), rows);
    }
}

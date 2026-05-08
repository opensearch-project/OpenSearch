/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.DelegationDescriptor;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.arrow.flight.transport.ArrowAllocatorProvider;
import org.opensearch.common.Nullable;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Data-node service that executes plan fragments against local shards.
 * Acquires a reader from the shard's composite engine, builds an
 * {@link ShardScanExecutionContext}, and invokes the backend's {@link SearchExecEngine}
 * to produce results.
 *
 * <p>Does NOT hold {@code IndicesService} — receives an already-resolved
 * {@link IndexShard} from the transport action.
 *
 * <p>Owns a service-lifetime {@link BufferAllocator} shared by every fragment, obtained as a child of the
 * node-level root via {@link ArrowAllocatorProvider}. One allocator per service means memory accounting is
 * reported at the service level. For the streaming path, Arrow Flight's outbound handler co-locates its
 * transfer target on the same root (see {@code FlightOutboundHandler#processBatchTask}), keeping transfers
 * same-root and avoiding the known cross-allocator bug with foreign-backed buffers from the C Data Interface.
 *
 * @opensearch.internal
 */
public class AnalyticsSearchService implements AutoCloseable {

    private final Map<String, AnalyticsSearchBackendPlugin> backends;
    private final AnalyticsOperationListener listener;
    private final BufferAllocator allocator;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public AnalyticsSearchService(Map<String, AnalyticsSearchBackendPlugin> backends) {
        this(backends, List.of(), null);
    }

    public AnalyticsSearchService(Map<String, AnalyticsSearchBackendPlugin> backends, NamedWriteableRegistry namedWriteableRegistry) {
        this(backends, List.of(), namedWriteableRegistry);
    }

    public AnalyticsSearchService(
        Map<String, AnalyticsSearchBackendPlugin> backends,
        List<AnalyticsOperationListener> listeners,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.backends = backends;
        this.listener = new AnalyticsOperationListener.CompositeListener(listeners);
        this.allocator = ArrowAllocatorProvider.newChildAllocator("analytics-search-service", Long.MAX_VALUE);
        this.namedWriteableRegistry = namedWriteableRegistry;
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
        try (FragmentResources ctx = startFragment(request, resolved, shard, task)) {
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

    public FragmentResources executeFragmentStreaming(FragmentExecutionRequest request, IndexShard shard, AnalyticsShardTask task) {
        ResolvedFragment resolved = resolveFragment(request, shard);
        try {
            return startFragment(request, resolved, shard, task);
        } catch (TaskCancelledException | IllegalStateException | IllegalArgumentException e) {
            listener.onFragmentFailure(resolved.queryId, resolved.stageId, resolved.shardIdStr, e);
            throw e;
        } catch (Exception e) {
            listener.onFragmentFailure(resolved.queryId, resolved.stageId, resolved.shardIdStr, e);
            throw new RuntimeException("Failed to start streaming fragment on " + shard.shardId(), e);
        }
    }

    private FragmentResources startFragment(FragmentExecutionRequest request, ResolvedFragment resolved, IndexShard shard, Task task)
        throws IOException {
        GatedCloseable<Reader> gatedReader = resolved.readerProvider.acquireReader();
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine = null;
        EngineResultStream stream = null;
        BackendExecutionContext backendContext = null;
        try {
            ShardScanExecutionContext ctx = buildContext(request, gatedReader.get(), resolved.plan, shard, task);
            AnalyticsSearchBackendPlugin backend = backends.get(resolved.plan.getBackendId());

            // Apply instruction handlers in order — each builds upon the previous handler's backend context
            List<InstructionNode> instructions = resolved.plan.getInstructions();
            if (!instructions.isEmpty()) {
                FragmentInstructionHandlerFactory factory = backend.getInstructionHandlerFactory();
                for (InstructionNode node : instructions) {
                    FragmentInstructionHandler handler = factory.createHandler(node);
                    backendContext = handler.apply(node, ctx, backendContext);
                }
            }

            // Handle exchange — if plan has delegation, ask accepting backend for handle and pass to driving
            // TODO: currently assumes single accepting backend. When multiple accepting backends exist
            // (e.g., Lucene + Tantivy), group expressions by acceptingBackendId and create one handle per group.
            DelegationDescriptor delegation = resolved.plan.getDelegationDescriptor();
            if (delegation != null) {
                String acceptingBackendId = delegation.delegatedExpressions().getFirst().getAcceptingBackendId();
                AnalyticsSearchBackendPlugin acceptingBackend = backends.get(acceptingBackendId);
                FilterDelegationHandle handle = acceptingBackend.getFilterDelegationHandle(delegation.delegatedExpressions(), ctx);
                backend.configureFilterDelegation(handle, backendContext);
            }

            engine = backend.getSearchExecEngineProvider().createSearchExecEngine(ctx, backendContext);
            stream = engine.execute(ctx);
            return new FragmentResources(gatedReader, engine, stream);
        } catch (Exception e) {
            try {
                new FragmentResources(gatedReader, engine, stream).close();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            // Close the backend execution context as a safety net for failure paths that
            // never reached / never finished the engine construction — if the handle was
            // already transferred, close() is a no-op (implementations must be idempotent).
            if (backendContext != null) {
                try {
                    backendContext.close();
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
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

    private ShardScanExecutionContext buildContext(
        FragmentExecutionRequest request,
        Reader reader,
        FragmentExecutionRequest.PlanAlternative plan,
        IndexShard shard,
        Task task
    ) {
        ShardScanExecutionContext ctx = new ShardScanExecutionContext(request.getShardId().getIndexName(), task, reader);
        ctx.setFragmentBytes(plan.getFragmentBytes());
        ctx.setAllocator(allocator);
        ctx.setMapperService(shard.mapperService());
        ctx.setIndexSettings(shard.indexSettings());
        ctx.setNamedWriteableRegistry(namedWriteableRegistry);
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

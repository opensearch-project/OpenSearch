/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.canmatch.AnalyticsCanMatchResponse;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.DelegationDescriptor;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Data-node service that executes plan fragments against local shards.
 * Acquires a reader from the shard's composite engine, builds an
 * {@link ShardScanExecutionContext}, and invokes the backend's {@link SearchExecEngine}
 * to produce results.
 *
 * <p>Does NOT hold {@code IndicesService} — receives an already-resolved
 * {@link IndexShard} from the transport action.
 *
 * <p>Owns a service-lifetime {@link BufferAllocator} shared by every fragment, obtained as a child of
 * the framework's QUERY pool via {@link ArrowNativeAllocator#getPoolAllocator(String)}. One allocator
 * per service means memory accounting is reported at the service level. For the streaming path, Arrow
 * Flight's outbound handler co-locates its transfer target on the same root (see
 * {@code FlightOutboundHandler#processBatchTask}), keeping transfers same-root and avoiding the known
 * cross-allocator bug with foreign-backed buffers from the C Data Interface.
 *
 * @opensearch.internal
 */
public class AnalyticsSearchService implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsSearchService.class);

    private final Map<String, AnalyticsSearchBackendPlugin> backends;
    private final AnalyticsOperationListener listener;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private TaskResourceTrackingService taskResourceTrackingService;
    private final BufferAllocator allocator;
    private final ArrowNativeAllocator nativeAllocator;

    public AnalyticsSearchService(Map<String, AnalyticsSearchBackendPlugin> backends, ArrowNativeAllocator nativeAllocator) {
        this(backends, List.of(), nativeAllocator, null);
    }

    public AnalyticsSearchService(
        Map<String, AnalyticsSearchBackendPlugin> backends,
        ArrowNativeAllocator nativeAllocator,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this(backends, List.of(), nativeAllocator, namedWriteableRegistry);
    }

    public AnalyticsSearchService(
        Map<String, AnalyticsSearchBackendPlugin> backends,
        List<AnalyticsOperationListener> listeners,
        ArrowNativeAllocator nativeAllocator,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.backends = backends;
        this.listener = new AnalyticsOperationListener.CompositeListener(listeners);
        this.nativeAllocator = nativeAllocator;
        // Source the service-level allocator from the unified framework's query pool so all
        // analytics-engine allocations are tracked and capped by the framework. Hard-fail if
        // the framework is missing — silently falling back to a separate root would break
        // Arrow's same-root invariant for cross-plugin handoff.
        //
        // Child uses Long.MAX_VALUE so dynamic resizes of parquet.native.pool.query.max take
        // effect immediately via Arrow's parent-cap check at allocateBytes — no listener needed.
        BufferAllocator queryPool = nativeAllocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_QUERY);
        this.allocator = queryPool.newChildAllocator("analytics-search-service", 0, Long.MAX_VALUE);
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    public void close() {
        allocator.close();
    }

    public void setTaskResourceTrackingService(TaskResourceTrackingService service) {
        this.taskResourceTrackingService = service;
    }

    /**
     * Evaluates can-match for a shard by delegating to the backend which has
     * the shard's file layout (via DatafusionReader / shard-view) and can check
     * Parquet row-group statistics against the filter predicate.
     *
     * <p>The backend (DataFusion) already knows which Parquet segment files belong
     * to the shard via its reader creation path. It iterates those files and calls
     * the native can-match evaluator (Rust) for each.
     */
    public AnalyticsCanMatchResponse canMatch(IndexShard shard, byte[] filterBytes, String backendId) {
        if (filterBytes == null || filterBytes.length == 0) {
            return AnalyticsCanMatchResponse.YES;
        }
        AnalyticsSearchBackendPlugin backend = backends.get(backendId);
        if (backend == null) {
            return AnalyticsCanMatchResponse.YES;
        }
        try {
            // TODO: pass shard path or ReaderProvider to the backend so it can resolve files.
            // The DataFusion backend uses its CustomCacheManager which already has the file
            // metadata cached keyed by path. The shard's data directory gives the base path;
            // the backend lists segment .parquet files under it.
            boolean matches = backend.canMatch(null, filterBytes);
            return matches ? AnalyticsCanMatchResponse.YES : AnalyticsCanMatchResponse.NO;
        } catch (Exception e) {
            LOGGER.warn("can-match evaluation failed for shard [{}], conservatively returning true", shard.shardId(), e);
            return AnalyticsCanMatchResponse.YES;
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

    /**
     * Async variant that forks fragment execution onto the given executor and streams
     * batches back through the channel. The transport thread returns immediately.
     */
    public void executeFragmentStreamingAsync(
        FragmentExecutionRequest request,
        IndexShard shard,
        AnalyticsShardTask task,
        StreamingFragmentResponseHandler responseHandler,
        Executor executor
    ) {
        try {
            executor.execute(() -> {
                try (FragmentResources ctx = executeFragmentStreaming(request, shard, task)) {
                    Iterator<EngineResultBatch> it = ctx.stream().iterator();
                    while (it.hasNext()) {
                        responseHandler.onBatch(it.next());
                    }
                    responseHandler.onComplete();
                } catch (Exception e) {
                    responseHandler.onFailure(e);
                }
            });
        } catch (Exception e) {
            responseHandler.onFailure(e);
        }
    }

    /**
     * Callback interface for async fragment streaming results.
     */
    public interface StreamingFragmentResponseHandler {
        void onBatch(EngineResultBatch batch) throws Exception;

        void onComplete();

        void onFailure(Exception e);
    }

    private FragmentResources startFragment(FragmentExecutionRequest request, ResolvedFragment resolved, IndexShard shard, Task task)
        throws IOException {
        GatedCloseable<Reader> gatedReader = resolved.readerProvider.acquireReader();
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine = null;
        EngineResultStream stream = null;
        BackendExecutionContext backendContext = null;
        Runnable trackerCleanup = null;
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

                if (task != null && taskResourceTrackingService != null) {
                    long taskId = task.getId();
                    TaskResourceTrackingService service = taskResourceTrackingService;
                    backend.setDelegationThreadTracker(new DelegationThreadTracker() {
                        @Override
                        public long trackStart() {
                            long threadId = Thread.currentThread().threadId();
                            service.taskExecutionStartedOnThread(taskId, threadId);
                            return threadId;
                        }

                        @Override
                        public void trackEnd(long threadId) {
                            service.taskExecutionFinishedOnThread(taskId, threadId);
                        }
                    });
                    trackerCleanup = () -> backend.setDelegationThreadTracker(null);
                }
            }

            engine = backend.getSearchExecEngineProvider().createSearchExecEngine(ctx, backendContext);
            stream = engine.execute(ctx);
            return new FragmentResources(gatedReader, engine, stream, trackerCleanup);
        } catch (Exception e) {
            LOGGER.error(
                () -> new org.apache.logging.log4j.message.ParameterizedMessage(
                    "startFragment failed [queryId={}, stageId={}, shardId={}]",
                    resolved.queryId,
                    resolved.stageId,
                    resolved.shardIdStr
                ),
                e
            );
            try {
                new FragmentResources(gatedReader, engine, stream, trackerCleanup).close();
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

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.FragmentExecutionStats;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.exec.action.FetchByRowIdsRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.DelegationDescriptor;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    /** Cross-phase reader cache for QTF — query phase stores, fetch phase acquires. */
    private final ReaderContextStore readerContextStore;
    private TaskResourceTrackingService taskResourceTrackingService;
    private final BufferAllocator allocator;
    private final ArrowNativeAllocator nativeAllocator;

    public AnalyticsSearchService(Map<String, AnalyticsSearchBackendPlugin> backends, ArrowNativeAllocator nativeAllocator) {
        this(backends, List.of(), nativeAllocator, null, null);
    }

    public AnalyticsSearchService(
        Map<String, AnalyticsSearchBackendPlugin> backends,
        ArrowNativeAllocator nativeAllocator,
        NamedWriteableRegistry namedWriteableRegistry,
        ReaderContextStore readerContextStore
    ) {
        this(backends, List.of(), nativeAllocator, namedWriteableRegistry, readerContextStore);
    }

    public AnalyticsSearchService(
        Map<String, AnalyticsSearchBackendPlugin> backends,
        List<AnalyticsOperationListener> listeners,
        ArrowNativeAllocator nativeAllocator,
        NamedWriteableRegistry namedWriteableRegistry,
        ReaderContextStore readerContextStore
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
        this.readerContextStore = readerContextStore;
    }

    @Override
    public void close() {
        allocator.close();
    }

    public void setTaskResourceTrackingService(TaskResourceTrackingService service) {
        this.taskResourceTrackingService = service;
    }

    public FragmentResources executeFragmentStreaming(FragmentExecutionRequest request, IndexShard shard, AnalyticsShardTask task) {
        return executeFragmentStreamingResolved(request, shard, task).resources;
    }

    private ResolvedExecution executeFragmentStreamingResolved(
        FragmentExecutionRequest request,
        IndexShard shard,
        AnalyticsShardTask task
    ) {
        ResolvedFragment resolved = resolveFragment(request, shard);
        try {
            FragmentResources resources = startFragment(request, resolved, shard, task);
            return new ResolvedExecution(resources, resolved);
        } catch (TaskCancelledException | IllegalStateException | IllegalArgumentException e) {
            listener.onFragmentFailure(resolved.queryId, resolved.stageId, resolved.shardIdStr, e);
            throw e;
        } catch (OpenSearchException e) {
            listener.onFragmentFailure(resolved.queryId, resolved.stageId, resolved.shardIdStr, e);
            throw e;
        } catch (Exception e) {
            listener.onFragmentFailure(resolved.queryId, resolved.stageId, resolved.shardIdStr, e);
            // Log the original failure with its full stack at the origin: the thrown exception is handed
            // to async dispatch / stream transport and can be re-wrapped or swallowed downstream, so this
            // is the one place guaranteed to see the real cause and stack.
            LOGGER.warn(
                new ParameterizedMessage(
                    "[FragmentExecution] failed to start streaming fragment on shard={} queryId={} stageId={}",
                    shard.shardId(),
                    resolved.queryId,
                    resolved.stageId
                ),
                e
            );
            // Convert native errors (e.g. memory-pool / admission trips) to typed exceptions via the
            // ACTUALLY-SELECTED backend so a resource-exhaustion failure surfaces as 429 instead of a
            // generic 500. Only wrap as a generic RuntimeException when conversion found nothing.
            Exception converted = convertWith(resolved.plan().getBackendId(), e);
            if (converted != e) {
                throw converted instanceof RuntimeException re ? re : new RuntimeException(converted);
            }
            throw new RuntimeException("Failed to start streaming fragment on " + shard.shardId(), e);
        }
    }

    /** Converts {@code e} via the named backend's exception SPI; returns {@code e} unchanged if the backend is absent or doesn't recognize it. */
    private Exception convertWith(String backendId, Exception e) {
        AnalyticsSearchBackendPlugin backend = backends.get(backendId);
        return backend == null ? e : backend.convertException(e);
    }

    private record ResolvedExecution(FragmentResources resources, ResolvedFragment resolved) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            resources.close();
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
                LOGGER.debug("[FragmentExecution] shard={} task={}", shard.shardId(), task.getId());
                final long startNanos = System.nanoTime();
                long rowsProduced = 0;
                try (ResolvedExecution exec = executeFragmentStreamingResolved(request, shard, task)) {
                    Iterator<EngineResultBatch> it = exec.resources().stream().iterator();
                    while (it.hasNext()) {
                        EngineResultBatch batch = it.next();
                        rowsProduced += batch.getRowCount();
                        responseHandler.onBatch(batch);
                    }
                    long fragmentTookNanos = System.nanoTime() - startNanos;
                    // Extract DataFusion execution metrics only when needed
                    if (request.profile() || LOGGER.isDebugEnabled()) {
                        byte[] metricsJson = exec.resources().getExecutionMetrics();
                        if (LOGGER.isDebugEnabled() && metricsJson != null) {
                            LOGGER.debug(
                                "[FragmentMetrics] shard={} metrics={}",
                                shard.shardId(),
                                new String(metricsJson, StandardCharsets.UTF_8)
                            );
                        }
                        if (request.profile() && metricsJson != null) {
                            responseHandler.onCompleteWithMetrics(metricsJson);
                        } else {
                            responseHandler.onComplete();
                        }
                    } else {
                        responseHandler.onComplete();
                    }
                    ResolvedFragment resolved = exec.resolved();
                    DelegationDescriptor delegation = resolved.plan().getDelegationDescriptor();
                    boolean usedSecondaryIndex = delegation != null;
                    int delegatedPredicateCount = delegation != null ? delegation.delegatedPredicateCount() : 0;
                    String filterTreeShape = delegation != null ? delegation.treeShape().name() : null;
                    boolean hasPartialAggregate = resolved.plan()
                        .getInstructions()
                        .stream()
                        .anyMatch(n -> n.type() == org.opensearch.analytics.spi.InstructionType.SETUP_PARTIAL_AGGREGATE);
                    FragmentExecutionStats stats = new FragmentExecutionStats(
                        rowsProduced,
                        usedSecondaryIndex,
                        delegatedPredicateCount,
                        filterTreeShape,
                        hasPartialAggregate,
                        task.getId(),
                        task.getHeader(Task.X_OPAQUE_ID)
                    );
                    listener.onFragmentSuccess(
                        request.getQueryId(),
                        request.getStageId(),
                        shard.shardId().toString(),
                        fragmentTookNanos,
                        shard.indexSettings(),
                        stats
                    );
                } catch (Exception e) {
                    // Query phase failed: no fetch will follow, so free the reader eagerly (no-op if already freed).
                    readerContextStore.freeContext(request.getQueryId(), shard.shardId());
                    responseHandler.onFailure(convertWith(selectedBackendId(request), e));
                }
            });
        } catch (Exception e) {
            responseHandler.onFailure(e);
        }
    }

    /**
     * QTF fetch phase: retrieves specific rows by global row ID via the backend SPI and
     * streams batches via {@link StreamingFragmentResponseHandler}. Forks onto
     * {@code executor} so the iterator drain doesn't pin the transport thread — mirrors
     * {@link #executeFragmentStreamingAsync}.
     *
     * <p>Reuses the {@link ReaderContext} opened during the query phase. If the context
     * is missing (expired before fetch arrived, or query-phase reader-store invariant
     * broken), the call fails — there is no cold-start fallback because shard-global
     * {@code __row_id__} values produced by one reader cannot be reinterpreted by
     * another (segment topology may differ across reopens).
     */
    public void executeFetchByRowIdsAsync(
        FetchByRowIdsRequest request,
        IndexShard shard,
        AnalyticsShardTask task,
        StreamingFragmentResponseHandler responseHandler,
        Executor executor
    ) {
        try {
            executor.execute(() -> drainFetchByRowIds(request, shard, task, responseHandler));
        } catch (Exception e) {
            responseHandler.onFailure(e);
        }
    }

    /**
     * Acquires the per-shard {@link ReaderContext}, materialises the rowId vector, invokes
     * the backend, drains the stream into {@code responseHandler}, and releases all resources
     * in a single try-with-resources scope. Runs on the caller's executor — exhausting the
     * iterator here lets the native engine apply backpressure when the channel is slow.
     */
    private void drainFetchByRowIds(
        FetchByRowIdsRequest request,
        IndexShard shard,
        AnalyticsShardTask task,
        StreamingFragmentResponseHandler responseHandler
    ) {
        assert task != null : "fetch on " + shard.shardId() + " requires a non-null AnalyticsShardTask";
        if (task.isCancelled()) {
            responseHandler.onFailure(new TaskCancelledException("Fetch task cancelled before execution: " + task.getReasonCancelled()));
            return;
        }
        long[] rowIds = request.getRowIds();
        String[] columns = request.getColumns();
        if (rowIds == null || rowIds.length == 0 || columns == null || columns.length == 0) {
            responseHandler.onFailure(
                new IllegalArgumentException(
                    "fetch on "
                        + shard.shardId()
                        + " requires non-empty rowIds and columns; got rowIds="
                        + (rowIds == null ? "null" : rowIds.length)
                        + ", columns="
                        + (columns == null ? "null" : columns.length)
                )
            );
            return;
        }
        ReaderContext readerContext = readerContextStore.acquireContext(request.getQueryId(), shard.shardId());
        if (readerContext == null) {
            responseHandler.onFailure(
                new IllegalStateException(
                    "No ReaderContext for queryId="
                        + request.getQueryId()
                        + " on "
                        + shard.shardId()
                        + " — query phase missing or context expired"
                )
            );
            return;
        }
        assert assertFetchInvariants(readerContext, request.getQueryId());
        AnalyticsSearchBackendPlugin backend = backends.get(request.getBackendId());
        if (backend == null) {
            // Fetch is terminal: free the reader eagerly.
            readerContextStore.releaseAndFree(request.getQueryId(), shard.shardId());
            responseHandler.onFailure(
                new IllegalStateException(
                    "No backend registered for backendId="
                        + request.getBackendId()
                        + " on "
                        + shard.shardId()
                        + "; available: "
                        + backends.keySet()
                )
            );
            return;
        }
        // Caller contract: rowIds must already be sorted ascending (RowSelection invariant on
        // native side). Asserted here so violations are caught in dev builds before the FFM call.
        assert assertAscending(rowIds);
        BigIntVector rowIdVector = null;
        FragmentResources resources = null;
        try {
            rowIdVector = new BigIntVector(DocumentInput.ROW_ID_FIELD, allocator);
            rowIdVector.allocateNew(rowIds.length);
            for (int i = 0; i < rowIds.length; i++) {
                rowIdVector.set(i, rowIds[i]);
            }
            rowIdVector.setValueCount(rowIds.length);
            EngineResultStream stream = backend.fetchByRowIds(readerContext.getReader(), rowIdVector, columns, allocator, task.getId());
            // FragmentResources keeps the rowIdVector alive until the stream drains — closing
            // it earlier would pull off-heap memory out from under the native FFM call.
            // Fetch is the terminal phase: no fetch follows, so close() frees the reader eagerly.
            resources = new FragmentResources(readerContextStore, readerContext, null, stream, null, rowIdVector, false);
        } catch (OpenSearchException e) {
            if (rowIdVector != null) rowIdVector.close();
            // Fetch is terminal: free the reader eagerly.
            readerContextStore.releaseAndFree(request.getQueryId(), shard.shardId());
            responseHandler.onFailure(e);
            return;
        } catch (Exception e) {
            if (rowIdVector != null) rowIdVector.close();
            // Fetch is terminal: free the reader eagerly.
            readerContextStore.releaseAndFree(request.getQueryId(), shard.shardId());
            Exception converted = backend.convertException(e);
            responseHandler.onFailure(
                converted == e ? new RuntimeException("Failed to execute fetch-by-row-ids on " + shard.shardId(), e) : converted
            );
            return;
        }
        // On cancel, release a fetch parked in the native pull via cooperative cancellation, not
        // stream.close() (which would race the in-flight native pull).
        task.setCancellationListener(() -> backend.cancelByContext(task.getId()));
        try (FragmentResources ctx = resources) {
            Iterator<EngineResultBatch> it = ctx.stream().iterator();
            while (it.hasNext()) {
                responseHandler.onBatch(it.next());
            }
            responseHandler.onComplete();
        } catch (Exception e) {
            responseHandler.onFailure(backend.convertException(e));
        } finally {
            task.clearCancellationListener();
        }
    }

    /**
     * Callback interface for async fragment streaming results.
     */
    public interface StreamingFragmentResponseHandler {
        void onBatch(EngineResultBatch batch) throws Exception;

        void onComplete();

        /** Called with execution metrics when profiling is enabled. Default delegates to onComplete(). */
        default void onCompleteWithMetrics(byte[] metrics) {
            onComplete();
        }

        void onFailure(Exception e);
    }

    private FragmentResources startFragment(FragmentExecutionRequest request, ResolvedFragment resolved, IndexShard shard, Task task)
        throws IOException {
        GatedCloseable<Reader> gatedReader = resolved.readerProvider.acquireReader();
        // A query that requested top-N docs (row-ids) will be followed by a fetch phase that reuses
        // this reader. When it does, close() keeps the reader in the store for the fetch; otherwise
        // close() frees it immediately instead of waiting for the reaper.
        // TODO: the coordinator (which knows the query shape) should tell us whether a fetch
        // follows, rather than us inferring it from the row-id signal here.
        boolean requiresTopDocs = requestsRowIds(resolved.plan.getInstructions());
        ReaderContext readerContext = readerContextStore.createContext(request.getQueryId(), shard.shardId(), gatedReader);
        assert assertReaderInvariants(gatedReader, readerContext, request.getQueryId(), shard);
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine = null;
        EngineResultStream stream = null;
        BackendExecutionContext backendContext = null;
        Runnable trackerCleanup = null;
        try {
            ShardScanExecutionContext ctx = buildContext(request, readerContext.getReader(), resolved.plan, shard, task);
            ctx.setHasPartialAggregate(
                resolved.plan.getInstructions()
                    .stream()
                    .anyMatch(n -> n.type() == org.opensearch.analytics.spi.InstructionType.SETUP_PARTIAL_AGGREGATE)
            );
            AnalyticsSearchBackendPlugin backend = backends.get(resolved.plan.getBackendId());

            backendContext = applyInstructionHandlers(backend, resolved.plan.getInstructions(), ctx);

            // Handle exchange — if plan has delegation, ask accepting backend for handle and pass to driving
            // TODO: currently assumes single accepting backend. When multiple accepting backends exist
            // (e.g., Lucene + Tantivy), group expressions by acceptingBackendId and create one handle per group.
            DelegationDescriptor delegation = resolved.plan.getDelegationDescriptor();
            if (delegation != null) {
                // Filter delegation routes per-query state via taskId; without a task we cannot
                // isolate concurrent queries from each other. Validate before allocating any
                // delegation resources to avoid leaks.
                if (task == null) {
                    throw new IllegalStateException("Filter delegation requires a tracked task for per-query isolation");
                }
                long contextId = task.getId();

                String acceptingBackendId = delegation.delegatedExpressions().getFirst().getAcceptingBackendId();
                AnalyticsSearchBackendPlugin acceptingBackend = backends.get(acceptingBackendId);
                FilterDelegationHandle handle = acceptingBackend.getFilterDelegationHandle(delegation.delegatedExpressions(), ctx);

                // Build a thread tracker when task resource tracking is available.
                DelegationThreadTracker tracker = null;
                if (taskResourceTrackingService != null) {
                    long taskId = task.getId();
                    TaskResourceTrackingService service = taskResourceTrackingService;
                    tracker = new DelegationThreadTracker() {
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
                    };
                }

                // Register handle and tracker together under the query's contextId so concurrent
                // queries have isolated FFM callback bindings. The returned cleanup removes the
                // binding after query execution completes.
                trackerCleanup = backend.configureFilterDelegation(contextId, handle, tracker, backendContext);
            }

            engine = backend.getSearchExecEngineProvider().createSearchExecEngine(ctx, backendContext);
            stream = engine.execute(ctx);
            return new FragmentResources(readerContextStore, readerContext, engine, stream, trackerCleanup, requiresTopDocs);
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
                // Query phase failed: no fetch will follow, so free the reader eagerly (requiresTopDocs=false).
                new FragmentResources(readerContextStore, readerContext, engine, stream, trackerCleanup, false).close();
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

    /**
     * Applies each instruction handler in order. Each handler reads the previous handler's
     * {@link BackendExecutionContext} and returns the next one. Returns {@code null} when the
     * instruction list is empty.
     */
    /**
     * Whether the query phase emits shard-global {@code __row_id__} values — i.e. a QTF query whose
     * fetch phase will reuse this reader. Derived from the {@link ShardScanInstructionNode} the
     * coordinator put in the plan (the same flag that makes the backend emit row ids).
     */
    static boolean requestsRowIds(List<InstructionNode> instructions) {
        for (InstructionNode node : instructions) {
            if (node instanceof ShardScanInstructionNode scan) {
                return scan.requestsRowIds();
            }
        }
        return false;
    }

    private static BackendExecutionContext applyInstructionHandlers(
        AnalyticsSearchBackendPlugin backend,
        List<InstructionNode> instructions,
        ShardScanExecutionContext ctx
    ) {
        if (instructions.isEmpty()) return null;
        FragmentInstructionHandlerFactory factory = backend.getInstructionHandlerFactory();
        BackendExecutionContext backendContext = null;
        for (InstructionNode node : instructions) {
            FragmentInstructionHandler handler = factory.createHandler(node);
            backendContext = handler.apply(node, ctx, backendContext);
        }
        return backendContext;
    }

    private record ResolvedFragment(IndexReaderProvider readerProvider, FragmentExecutionRequest.PlanAlternative plan, String queryId,
        int stageId, String shardIdStr) {
    }

    /**
     * Backend id of the plan alternative {@code request} will actually run — the first whose backend is
     * registered locally. Mirrors {@link #resolveFragment}'s selection so exception conversion uses the
     * same backend that produced the failure. Returns null if none is registered.
     */
    private String selectedBackendId(FragmentExecutionRequest request) {
        for (FragmentExecutionRequest.PlanAlternative alt : request.getPlanAlternatives()) {
            if (backends.containsKey(alt.getBackendId())) {
                return alt.getBackendId();
            }
        }
        return null;
    }

    private ResolvedFragment resolveFragment(FragmentExecutionRequest request, IndexShard shard) {
        IndexReaderProvider readerProvider = shard.getReaderProvider();
        if (readerProvider == null) {
            throw new IllegalStateException("No ReaderProvider on " + shard.shardId());
        }

        // Backend selection happens on the coordinator (PlanAlternativeSelector), so the
        // request typically carries a single alternative. We still iterate to handle the
        // case where a stage genuinely has multiple value-producing alternatives — pick the
        // first one whose backend is registered locally.
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
        // Fallback table name only — the backend derives the actual registration name from the
        // Substrait fragment's NamedTable (which carries the planner's logical alias/pattern name),
        // so this concrete shard index name is used only when no plan is supplied.
        String tableName = request.getShardId().getIndexName();
        ShardScanExecutionContext ctx = new ShardScanExecutionContext(tableName, task, reader);
        ctx.setFragmentBytes(plan.getFragmentBytes());
        ctx.setAllocator(allocator);
        ctx.setMapperService(shard.mapperService());
        ctx.setIndexSettings(shard.indexSettings());
        ctx.setNamedWriteableRegistry(namedWriteableRegistry);
        ctx.setQueryCache(shard.getQueryCache());
        ctx.setQueryCachingPolicy(shard.getQueryCachingPolicy());
        ctx.setShardId(shard.shardId());
        return ctx;
    }

    // ── Assertion helpers (invoked only when -ea is enabled; bodies are dead in production) ──

    private static boolean assertReaderInvariants(
        GatedCloseable<Reader> gatedReader,
        ReaderContext readerContext,
        String queryId,
        IndexShard shard
    ) {
        if (gatedReader == null) {
            throw new AssertionError("acquireReader returned null for shard " + shard.shardId());
        }
        if (readerContext == null) {
            throw new AssertionError("createContext returned null for queryId=" + queryId);
        }
        if (readerContext.getReader() == null) {
            throw new AssertionError("ReaderContext returned null reader for queryId=" + queryId);
        }
        return true;
    }

    private boolean assertFetchInvariants(ReaderContext readerContext, String queryId) {
        if (readerContext.getReader() == null) {
            throw new AssertionError("acquired ReaderContext has null reader for queryId=" + queryId);
        }
        if (backends.isEmpty()) {
            throw new AssertionError("no backends registered — service constructor invariant violated");
        }
        return true;
    }

    private static boolean assertAscending(long[] values) {
        for (int i = 1; i < values.length; i++) {
            if (values[i] < values[i - 1]) {
                throw new AssertionError("rowIds not ascending at index " + i + ": " + values[i - 1] + " > " + values[i]);
            }
        }
        return true;
    }

}

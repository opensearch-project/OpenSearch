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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.FragmentExecutionStats;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.exec.action.FetchByRowIdsRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.WorkerFragmentRequest;
import org.opensearch.analytics.exec.shuffle.ShuffleSenderImpl;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.DelegationDescriptor;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.InstructionType;
import org.opensearch.analytics.spi.ShuffleBufferRegistry;
import org.opensearch.analytics.spi.ShuffleProducerOutputState;
import org.opensearch.analytics.spi.ShuffleSender;
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
    private ShuffleBufferRegistry shuffleBufferRegistry;
    private org.opensearch.transport.client.Client client;
    private org.opensearch.threadpool.ThreadPool threadPool;
    private org.opensearch.cluster.service.ClusterService clusterService;
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

    /**
     * Per-node shuffle buffer registry. Plumbed into every {@link ShardScanExecutionContext} so
     * hash-shuffle scan handlers can reach the buffer slice they need without a hard dependency
     * on the analytics-engine plugin's internals (the registry is an SPI surface). Set once at
     * plugin startup; null until then, in which case hash-shuffle handlers see a null registry
     * and fail-fast with a typed error rather than null-deref.
     */
    public void setShuffleBufferRegistry(ShuffleBufferRegistry registry) {
        this.shuffleBufferRegistry = registry;
    }

    /**
     * Plumb the transport client + thread pool + cluster state needed to construct a
     * {@link org.opensearch.analytics.spi.ShuffleSender} for hash-shuffle producer fragments.
     * Until set, fragments that emit a {@code ShuffleProducerOutputState} fail fast with a typed
     * error (rather than null-deref) so misconfiguration is obvious during plugin startup.
     */
    public void setShuffleSenderDeps(
        org.opensearch.transport.client.Client client,
        org.opensearch.threadpool.ThreadPool threadPool,
        org.opensearch.cluster.service.ClusterService clusterService
    ) {
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
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
        } catch (Exception e) {
            listener.onFragmentFailure(resolved.queryId, resolved.stageId, resolved.shardIdStr, e);
            throw new RuntimeException("Failed to start streaming fragment on " + shard.shardId(), e);
        }
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
     *
     * <p>Two output paths:
     * <ul>
     *   <li><b>Standard path:</b> batches go back to the originating coordinator through
     *       {@code responseHandler.onBatch} / {@code onComplete}.</li>
     *   <li><b>Hash-shuffle producer path:</b> when the fragment's instruction chain produced a
     *       {@code ShuffleProducerOutputState}, the engine's output drains into the partitioned
     *       sink the framework attached to {@link FragmentResources#partitionedSink}; the
     *       response handler still receives a single {@code onComplete} (no batches) so the
     *       coordinator's transport stream closes cleanly. The peer-to-peer shuffle traffic
     *       happens out-of-band on {@code AnalyticsShuffleDataAction}.</li>
     * </ul>
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
                    if (exec.resources().partitionedSink() != null) {
                        // Drain into the partitioned sink AND emit a single header-only
                        // zero-row response on the coordinator-bound stream so it has at
                        // least one Arrow Flight frame (the streaming transport requires ≥1
                        // schema-bearing frame before completeStream — see
                        // DatafusionResultStream's empty-stream synthesis comment). The
                        // response carries no row data (rowsProduced stays 0 on this stream);
                        // producer output reaches the worker peer-to-peer via the shuffle transport.
                        drainAndEmitHeader(exec.resources(), responseHandler);
                    } else {
                        Iterator<EngineResultBatch> it = exec.resources().stream().iterator();
                        while (it.hasNext()) {
                            EngineResultBatch batch = it.next();
                            rowsProduced += batch.getRowCount();
                            responseHandler.onBatch(batch);
                        }
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
                        .anyMatch(n -> n.type() == InstructionType.SETUP_PARTIAL_AGGREGATE);
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
                    responseHandler.onFailure(e);
                }
            });
        } catch (Exception e) {
            responseHandler.onFailure(e);
        }
    }

    /**
     * Drains the engine result stream into the producer's partitioned sink AND emits a
     * single header-only zero-row response on the coordinator-bound stream. Without that
     * frame, the streaming transport never sees an end-of-stream marker (Flight requires
     * ≥1 schema-bearing frame before completeStream), so the coordinator's stage listener
     * never fires onResponse(null) and the producer task hangs.
     *
     * <p>Schema selection: take the schema from the first batch the engine emits and
     * transfer that batch into the sink. If the engine emits zero batches, fall back to an
     * empty schema so the response carries a recognizable Arrow Flight frame.
     */
    private void drainAndEmitHeader(FragmentResources ctx, StreamingFragmentResponseHandler responseHandler) throws Exception {
        drainIntoPartitionedSink(ctx.partitionedSink(), ctx.stream(), responseHandler);
    }

    /**
     * Drains a hash-shuffle producer fragment's engine output into its {@code partitionedSink}
     * (which ships batches out-of-band to worker peers via the shuffle transport), then emits a
     * single zero-row, schema-bearing response on the coordinator-bound stream so the streaming
     * transport has ≥1 frame to terminate on. Reader-independent — shared by the shard-fragment
     * ({@link #startFragment}) and worker-fragment ({@link #executeWorkerFragmentStreamingAsync})
     * producer paths. Closes {@code sink} in {@code finally} so isLast markers ship before the
     * caller tears down the engine; the sink contract is idempotent-on-close.
     */
    private void drainIntoPartitionedSink(
        ExchangeSink sink,
        EngineResultStream resultStream,
        StreamingFragmentResponseHandler responseHandler
    ) throws Exception {
        int count = 0;
        Schema capturedSchema = null;
        try {
            Iterator<EngineResultBatch> it = resultStream.iterator();
            while (it.hasNext()) {
                VectorSchemaRoot batch = it.next().getArrowRoot();
                if (capturedSchema == null) {
                    capturedSchema = batch.getSchema();
                }
                sink.feed(batch);
                count++;
            }
            LOGGER.debug("drainAndEmitHeader: drained {} batches, closing sink", count);
        } finally {
            // Close the sink here so isLast markers ship before FragmentResources.close() tears
            // down the engine. close() is idempotent on the sink contract; FragmentResources
            // does NOT redundantly close it (see comment in FragmentResources.close()).
            sink.close();
        }

        // Emit one zero-row response carrying the captured schema. The Flight transport
        // serializes this as a regular frame; the coordinator's listener treats it the
        // same as any other batch, decrements the partial-task counter, and the stage
        // transitions to SUCCEEDED on isLast=true.
        if (capturedSchema == null) {
            capturedSchema = new Schema(List.of());
        }
        try (VectorSchemaRoot headerRoot = VectorSchemaRoot.create(capturedSchema, allocator)) {
            headerRoot.setRowCount(0);
            final VectorSchemaRoot vsrRef = headerRoot;
            final Schema schemaRef = capturedSchema;
            responseHandler.onBatch(new EngineResultBatch() {
                @Override
                public VectorSchemaRoot getArrowRoot() {
                    return vsrRef;
                }

                @Override
                public List<String> getFieldNames() {
                    return schemaRef.getFields().stream().map(Field::getName).toList();
                }

                @Override
                public int getRowCount() {
                    return 0;
                }

                @Override
                public Object getFieldValue(String fieldName, int rowIndex) {
                    return null;
                }
            });
        }
    }

    /**
     * Async variant for hash-shuffle worker fragments. Skips {@code IndicesService.getShard}
     * and reader acquisition: workers don't scan shards, they read only from named-input
     * streams that {@link org.opensearch.analytics.spi.ShuffleScanInstructionNode} handlers
     * register on the session context (created by {@link
     * org.opensearch.analytics.spi.ShuffleWorkerSetupInstructionNode}).
     *
     * <p>Builds a {@link ShardScanExecutionContext} with {@code reader=null}; the backend's
     * {@link org.opensearch.analytics.spi.SearchExecEngineProvider} must tolerate this and
     * route through its session-context execution path rather than the shard-scan one.
     */
    public void executeWorkerFragmentStreamingAsync(
        WorkerFragmentRequest request,
        AnalyticsShardTask task,
        StreamingFragmentResponseHandler responseHandler,
        Executor executor
    ) {
        try {
            executor.execute(() -> {
                FragmentInstructionHandler handler;
                SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine = null;
                EngineResultStream stream = null;
                BackendExecutionContext backendContext = null;
                try {
                    FragmentExecutionRequest.PlanAlternative plan = selectWorkerPlan(request);
                    AnalyticsSearchBackendPlugin backend = backends.get(plan.getBackendId());
                    ShardScanExecutionContext ctx = new ShardScanExecutionContext(/* tableName */ "", task, /* reader */ null);
                    ctx.setFragmentBytes(plan.getFragmentBytes());
                    ctx.setAllocator(allocator);
                    ctx.setNamedWriteableRegistry(namedWriteableRegistry);
                    ctx.setShuffleBufferRegistry(shuffleBufferRegistry);

                    List<InstructionNode> instructions = plan.getInstructions();
                    if (!instructions.isEmpty()) {
                        FragmentInstructionHandlerFactory factory = backend.getInstructionHandlerFactory();
                        for (InstructionNode node : instructions) {
                            handler = factory.createHandler(node);
                            backendContext = handler.apply(node, ctx, backendContext);
                        }
                    }

                    // Cascaded shuffle: a worker fragment can ALSO be a shuffle producer (its plan
                    // carries a ShuffleProducerInstructionNode) — e.g. an intermediate join level whose
                    // output feeds a higher shuffle. Same instruction-driven mechanism as the shard
                    // path (resolveProducerSink), so no dedicated stage type is needed. Non-producer
                    // workers (the common leaf-consumer case) get a null sink and drain to the response.
                    ProducerSinkResolution producer = resolveProducerSink(
                        backend,
                        backendContext,
                        ctx,
                        request.getQueryId(),
                        request.getStageId()
                    );
                    ExchangeSink partitionedSink = producer.partitionedSink();
                    backendContext = producer.engineContext();

                    engine = backend.getSearchExecEngineProvider().createSearchExecEngine(ctx, backendContext);
                    stream = engine.execute(ctx);
                    if (partitionedSink != null) {
                        // Producer worker: ship batches out-of-band via the partitioned sink and emit
                        // a single header-only frame on the coordinator-bound stream (mirrors the shard
                        // producer path). drainIntoPartitionedSink closes the sink when done.
                        drainIntoPartitionedSink(partitionedSink, stream, responseHandler);
                        responseHandler.onComplete();
                    } else {
                        Iterator<EngineResultBatch> it = stream.iterator();
                        while (it.hasNext()) {
                            responseHandler.onBatch(it.next());
                        }
                        responseHandler.onComplete();
                    }
                } catch (Exception e) {
                    LOGGER.error(
                        () -> new org.apache.logging.log4j.message.ParameterizedMessage(
                            "executeWorkerFragmentStreamingAsync failed [queryId={}, stageId={}, partition={}]",
                            request.getQueryId(),
                            request.getStageId(),
                            request.getPartitionIndex()
                        ),
                        e
                    );
                    responseHandler.onFailure(e);
                } finally {
                    if (stream != null) {
                        try {
                            stream.close();
                        } catch (Exception ignore) {}
                    }
                    if (engine != null) {
                        try {
                            engine.close();
                        } catch (Exception ignore) {}
                    }
                    if (backendContext != null) {
                        try {
                            backendContext.close();
                        } catch (Exception ignore) {}
                    }
                }
            });
        } catch (Exception e) {
            responseHandler.onFailure(e);
        }
    }

    /** Selects the first plan alternative whose backend is registered on this node — same logic
     *  the shard-fragment path uses but inlined here so workers don't need a shardId/IndexShard
     *  to drive selection. */
    private FragmentExecutionRequest.PlanAlternative selectWorkerPlan(WorkerFragmentRequest request) {
        for (FragmentExecutionRequest.PlanAlternative alt : request.getPlanAlternatives()) {
            if (backends.containsKey(alt.getBackendId())) {
                return alt;
            }
        }
        throw new IllegalArgumentException(
            "No worker plan alternative matches available backends. Alternatives: "
                + request.getPlanAlternatives().stream().map(FragmentExecutionRequest.PlanAlternative::getBackendId).toList()
                + ". Available: "
                + backends.keySet()
        );
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
        if (task != null && task.isCancelled()) {
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
            readerContextStore.releaseContext(request.getQueryId(), shard.shardId());
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
            resources = new FragmentResources(readerContextStore, readerContext, null, stream, null, rowIdVector);
        } catch (Exception e) {
            if (rowIdVector != null) rowIdVector.close();
            readerContextStore.releaseContext(request.getQueryId(), shard.shardId());
            responseHandler.onFailure(new RuntimeException("Failed to execute fetch-by-row-ids on " + shard.shardId(), e));
            return;
        }
        try (FragmentResources ctx = resources) {
            Iterator<EngineResultBatch> it = ctx.stream().iterator();
            while (it.hasNext()) {
                responseHandler.onBatch(it.next());
            }
            responseHandler.onComplete();
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

        /** Called with execution metrics when profiling is enabled. Default delegates to onComplete(). */
        default void onCompleteWithMetrics(byte[] metrics) {
            onComplete();
        }

        void onFailure(Exception e);
    }

    private FragmentResources startFragment(FragmentExecutionRequest request, ResolvedFragment resolved, IndexShard shard, Task task)
        throws IOException {
        GatedCloseable<Reader> gatedReader = resolved.readerProvider.acquireReader();
        // QTF: hand the reader to the store so the fetch phase can reuse it without re-opening.
        // FragmentResources holds a reference to the ReaderContext; close() releases it back
        // to the store, the reaper closes after keepAlive.
        ReaderContext readerContext = readerContextStore.createContext(request.getQueryId(), shard.shardId(), gatedReader);
        assert assertReaderInvariants(gatedReader, readerContext, request.getQueryId(), shard);
        SearchExecEngine<ShardScanExecutionContext, EngineResultStream> engine = null;
        EngineResultStream stream = null;
        BackendExecutionContext backendContext = null;
        Runnable trackerCleanup = null;
        try {
            ShardScanExecutionContext ctx = buildContext(request, readerContext.getReader(), resolved.plan, shard, task);
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

            // Hash-shuffle producer routing: if the instruction chain produced a
            // ShuffleProducerOutputState, build the framework ShuffleSender + the backend's
            // partitioned sink, and unwrap the carrier so the engine factory sees the upstream
            // session state (not the carrier). The drain into the sink happens later in
            // executeFragmentStreamingAsync; we just attach the sink to FragmentResources here.
            ProducerSinkResolution producer = resolveProducerSink(backend, backendContext, ctx, resolved.queryId, resolved.stageId);
            ExchangeSink partitionedSink = producer.partitionedSink();
            backendContext = producer.engineContext();

            engine = backend.getSearchExecEngineProvider().createSearchExecEngine(ctx, backendContext);
            stream = engine.execute(ctx);
            return new FragmentResources(readerContextStore, readerContext, engine, stream, trackerCleanup, null, partitionedSink, ctx);
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
                new FragmentResources(readerContextStore, readerContext, engine, stream, trackerCleanup).close();
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
     * Builds the backend-specific partitioned sink for a hash-shuffle producer fragment. The
     * framework constructs the {@link org.opensearch.analytics.spi.ShuffleSender} pre-stamped
     * with {@code (queryId, targetStageId, side)} from {@code producerState}, then hands it to
     * the backend's {@code createPartitionedSink}. The backend never sees transport types.
     */
    private ExchangeSink buildPartitionedSink(
        AnalyticsSearchBackendPlugin backend,
        ShuffleProducerOutputState producerState,
        ShardScanExecutionContext ctx,
        String queryId,
        int stageId
    ) {
        ShuffleSender sender = new ShuffleSenderImpl(
            client,
            threadPool,
            clusterService,
            producerState.getQueryId(),
            producerState.getTargetStageId(),
            producerState.getSide()
        );
        // Producer's ExchangeSinkContext carries no child inputs (the producer IS the source) and
        // no downstream sink (the partitioned sink ships out-of-band via the ShuffleSender, not
        // into another in-process sink). fragmentBytes is left empty for the same reason — the
        // partitioning operates on the engine's terminal output, not on a plan.
        ExchangeSinkContext sinkCtx = new ExchangeSinkContext(
            queryId,
            stageId,
            ctx.getTask() == null ? 0L : ctx.getTask().getId(),
            new byte[0],
            ctx.getAllocator(),
            List.of(),
            /* downstream */ null
        );
        return backend.getExchangeSinkProvider()
            .createPartitionedSink(
                producerState.getHashKeyChannels(),
                producerState.getPartitionCount(),
                producerState.getTargetWorkerNodeIds(),
                sender,
                sinkCtx
            );
    }

    /**
     * Holder for the result of {@link #resolveProducerSink}: the partitioned sink to drain into
     * (null when this fragment is not a hash-shuffle producer) and the {@link BackendExecutionContext}
     * the engine should run against (the upstream session state, unwrapped from the producer carrier).
     */
    private record ProducerSinkResolution(ExchangeSink partitionedSink, BackendExecutionContext engineContext) {
    }

    /**
     * Hash-shuffle producer routing, shared by the shard-fragment ({@link #startFragment}) and
     * worker-fragment ({@link #executeWorkerFragmentStreamingAsync}) paths. When the instruction
     * chain produced a {@link ShuffleProducerOutputState}, builds the framework ShuffleSender + the
     * backend's partitioned sink and unwraps the carrier so the engine factory sees the upstream
     * session state. Otherwise returns the context unchanged with a null sink (non-producer fragment).
     * Reader-independent — takes {@code queryId}/{@code stageId} directly rather than a shard-only
     * {@code ResolvedFragment}.
     */
    private ProducerSinkResolution resolveProducerSink(
        AnalyticsSearchBackendPlugin backend,
        BackendExecutionContext backendContext,
        ShardScanExecutionContext ctx,
        String queryId,
        int stageId
    ) {
        if (backendContext instanceof ShuffleProducerOutputState producerState) {
            if (client == null || threadPool == null || clusterService == null) {
                throw new IllegalStateException(
                    "AnalyticsSearchService: shuffle sender deps not plumbed; "
                        + "setShuffleSenderDeps(client, threadPool, clusterService) must be called at plugin startup"
                );
            }
            ExchangeSink partitionedSink = buildPartitionedSink(backend, producerState, ctx, queryId, stageId);
            // Engine sees the upstream session state, not the carrier.
            return new ProducerSinkResolution(partitionedSink, producerState.getDelegate());
        }
        return new ProducerSinkResolution(null, backendContext);
    }

    /**
     * Applies each instruction handler in order. Each handler reads the previous handler's
     * {@link BackendExecutionContext} and returns the next one. Returns {@code null} when the
     * instruction list is empty.
     */
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
        ctx.setShuffleBufferRegistry(shuffleBufferRegistry);
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.arrow.search.ArrowSearchPlugin;
import org.opensearch.arrow.search.ArrowSearchSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * Serves a shard stream as one query phase, like a streaming search. The scan's collectors send each batch from within
 * collection as soon as it is full. A send pauses its slice while the coordinator is not ready, and the slice resumes where
 * it stopped.
 * <p>
 * The query phase runs on a virtual thread, and so does each slice when concurrent segment search collects the shard in
 * parallel, so a pause holds no platform thread. Compute slots bound how many slices compute at once. Every path ends the
 * stream with exactly one terminal call, on a virtual thread since its sends wait for the coordinator. Only a failure at
 * node shutdown, which sends no batches, ends the stream on the calling thread. On success, the stream first sends the rows
 * not sent yet, and its last batch carries the shard's summary.
 */
public final class ArrowShardStreamHandler implements TransportRequestHandler<ArrowShardStreamRequest> {

    /** The action of shard streams. */
    public static final String ACTION = "indices:data/read/search/arrow[s]";

    private static final Logger logger = LogManager.getLogger(ArrowShardStreamHandler.class);

    private final SearchService searchService;
    private final IndicesService indicesService;
    private final TaskManager taskManager;
    private final ClusterService clusterService;
    private final ArrowScanRegistry registry;
    private final ArrowSearchSettings settings;
    private final BufferAllocator allocator;
    private final Executor sliceExecutor;

    /**
     * Creates the handler.
     *
     * @param searchService  runs the query phase
     * @param indicesService resolves shards and the field-level security filter
     * @param taskManager    cancels scans of closing shards
     * @param clusterService provides the default search timeout
     * @param registry       the scans of this data node
     * @param settings       the plugin settings
     * @param allocator      allocates batches
     * @param sliceExecutor  the virtual-thread executor that collects the slices of a scan's query phase, and ends streams
     *                       whose query phase completed on another thread
     */
    public ArrowShardStreamHandler(
        SearchService searchService,
        IndicesService indicesService,
        TaskManager taskManager,
        ClusterService clusterService,
        ArrowScanRegistry registry,
        ArrowSearchSettings settings,
        BufferAllocator allocator,
        Executor sliceExecutor
    ) {
        this.searchService = searchService;
        this.indicesService = indicesService;
        this.taskManager = taskManager;
        this.clusterService = clusterService;
        this.registry = registry;
        this.settings = settings;
        this.allocator = allocator;
        this.sliceExecutor = sliceExecutor;
    }

    @Override
    public void messageReceived(ArrowShardStreamRequest request, TransportChannel channel, Task task) {
        final ArrowShardScanTask scanTask = (ArrowShardScanTask) task;
        final ShardSearchRequest shardRequest = request;
        final ShardId shardId = shardRequest.shardId();
        final StreamEnd end = new StreamEnd(channel, shardId, shardRequest.allowPartialSearchResults());
        try {
            final IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
            if (shard.isSystem() || shard.indexSettings().isSearchThrottled()) {
                // Their search pools are small and shared, and a scan stays open as long as the client reads.
                throw new IllegalArgumentException("Arrow search does not support system or search-throttled indices");
            }
            end.slot = registry.acquireScanSlot(settings.admissionWait());
            channel.setBatchReadyTimeout(request.spec().producerReadyTimeout());
            final Predicate<String> fieldVisible = indicesService.getFieldFilter().apply(shardId.getIndexName());
            end.state = new ScanState(
                request.spec(),
                allocator,
                fieldVisible,
                batch -> channel.sendResponseBatch(new ArrowScanBatchResponse(batch, null), true),
                registry::acquireBatchSlot,
                () -> channel.isCancelled() || scanTask.isCancelled(),
                computeBudgetNanos(shardRequest.source()),
                sliceExecutor
            );
            final ShardSearchRequest scanRequest = scanRequest(shardRequest, end.state);
            end.registration = registry.register(shardId, reason -> taskManager.cancel(scanTask, reason, () -> {}));
            searchService.executeQueryPhase(scanRequest, false, scanTask, onVirtualThread(end), ArrowSearchPlugin.EXECUTOR);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            end.onFailure(e);
        }
    }

    /**
     * Ends the stream on a virtual thread, since its sends wait for the coordinator. The query phase usually completes on
     * the virtual thread it ran on. When the shard cannot match, or the request fails before the phase starts, the listener
     * runs on the thread that found out, such as a refresh or transport thread. At node shutdown the executor rejects the
     * hand-off, and the failure ends the stream on that thread; it sends no batches.
     */
    private ActionListener<SearchPhaseResult> onVirtualThread(StreamEnd end) {
        return new ActionListener<>() {
            @Override
            public void onResponse(SearchPhaseResult result) {
                runOnVirtualThread(() -> end.onResponse(result), end);
            }

            @Override
            public void onFailure(Exception e) {
                runOnVirtualThread(() -> end.onFailure(e), end);
            }
        };
    }

    private void runOnVirtualThread(Runnable action, StreamEnd end) {
        if (Thread.currentThread().isVirtual()) {
            action.run();
            return;
        }
        try {
            sliceExecutor.execute(action);
        } catch (RejectedExecutionException e) {
            // The executor rejects only once the node shuts down.
            end.onFailure(e);
        }
    }

    /**
     * Returns the compute budget of each slice in nanoseconds, or -1 for none. The search timeout bounds the time a slice
     * computes, not the time it waits for the coordinator, so the scan enforces it instead of the query phase.
     */
    private long computeBudgetNanos(@Nullable SearchSourceBuilder source) {
        TimeValue timeout = source == null ? null : source.timeout();
        if (timeout == null) {
            timeout = clusterService.getClusterSettings().get(SearchService.DEFAULT_SEARCH_TIMEOUT_SETTING);
        }
        return timeout == null || timeout.equals(SearchService.NO_TIMEOUT) ? -1 : timeout.nanos();
    }

    /**
     * Builds the shard request the query phase runs: the coordinator's shard request, marked for the scan's collectors.
     * The request cache stays off, since a cached result would skip the collectors.
     */
    static ShardSearchRequest scanRequest(ShardSearchRequest request, ScanState state) {
        final SearchRequest searchRequest = new SearchRequest().source(scanSource(request.source(), state))
            .requestCache(false)
            .allowPartialSearchResults(request.allowPartialSearchResults())
            .preference(request.preference());
        final ShardSearchRequest scan = new ShardSearchRequest(
            new OriginalIndices(request.indices(), request.indicesOptions()),
            searchRequest,
            request.shardId(),
            request.numberOfShards(),
            request.getAliasFilter(),
            request.indexBoost(),
            request.nowInMillis(),
            request.getClusterAlias(),
            request.indexRoutings()
        );
        scan.canReturnNullResponseIfMatchNoDocs(request.canReturnNullResponseIfMatchNoDocs());
        return scan;
    }

    /**
     * Builds the source the query phase runs, marked with the scan's {@link ArrowScanExt}. The scan applies {@code size} and
     * {@code terminate_after} as its row limit and {@code timeout} as its compute budget, so the query phase gets none of
     * them.
     */
    private static SearchSourceBuilder scanSource(@Nullable SearchSourceBuilder source, ScanState state) {
        final SearchSourceBuilder scan = source == null ? new SearchSourceBuilder() : source.shallowCopy();
        return scan.ext(List.of(new ArrowScanExt(state)))
            .size(0)
            .from(0)
            .trackTotalHits(true)
            .terminateAfter(SearchContext.DEFAULT_TERMINATE_AFTER)
            .timeout(SearchService.NO_TIMEOUT);
    }

    /**
     * Ends the stream once the query phase is over. On success, it sends the rows not sent yet and the shard's summary;
     * otherwise, the failure. Only the first outcome is sent.
     */
    private final class StreamEnd implements ActionListener<SearchPhaseResult> {
        private final TransportChannel channel;
        private final ShardId shardId;
        private final boolean allowPartialResults;
        private final AtomicBoolean ended = new AtomicBoolean();
        ScanState state;
        Releasable slot;
        Releasable registration;

        StreamEnd(TransportChannel channel, ShardId shardId, boolean allowPartialResults) {
            this.channel = channel;
            this.shardId = shardId;
            this.allowPartialResults = allowPartialResults;
        }

        @Override
        public void onResponse(SearchPhaseResult result) {
            if (ended.compareAndSet(false, true) == false) {
                return;
            }
            final ScanState scan = state;
            // Each slice may hold a batch of rows not sent yet; the last batch sent carries the summary.
            final List<VectorSchemaRoot> remaining = new ArrayList<>();
            int sent = 0;
            try {
                if (scan.collected == false && cannotMatch(result) == false) {
                    // Another plugin's collector or query phase searcher ran the phase instead of the scan's collectors, so
                    // rows would be missing.
                    throw new IllegalArgumentException(
                        "Arrow search cannot run this query on shard " + shardId + ": another plugin collected its query phase"
                    );
                }
                if (scan.timedOut && allowPartialResults == false) {
                    throw new OpenSearchStatusException("shard " + shardId + " exceeded the search timeout", RestStatus.GATEWAY_TIMEOUT);
                }
                remaining.addAll(scan.takeRemainingRows());
                if (remaining.isEmpty()) {
                    remaining.add(emptyBatch(scan));
                }
                long rows = scan.rowsSent.get();
                for (VectorSchemaRoot batch : remaining) {
                    rows += batch.getRowCount();
                }
                final ShardSummary summary = new ShardSummary(rows, scan.terminatedEarly, scan.timedOut);
                while (sent < remaining.size()) {
                    final VectorSchemaRoot batch = remaining.get(sent);
                    final boolean last = sent == remaining.size() - 1;
                    // The channel owns a batch once it is handed over, whether or not the send succeeds.
                    sent++;
                    channel.sendResponseBatch(new ArrowScanBatchResponse(batch, last ? summary.encode() : null), true);
                }
                channel.completeStream();
                release();
            } catch (Exception e) {
                for (VectorSchemaRoot batch : remaining.subList(sent, remaining.size())) {
                    batch.close();
                }
                release();
                sendFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (ended.compareAndSet(false, true) == false) {
                return;
            }
            release();
            sendFailure(e);
        }

        private void release() {
            Releasables.close(state, registration, slot);
        }

        private void sendFailure(Exception failure) {
            final StreamException wire = failure instanceof StreamException se
                ? se
                : new StreamException(StreamErrorCode.forFailure(failure), shardId + " " + rootMessage(failure), failure);
            if (wire.getErrorCode() != StreamErrorCode.CANCELLED && wire.getErrorCode().restStatus().getStatus() >= 500) {
                logger.warn(() -> "arrow scan of shard " + shardId + " failed", failure);
            } else {
                logger.debug("arrow scan of shard {} ended: {}", shardId, failure.getMessage());
            }
            try {
                channel.sendResponse(wire);
            } catch (Exception e) {
                // The coordinator closed the stream, so nobody is left to tell.
                logger.debug("could not send the failure of the arrow scan of shard {}: {}", shardId, e.getMessage());
            }
        }
    }

    /** Whether the query phase ended early because the shard cannot match the query, without collecting. */
    private static boolean cannotMatch(SearchPhaseResult result) {
        return result.queryResult() != null && result.queryResult().isNull();
    }

    private static VectorSchemaRoot emptyBatch(ScanState scan) {
        final VectorSchemaRoot root = VectorSchemaRoot.create(scan.schema, scan.allocator);
        try {
            // An empty batch needs no capacity; Arrow's default reserves thousands of values per column.
            for (FieldVector vector : root.getFieldVectors()) {
                vector.setInitialCapacity(0);
            }
            root.allocateNew();
            root.setRowCount(0);
            return root;
        } catch (OutOfMemoryException e) {
            root.close();
            throw ArrowScanCollector.rejection(e);
        } catch (RuntimeException e) {
            root.close();
            throw e;
        }
    }

    /** The message of the innermost cause, which names the actual problem rather than the phase that hit it. */
    private static String rootMessage(Exception failure) {
        Throwable cause = ExceptionsHelper.unwrapCause(failure);
        while (cause.getCause() != null && cause.getCause() != cause && cause.getCause().getMessage() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    }
}

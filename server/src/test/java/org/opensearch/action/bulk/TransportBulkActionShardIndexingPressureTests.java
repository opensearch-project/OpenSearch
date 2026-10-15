/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.bulk;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.Version;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.stats.DocStatusStats;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.update.UpdateHelper;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.action.index.MappingUpdatedAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.remote.RemoteStorePressureService;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.SystemIndices;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.mockito.ArgumentCaptor;

import static org.opensearch.ingest.IngestServiceTests.createIngestServiceWithProcessors;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportBulkActionShardIndexingPressureTests extends OpenSearchTestCase {
    private final OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException("shard pressure rejected");
    private final Set<Integer> rejectedAdmissions = new HashSet<>();
    private final List<ShardId> attemptedShards = new ArrayList<>();
    private final List<BulkShardRequest> dispatchedShards = new ArrayList<>();
    private final Map<ShardId, AtomicInteger> permitCloses = new HashMap<>();
    private final AtomicInteger requestPermitCloses = new AtomicInteger();
    private final List<Span> spans = new ArrayList<>();

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private IndexingPressureService pressureService;
    private IndicesService indicesService;
    private TransportBulkAction action;
    private Tracer tracer;
    private LongSupplier relativeTimeProvider = System::nanoTime;
    private boolean holdFirstShard;
    private boolean failAfterSecondResponse;
    private long serviceTimeNanos = BulkShardResponse.DEFAULT_SERVICE_TIME_IN_NANOS;
    private int nodeQueueSize = BulkShardResponse.DEFAULT_QUEUE_SIZE;
    private Exception executionFailure;
    private Tuple<BulkShardRequest, ActionListener<BulkShardResponse>> heldShard;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        DiscoveryNode node = new DiscoveryNode(
            "node",
            buildNewFakeTransportAddress(),
            Map.of(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        clusterService = createClusterService(threadPool, node);
        transportService = new CapturingTransport().createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> node,
            null,
            Set.of(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routing = RoutingTable.builder();
        for (String index : List.of("test-index-0", "test-index-1", "test-index-2", ".system-index")) {
            IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .system(index.startsWith("."))
                .build();
            metadata.put(indexMetadata, false);
            ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
            routing.add(
                IndexRoutingTable.builder(indexMetadata.getIndex())
                    .addShard(TestShardRouting.newShardRouting(shardId, node.getId(), true, ShardRoutingState.STARTED))
            );
        }
        setState(clusterService, ClusterState.builder(clusterService.state()).metadata(metadata).routingTable(routing.build()).build());

        pressureService = mock(IndexingPressureService.class);
        when(pressureService.markCoordinatingOperationStarted(any(LongSupplier.class), anyBoolean())).thenReturn(
            requestPermitCloses::incrementAndGet
        );
        when(pressureService.markCoordinatingOperationStarted(any(ShardId.class), any(LongSupplier.class), anyBoolean())).thenAnswer(
            invocation -> {
                ShardId shardId = invocation.getArgument(0);
                attemptedShards.add(shardId);
                if (rejectedAdmissions.contains(attemptedShards.size())) {
                    throw rejection;
                }
                AtomicInteger closes = new AtomicInteger();
                permitCloses.put(shardId, closes);
                return (Releasable) closes::incrementAndGet;
            }
        );
        indicesService = mock(IndicesService.class);
        tracer = mock(Tracer.class);
        when(tracer.isRecording()).thenReturn(true);
        when(tracer.startSpan(any(SpanCreationContext.class))).thenAnswer(invocation -> {
            Span span = mock(Span.class);
            spans.add(span);
            return span;
        });
        when(tracer.withSpanInScope(any(Span.class))).thenReturn(SpanScope.NO_OP);
        ActionFilters filters = new ActionFilters(Set.of());
        SystemIndices systemIndices = new SystemIndices(Map.of());
        TransportShardBulkAction shardAction = new TransportShardBulkAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            mock(ShardStateAction.class),
            mock(MappingUpdatedAction.class),
            mock(UpdateHelper.class),
            filters,
            pressureService,
            mock(SegmentReplicationPressureService.class),
            mock(RemoteStorePressureService.class),
            systemIndices,
            NoopTracer.INSTANCE
        ) {
            @Override
            protected void doExecute(Task task, BulkShardRequest request, ActionListener<BulkShardResponse> listener) {
                dispatchedShards.add(request);
                if (holdFirstShard && dispatchedShards.size() == 1) {
                    heldShard = Tuple.tuple(request, listener);
                } else if (executionFailure != null) {
                    listener.onFailure(executionFailure);
                } else {
                    listener.onResponse(successfulResponse(request));
                    if (failAfterSecondResponse && dispatchedShards.size() == 2) {
                        throw new IllegalStateException("failure after synchronous response");
                    }
                }
            }
        };
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(threadPool.getThreadContext());
        action = new TransportBulkAction(
            threadPool,
            transportService,
            clusterService,
            createIngestServiceWithProcessors(Map.of()),
            shardAction,
            null,
            filters,
            resolver,
            new AutoCreateIndex(Settings.EMPTY, clusterService.getClusterSettings(), resolver, systemIndices),
            pressureService,
            indicesService,
            systemIndices,
            () -> relativeTimeProvider.getAsLong(),
            tracer
        ) {
            @Override
            protected boolean needToCheck() {
                return false;
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        try {
            completeHeldShard();
        } finally {
            try {
                Releasables.close(transportService, clusterService);
            } finally {
                ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
                super.tearDown();
            }
        }
    }

    public void testShardPressureRejectionWaitsForInFlightRequestBuffers() throws Exception {
        rejectedAdmissions.add(2);
        holdFirstShard = true;
        byte[] content = bulkContent();
        BulkRequest request = parseBulk(content);
        AtomicInteger releases = new AtomicInteger();
        AtomicInteger successCallbacks = new AtomicInteger();
        AtomicInteger failureCallbacks = new AtomicInteger();
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.runBefore(ActionListener.wrap(response -> {
            successCallbacks.incrementAndGet();
            future.onResponse(response);
        }, failure -> {
            failureCallbacks.incrementAndGet();
            future.onFailure(failure);
        }), () -> {
            releases.incrementAndGet();
            Arrays.fill(content, (byte) '?');
        });

        ActionTestUtils.execute(action, null, request, listener);

        assertFalse("the bulk request must wait for the accepted shard before releasing its buffers", future.isDone());
        // Admission order is controlled by invocation count, not the iteration order of shard groups.
        assertEquals(3, attemptedShards.size());
        assertEquals(2, dispatchedShards.size());
        assertEquals(attemptedShards.get(0), heldShard.v1().shardId());
        assertEquals(attemptedShards.get(2), dispatchedShards.get(1).shardId());
        assertEquals(0, releases.get());
        assertEquals(0, requestPermitCloses.get());
        assertEquals(0, permitCloses.get(heldShard.v1().shardId()).get());
        for (BulkItemRequest item : heldShard.v1().items()) {
            assertEquals(source(item.id()), ((IndexRequest) item.request()).source().utf8ToString());
        }

        completeHeldShard();

        assertBulkResult(future.actionGet(), 2);
        assertEquals(1, successCallbacks.get());
        assertEquals(0, failureCallbacks.get());
        assertEquals(1, releases.get());
        assertEquals('?', content[0]);
        assertReleased();
    }

    public void testAllShardsRejectedByShardPressure() throws Exception {
        rejectedAdmissions.addAll(Set.of(1, 2, 3));
        BulkResponse response = ActionTestUtils.executeBlocking(action, parseBulk(bulkContent()));
        assertBulkResult(response, 6);
        assertTrue(dispatchedShards.isEmpty());
        assertTrue(permitCloses.isEmpty());
        assertReleased();
    }

    public void testBulkTookIncludesTimeBeforeSuccessfulShardDispatch() throws Exception {
        useClockWithPreShardDelay();
        BulkResponse response = ActionTestUtils.executeBlocking(action, parseBulk(bulkContent()));
        assertFalse(response.hasFailures());
        assertEquals(50, response.getTook().getMillis());
        assertReleased();
    }

    public void testBulkTookIncludesTimeBeforeRejectedShardAdmission() throws Exception {
        useClockWithPreShardDelay();
        rejectedAdmissions.addAll(Set.of(1, 2, 3));
        BulkResponse response = ActionTestUtils.executeBlocking(action, parseBulk(bulkContent()));
        assertBulkResult(response, 6);
        assertEquals(50, response.getTook().getMillis());
        assertReleased();
    }

    public void testShardPressureRejectionCompletesLastShard() throws Exception {
        rejectedAdmissions.add(3);
        BulkResponse response = ActionTestUtils.executeBlocking(action, parseBulk(bulkContent()));
        assertBulkResult(response, 2);
        assertEquals(2, dispatchedShards.size());
        assertReleased();
    }

    public void testShardPressureRejectionDoesNotSkipLaterShards() throws Exception {
        rejectedAdmissions.add(1);
        BulkResponse response = ActionTestUtils.executeBlocking(action, parseBulk(bulkContent()));
        assertBulkResult(response, 2);
        assertEquals(2, dispatchedShards.size());
        assertReleased();
    }

    public void testDispatchedShardFailureStillReleasesPressure() throws Exception {
        executionFailure = rejection;
        BulkResponse response = ActionTestUtils.executeBlocking(action, parseBulk(bulkContent()));
        assertBulkResult(response, 6);
        assertEquals(3, dispatchedShards.size());
        assertReleased();
    }

    public void testSynchronousDispatchFailureWaitsForInFlightRequestBuffers() throws Exception {
        holdFirstShard = true;
        IllegalStateException failure = new IllegalStateException("failed to enter dispatch scope");
        Set<Span> failedScopes = new HashSet<>();
        when(tracer.withSpanInScope(any(Span.class))).thenAnswer(invocation -> {
            Span span = invocation.getArgument(0);
            // Fail only the second shard's dispatch entry; its failure callback must be able to enter its own scope.
            if (spans.indexOf(span) == 1 && failedScopes.add(span)) {
                throw failure;
            }
            return SpanScope.NO_OP;
        });
        AtomicInteger releases = new AtomicInteger();
        PlainActionFuture<BulkResponse> future = executeWithTrackedBuffer(bulkContent(), releases);

        assertHeldRequestPending(future, releases);
        assertEquals(3, attemptedShards.size());
        assertEquals(2, dispatchedShards.size());
        assertEquals(attemptedShards.get(2), dispatchedShards.get(1).shardId());
        assertEquals(1, permitCloses.get(attemptedShards.get(1)).get());
        verify(spans.get(1)).setError(failure);

        completeHeldShard();

        assertTrue(future.isDone());
        BulkResponse response = future.actionGet();
        assertEquals(RestStatus.OK, response.status());
        assertTrue(response.hasFailures());
        assertEquals(6, response.getItems().length);
        int failedItems = 0;
        for (BulkItemResponse item : response.getItems()) {
            if (item.getIndex().equals(attemptedShards.get(1).getIndexName())) {
                assertTrue(item.isFailed());
                assertEquals(RestStatus.INTERNAL_SERVER_ERROR, item.status());
                assertSame(failure, item.getFailure().getCause());
                failedItems++;
            } else {
                assertFalse(item.isFailed());
                assertEquals(RestStatus.CREATED, item.status());
            }
        }
        assertEquals(2, failedItems);
        assertEquals(1, releases.get());
        assertDocStatusStats(4, 0, 2);
        assertReleased();
    }

    public void testFailureAfterSynchronousResponseDoesNotCompleteShardTwice() throws Exception {
        holdFirstShard = true;
        failAfterSecondResponse = true;
        AtomicInteger releases = new AtomicInteger();
        PlainActionFuture<BulkResponse> future = executeWithTrackedBuffer(bulkContent(), releases);

        assertHeldRequestPending(future, releases);
        assertEquals(3, dispatchedShards.size());
        assertEquals(1, permitCloses.get(dispatchedShards.get(1).shardId()).get());
        verify(spans.get(1), never()).setError(any(Exception.class));

        completeHeldShard();

        assertTrue(future.isDone());
        BulkResponse response = future.actionGet();
        assertFalse(response.hasFailures());
        assertEquals(6, response.getItems().length);
        assertEquals(1, releases.get());
        assertDocStatusStats(6, 0, 0);
        assertReleased();
    }

    public void testScopeCloseFailureAfterDispatchWaitsForActualCallback() throws Exception {
        holdFirstShard = true;
        SpanScope failingScope = mock(SpanScope.class);
        doThrow(new IllegalStateException("failed to close dispatch scope")).when(failingScope).close();
        Set<Span> enteredScopes = new HashSet<>();
        when(tracer.withSpanInScope(any(Span.class))).thenAnswer(invocation -> {
            Span span = invocation.getArgument(0);
            // Only the dispatch scope fails. The later response callback has its own, successful scope.
            if (spans.indexOf(span) == 0 && enteredScopes.add(span)) {
                return failingScope;
            }
            return SpanScope.NO_OP;
        });
        AtomicInteger releases = new AtomicInteger();
        PlainActionFuture<BulkResponse> future = executeWithTrackedBuffer(bulkContent(), releases);

        assertHeldRequestPending(future, releases);
        assertEquals(3, dispatchedShards.size());
        verify(spans.get(0), never()).endSpan();

        completeHeldShard();

        assertTrue(future.isDone());
        assertFalse(future.actionGet().hasFailures());
        assertEquals(1, releases.get());
        assertDocStatusStats(6, 0, 0);
        assertReleased();
    }

    public void testNegativeQueueSizeDoesNotFailIndexedItems() throws Exception {
        assertInvalidMetricsDoNotFailIndexedItems(BulkShardResponse.DEFAULT_SERVICE_TIME_IN_NANOS, -1);
    }

    public void testNegativeServiceTimeDoesNotFailIndexedItems() throws Exception {
        assertInvalidMetricsDoNotFailIndexedItems(-1, BulkShardResponse.DEFAULT_QUEUE_SIZE);
    }

    public void testWholeRequestPressureRejectionRemainsRequestFailure() throws Exception {
        when(pressureService.markCoordinatingOperationStarted(any(LongSupplier.class), anyBoolean())).thenThrow(rejection);
        Exception failure = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> ActionTestUtils.executeBlocking(action, parseBulk(bulkContent()))
        );
        assertSame(rejection, failure);
        assertTrue(attemptedShards.isEmpty());
        assertTrue(dispatchedShards.isEmpty());
        assertEquals(0, requestPermitCloses.get());
    }

    public void testUnexpectedAdmissionFailureIsReturnedAsItemFailures() throws Exception {
        IllegalArgumentException failure = new IllegalArgumentException("unexpected admission failure");
        doThrow(failure).when(pressureService).markCoordinatingOperationStarted(any(ShardId.class), any(LongSupplier.class), anyBoolean());
        BulkResponse response = ActionTestUtils.executeBlocking(action, parseBulk(bulkContent()));
        assertEquals(RestStatus.OK, response.status());
        assertTrue(response.hasFailures());
        assertEquals(6, response.getItems().length);
        for (BulkItemResponse item : response.getItems()) {
            assertTrue(item.isFailed());
            assertEquals(RestStatus.BAD_REQUEST, item.status());
            assertSame(failure, item.getFailure().getCause());
        }
        verify(pressureService, times(3)).markCoordinatingOperationStarted(any(ShardId.class), any(LongSupplier.class), anyBoolean());
        assertTrue(dispatchedShards.isEmpty());
        assertReleased();
    }

    public void testSystemOnlyBulkPreservesForcedAdmission() {
        BulkRequest request = new BulkRequest().add(new IndexRequest(".system-index").id("0").source(Map.of("value", "system")));
        assertFalse(ActionTestUtils.executeBlocking(action, request).hasFailures());
        verify(pressureService).markCoordinatingOperationStarted(any(LongSupplier.class), eq(true));
        verify(pressureService).markCoordinatingOperationStarted(any(ShardId.class), any(LongSupplier.class), eq(true));
        assertReleased();
    }

    public void testMixedBulkDoesNotForceShardAdmission() {
        BulkRequest request = new BulkRequest().add(new IndexRequest(".system-index").id("0").source(Map.of("value", "system")))
            .add(new IndexRequest("test-index-0").id("1").source(Map.of("value", "ordinary")));
        assertFalse(ActionTestUtils.executeBlocking(action, request).hasFailures());
        verify(pressureService).markCoordinatingOperationStarted(any(LongSupplier.class), eq(false));
        for (ShardId shardId : attemptedShards) {
            verify(pressureService).markCoordinatingOperationStarted(eq(shardId), any(LongSupplier.class), eq(false));
        }
        assertReleased();
    }

    private void assertBulkResult(BulkResponse response, int failures) {
        assertEquals(RestStatus.OK, response.status());
        assertTrue(response.hasFailures());
        assertEquals(6, response.getItems().length);
        int actualFailures = 0;
        for (int i = 0; i < response.getItems().length; i++) {
            BulkItemResponse item = response.getItems()[i];
            assertEquals(i, item.getItemId());
            assertEquals(Integer.toString(i), item.getId());
            assertEquals("test-index-" + (i % 3), item.getIndex());
            assertEquals(i < 3 ? DocWriteRequest.OpType.INDEX : DocWriteRequest.OpType.CREATE, item.getOpType());
            boolean rejected = executionFailure != null
                || rejectedAdmissions.stream().anyMatch(attempt -> attemptedShards.get(attempt - 1).getIndexName().equals(item.getIndex()));
            assertEquals(rejected, item.isFailed());
            if (rejected) {
                actualFailures++;
                assertEquals(RestStatus.TOO_MANY_REQUESTS, item.status());
                assertSame(rejection, item.getFailure().getCause());
            } else {
                assertEquals(RestStatus.CREATED, item.status());
            }
        }
        assertEquals(failures, actualFailures);
        assertDocStatusStats(6 - failures, failures, 0);
    }

    private void assertDocStatusStats(int successes, int clientErrors, int serverErrors) {
        ArgumentCaptor<DocStatusStats> stats = ArgumentCaptor.forClass(DocStatusStats.class);
        verify(indicesService).addDocStatusStats(stats.capture());
        assertEquals(successes, stats.getValue().getDocStatusCounter()[1].longValue());
        assertEquals(clientErrors, stats.getValue().getDocStatusCounter()[3].longValue());
        assertEquals(serverErrors, stats.getValue().getDocStatusCounter()[4].longValue());
    }

    private void assertReleased() {
        assertEquals(1, requestPermitCloses.get());
        for (AtomicInteger closes : permitCloses.values()) {
            assertEquals(1, closes.get());
        }
        for (Span span : spans) {
            verify(span).endSpan();
        }
    }

    private PlainActionFuture<BulkResponse> executeWithTrackedBuffer(byte[] content, AtomicInteger releases) throws Exception {
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(action, null, parseBulk(content), ActionListener.runBefore(future, () -> {
            releases.incrementAndGet();
            Arrays.fill(content, (byte) '?');
        }));
        return future;
    }

    private void assertHeldRequestPending(PlainActionFuture<BulkResponse> future, AtomicInteger releases) {
        assertFalse("the bulk must wait for the accepted shard", future.isDone());
        assertEquals(0, releases.get());
        assertEquals(0, requestPermitCloses.get());
        assertNotNull(heldShard);
        assertEquals(0, permitCloses.get(heldShard.v1().shardId()).get());
        for (BulkItemRequest item : heldShard.v1().items()) {
            assertEquals(source(item.id()), ((IndexRequest) item.request()).source().utf8ToString());
        }
    }

    private void assertInvalidMetricsDoNotFailIndexedItems(long serviceTimeNanos, int nodeQueueSize) throws Exception {
        this.serviceTimeNanos = serviceTimeNanos;
        this.nodeQueueSize = nodeQueueSize;
        String index = "test-index-0";
        ClusterState state = clusterService.state();
        IndexMetadata current = state.metadata().index(index);
        IndexMetadata appendOnly = IndexMetadata.builder(current)
            .settings(
                Settings.builder()
                    .put(current.getSettings())
                    .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
                    .put(IndexMetadata.INDEX_BULK_ADAPTIVE_SHARD_SELECTION_ENABLED.getKey(), true)
            )
            .build();
        setState(clusterService, ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).put(appendOnly, false)).build());
        BulkRequest request = new BulkRequest().add(
            new IndexRequest(index).id("0").opType(DocWriteRequest.OpType.CREATE).source(Map.of("value", "first"))
        ).add(new IndexRequest(index).id("1").opType(DocWriteRequest.OpType.CREATE).source(Map.of("value", "second")));
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(TransportBulkAction.class))) {
            // The warning proves the response reached the invalid statistics check.
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "invalid node statistics",
                    TransportBulkAction.class.getCanonicalName(),
                    Level.WARN,
                    "Ignoring invalid node statistics from * for *: queue size ["
                        + nodeQueueSize
                        + "], service time EWMA ["
                        + serviceTimeNanos
                        + "]"
                )
            );
            ActionTestUtils.execute(action, null, request, future);
            appender.assertAllExpectationsMatched();
        }

        assertTrue("invalid metrics must not leave the shard counter unfinished", future.isDone());
        BulkResponse response = future.actionGet();
        assertEquals(RestStatus.OK, response.status());
        assertFalse(response.hasFailures());
        assertEquals(2, response.getItems().length);
        for (int i = 0; i < response.getItems().length; i++) {
            BulkItemResponse item = response.getItems()[i];
            assertEquals(i, item.getItemId());
            assertEquals(Integer.toString(i), item.getId());
            assertEquals(index, item.getIndex());
            assertEquals(RestStatus.CREATED, item.status());
        }
        assertEquals(1, dispatchedShards.size());
        assertDocStatusStats(2, 0, 0);
        assertReleased();
    }

    private void useClockWithPreShardDelay() {
        LongSupplier clock = mock(LongSupplier.class);
        // The first reading starts the bulk operation; subsequent readings include 50ms of bulk preparation.
        when(clock.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(50));
        relativeTimeProvider = clock;
    }

    private void completeHeldShard() {
        if (heldShard != null) {
            Tuple<BulkShardRequest, ActionListener<BulkShardResponse>> held = heldShard;
            heldShard = null;
            held.v2().onResponse(successfulResponse(held.v1()));
        }
    }

    private BulkShardResponse successfulResponse(BulkShardRequest request) {
        BulkItemResponse[] items = new BulkItemResponse[request.items().length];
        for (int i = 0; i < items.length; i++) {
            BulkItemRequest item = request.items()[i];
            items[i] = new BulkItemResponse(
                item.id(),
                item.request().opType(),
                new IndexResponse(request.shardId(), item.request().id(), 0, 1, 1, true)
            );
        }
        BulkShardResponse response = new BulkShardResponse(request.shardId(), items, serviceTimeNanos, nodeQueueSize);
        response.setShardInfo(new ReplicationResponse.ShardInfo(1, 1));
        return response;
    }

    private static BulkRequest parseBulk(byte[] content) throws Exception {
        return new BulkRequest().add(new BytesArray(content), null, MediaTypeRegistry.JSON);
    }

    private static byte[] bulkContent() {
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 6; i++) {
            String operation = i < 3 ? "index" : "create";
            content.append("{\"")
                .append(operation)
                .append("\":{\"_index\":\"test-index-")
                .append(i % 3)
                .append("\",\"_id\":\"")
                .append(i)
                .append("\"}}\n")
                .append(source(i))
                .append('\n');
        }
        return content.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String source(int item) {
        return "{\"value\":\"value-" + item + "\"}";
    }
}

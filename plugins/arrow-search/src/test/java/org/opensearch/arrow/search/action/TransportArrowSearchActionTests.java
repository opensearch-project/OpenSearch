/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.arrow.search.ArrowSearchSettings;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.node.Node.NODE_NAME_SETTING;

public class TransportArrowSearchActionTests extends OpenSearchTestCase {

    private static final TimeValue IDLE_TIMEOUT = TimeValue.timeValueSeconds(60);

    private final CoordinatorBudget budget = new CoordinatorBudget(() -> 100);
    private final CoordinatorBudget.Reservation reservation = budget.reserve(64, 10);
    private final DeterministicTaskQueue scheduler = new DeterministicTaskQueue(
        Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(),
        random()
    );
    private final AtomicInteger resets = new AtomicInteger();
    /** The batch gRPC still holds for the client, which a reset discards. */
    private Releasable unreadBatch;
    private final ArrowBatchSink sink = new ArrowBatchSink() {
        @Override
        public void sendSchema(Schema schema) {}

        @Override
        public void sendBatch(VectorSchemaRoot batch) {
            batch.close();
        }

        @Override
        public void complete(SearchResponse summary) {}

        @Override
        public void fail(Exception failure) {}

        @Override
        public void reset() {
            resets.incrementAndGet();
            if (unreadBatch != null) {
                unreadBatch.close();
            }
        }

        @Override
        public void setOnCancel(Runnable onCancel) {}
    };

    private static ArrowSearchSettings settings(Settings settings) {
        return new ArrowSearchSettings(settings, new ClusterSettings(settings, Set.copyOf(ArrowSearchSettings.all())));
    }

    private void runScheduledTasks() {
        while (scheduler.hasDeferredTasks()) {
            scheduler.advanceTime();
            scheduler.runAllRunnableTasks();
        }
    }

    private static TransportArrowSearchAction.BatchSizes batchSizes(Settings settings) {
        return TransportArrowSearchAction.BatchSizes.of(settings(settings), DefaultRoundingPolicy.DEFAULT_ROUNDING_POLICY);
    }

    public void testStreamsAreSizedForRowsLargerThanBatches() {
        TransportArrowSearchAction.BatchSizes sizes = batchSizes(
            Settings.builder().put(ArrowSearchSettings.MAX_ROW_SIZE.getKey(), "2mb").build()
        );
        assertEquals(new ByteSizeValue(1, ByteSizeUnit.MB).getBytes(), sizes.batchBytes());
        // A row larger than the batch size goes out as a batch of its own, so a stream may hold only such batches.
        assertEquals(new ByteSizeValue(2, ByteSizeUnit.MB).getBytes(), sizes.maxBatchBytes());
        assertEquals(sizes.maxBatchBytes(), sizes.batchMemory());
        assertEquals(TransportArrowSearchAction.BATCHES_IN_FLIGHT_PER_STREAM * sizes.maxBatchBytes(), sizes.perStream());
        assertEquals("the batch gRPC holds for the client", sizes.maxBatchBytes(), sizes.perSearch());
    }

    public void testBatchesCountAsTheMemoryArrowAllocatesForThem() {
        // A received batch's body is one buffer, which Arrow rounds up to the next power of two below its 16MB chunk size.
        long mb = new ByteSizeValue(1, ByteSizeUnit.MB).getBytes();
        TransportArrowSearchAction.BatchSizes largest = batchSizes(
            Settings.builder().put(ArrowSearchSettings.BATCH_SIZE.getKey(), "3mb").build()
        );
        assertEquals(3 * mb, largest.maxBatchBytes());
        assertEquals(4 * mb, largest.batchMemory());
        assertEquals(TransportArrowSearchAction.BATCHES_IN_FLIGHT_PER_STREAM * 4 * mb, largest.perStream());
        assertEquals(4 * mb, largest.perSearch());

        assertEquals(2 * mb, batchSizes(Settings.builder().put(ArrowSearchSettings.BATCH_SIZE.getKey(), "1100kb").build()).batchMemory());
        assertEquals(
            "allocations of a chunk or more are not rounded",
            20 * mb,
            batchSizes(Settings.builder().put(ArrowSearchSettings.MAX_ROW_SIZE.getKey(), "20mb").build()).batchMemory()
        );
    }

    public void testRowsSmallerThanBatchesDoNotShrinkThem() {
        TransportArrowSearchAction.BatchSizes sizes = batchSizes(
            Settings.builder()
                .put(ArrowSearchSettings.BATCH_SIZE.getKey(), "256kb")
                .put(ArrowSearchSettings.FIRST_BATCH_SIZE.getKey(), "1mb")
                .put(ArrowSearchSettings.MAX_ROW_SIZE.getKey(), "16kb")
                .build()
        );
        long batchBytes = new ByteSizeValue(256, ByteSizeUnit.KB).getBytes();
        assertEquals(batchBytes, sizes.maxBatchBytes());
        assertEquals(batchBytes, sizes.firstBatchBytes());
        assertEquals(TransportArrowSearchAction.BATCHES_IN_FLIGHT_PER_STREAM * batchBytes, sizes.perStream());
    }

    public void testSearchWhoseBatchesWereAllReadReleasesItsReservation() {
        TransportArrowSearchAction.endSearch(reservation, sink, scheduler.getThreadPool(), IDLE_TIMEOUT);
        assertEquals(0, budget.reserved());
        assertFalse(scheduler.hasDeferredTasks());
    }

    public void testClientThatDoesNotReadItsLastBatchesIsReset() {
        unreadBatch = reservation.holdBatch(10);
        TransportArrowSearchAction.endSearch(reservation, sink, scheduler.getThreadPool(), IDLE_TIMEOUT);
        assertEquals("only the batch the client has not read still counts against the coordinator", 10, budget.reserved());

        runScheduledTasks();
        assertEquals(IDLE_TIMEOUT.millis(), scheduler.getCurrentTimeMillis());
        assertEquals(1, resets.get());
        assertEquals(0, budget.reserved());
    }

    public void testClientThatReadsItsLastBatchesInTimeIsNotReset() {
        Releasable batch = reservation.holdBatch(10);
        TransportArrowSearchAction.endSearch(reservation, sink, scheduler.getThreadPool(), IDLE_TIMEOUT);
        batch.close();
        assertEquals(0, budget.reserved());

        runScheduledTasks();
        assertEquals(0, resets.get());
    }

    public void testSearchEndedTwiceKeepsItsUnreadBatchesCharged() {
        unreadBatch = reservation.holdBatch(10);
        AtomicInteger scheduled = new AtomicInteger();
        Scheduler counting = (command, delay, executor) -> {
            scheduled.incrementAndGet();
            return scheduler.getThreadPool().schedule(command, delay, executor);
        };
        // This happens when the listener's response handling throws and the listener is then notified of the failure.
        TransportArrowSearchAction.endSearch(reservation, sink, counting, IDLE_TIMEOUT);
        TransportArrowSearchAction.endSearch(reservation, sink, counting, IDLE_TIMEOUT);
        assertEquals("the second end does not release the batch's hold", 10, budget.reserved());
        assertEquals("the second end does not schedule another reset", 1, scheduled.get());

        runScheduledTasks();
        assertEquals("the call is reset once", 1, resets.get());
        assertEquals(0, budget.reserved());
    }

    public void testSearchReservesOnlyWhatItsStreamsNeed() {
        assertEquals("one stream per open stream, and the batch gRPC holds", 45, TransportArrowSearchAction.reservationBytes(21, 3, 64, 2));
        assertEquals(24, TransportArrowSearchAction.reservationBytes(21, 3, 64, 1));
        assertEquals("within the per-request limit", 45, TransportArrowSearchAction.reservationBytes(21, 3, 64, 100));
        assertEquals(45, TransportArrowSearchAction.reservationBytes(21, 3, 64, Long.MAX_VALUE));
        assertEquals("at least one stream", 24, TransportArrowSearchAction.reservationBytes(21, 3, 10, 5));
        assertEquals(
            "an unlimited request does not overflow",
            (Long.MAX_VALUE - 3) / 21 * 21 + 3,
            TransportArrowSearchAction.reservationBytes(21, 3, Long.MAX_VALUE, Long.MAX_VALUE)
        );
    }

    public void testDefaultsReserveTenStreamsAndTheBatchGrpcHolds() {
        TransportArrowSearchAction.BatchSizes sizes = batchSizes(Settings.EMPTY);
        long mb = new ByteSizeValue(1, ByteSizeUnit.MB).getBytes();
        long perRequest = ArrowSearchSettings.MAX_BUFFERED_BYTES_PER_REQUEST.get(Settings.EMPTY).getBytes();
        long reserved = TransportArrowSearchAction.reservationBytes(sizes.perStream(), sizes.perSearch(), perRequest, 100);
        assertEquals(10 * 6 * mb + mb, reserved);
        assertEquals(
            "one stream needs seven batches",
            7 * mb,
            TransportArrowSearchAction.reservationBytes(sizes.perStream(), sizes.perSearch(), perRequest, 1)
        );
    }

    private static long maxOpenStreams(
        ClusterState state,
        IndexNameExpressionResolver resolver,
        OperationRouting routing,
        SearchRequest request
    ) {
        return TransportArrowSearchAction.ShardTargets.of(state, resolver, routing, request)
            .maxOpenStreams(request.getMaxConcurrentShardRequests());
    }

    public void testStreamLimitFollowsTheConcurrentShardRequestsTheSearchRunsWith() {
        ClusterState state = ClusterStateCreationUtils.state(1, new String[] { "idx" }, 20);
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        OperationRouting routing = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        SearchRequest request = new SearchRequest("idx");
        TransportArrowSearchAction.ShardTargets targets = TransportArrowSearchAction.ShardTargets.of(state, resolver, routing, request);
        // A workload group may raise the value when the search starts, after its targets are resolved.
        request.setMaxConcurrentShardRequests(20);
        assertEquals(20, targets.maxOpenStreams(request.getMaxConcurrentShardRequests()));
    }

    public void testSearchOpensAtMostOneStreamPerRoutedShardAndConcurrentShardRequest() {
        // Twenty shards over three nodes.
        ClusterState state = ClusterStateCreationUtils.state(3, new String[] { "idx" }, 20);
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        OperationRouting routing = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertEquals("five shard requests at once on each node", 15, maxOpenStreams(state, resolver, routing, new SearchRequest("idx")));
        assertEquals(1, maxOpenStreams(state, resolver, routing, new SearchRequest("idx").routing("tenant")));
        assertEquals(2, maxOpenStreams(state, resolver, routing, new SearchRequest("idx").preference("_shards:0,1")));
        SearchRequest throttled = new SearchRequest("idx");
        throttled.setMaxConcurrentShardRequests(1);
        assertEquals(3, maxOpenStreams(state, resolver, routing, throttled));
        assertEquals(
            "a search that cannot resolve its shards fails",
            1,
            maxOpenStreams(state, resolver, routing, new SearchRequest("missing"))
        );

        assertEquals(2, new TransportArrowSearchAction.ShardTargets(2, 2).maxOpenStreams(5));
        assertEquals(1, new TransportArrowSearchAction.ShardTargets(0, 0).maxOpenStreams(5));
    }

    public void testStoppedSearchCancelsAgainWhileStreamsAreOpening() {
        List<List<String>> cancels = new ArrayList<>();
        Set<String> opening = new HashSet<>(Set.of("n1", "n2"));
        TransportArrowSearchAction.cancelChildren(new String[0], (nodes, done) -> {
            cancels.add(List.of(nodes));
            done.run();
        }, () -> Set.copyOf(opening), scheduler.getThreadPool(), TransportArrowSearchAction.CANCEL_RETRY_INTERVAL);
        assertEquals("first every node", List.of(List.of()), cancels);

        scheduler.advanceTime();
        scheduler.runAllRunnableTasks();
        assertEquals(TransportArrowSearchAction.CANCEL_RETRY_INTERVAL.millis(), scheduler.getCurrentTimeMillis());
        assertEquals(2, cancels.size());
        assertEquals("then the nodes of the streams still opening", Set.of("n1", "n2"), Set.copyOf(cancels.get(1)));

        // The first batch of the stream on n1 arrived, so that stream is cancelled directly.
        opening.remove("n1");
        scheduler.advanceTime();
        scheduler.runAllRunnableTasks();
        assertEquals(List.of("n2"), cancels.get(2));

        opening.clear();
        runScheduledTasks();
        assertEquals("no stream is opening anymore", 3, cancels.size());
    }

    public void testSearchWithoutOpeningStreamsCancelsOnce() {
        AtomicInteger cancels = new AtomicInteger();
        TransportArrowSearchAction.cancelChildren(new String[0], (nodes, done) -> {
            cancels.incrementAndGet();
            done.run();
        }, Set::of, scheduler.getThreadPool(), TransportArrowSearchAction.CANCEL_RETRY_INTERVAL);
        assertFalse(scheduler.hasDeferredTasks());
        assertEquals(1, cancels.get());
    }
}

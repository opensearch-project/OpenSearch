/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.arrow.transport.ArrowBatchResponseHandler;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Verifies producer-side back-pressure under a slow consumer. The producer parks on
 * gRPC's {@code isReady()} once the per-stream outbound buffer crosses
 * {@code arrow.flight.channel.outbound_buffer_threshold} so the flight pool stays
 * within bounds and the stream completes cleanly. Producer wall-clock must reflect
 * the consumer's pacing, evidence that the producer was throttled rather than
 * racing ahead.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 2, maxNumDataNodes = 2)
public class BackpressureProducerIT extends OpenSearchIntegTestCase {

    private static final long MB = 1024L * 1024L;

    /** 8× the gRPC threshold — comfortable headroom for any in-flight queue overshoot
     *  between when {@code isReady()} flips false and when the producer next observes it. */
    private static final long FLIGHT_POOL_CAP_BYTES = 64 * MB;

    /** Set well below the pool cap so {@code isReady()} flips before the allocator runs out. */
    private static final long GRPC_THRESHOLD_BYTES = 8 * MB;

    private static final int BATCH_COUNT = 64;
    private static final int ROWS_PER_BATCH = 256 * 1024; // ~1 MiB per batch
    private static final long CONSUMER_SLEEP_MS = 200;
    /** Per-batch producer compute paces allocation comfortably below gRPC's drain rate
     *  so the eventloop's queue never grows beyond a handful of batches between gate checks. */
    private static final long PRODUCER_SLEEP_MS = 50;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(2);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BackpressureTestPlugin.class, ArrowBasePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                FlightStreamPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                FlightStreamPlugin.class.getName(),
                null,
                List.of(ArrowBasePlugin.class.getName()),
                false
            )
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("node.native_memory.limit", "512mb")
            .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 256 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, FLIGHT_POOL_CAP_BYTES)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, 16 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 16 * MB)
            .put(ServerConfig.FLIGHT_OUTBOUND_BUFFER_THRESHOLD.getKey(), new ByteSizeValue(GRPC_THRESHOLD_BYTES, ByteSizeUnit.BYTES))
            .build();
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testSlowConsumerStreamCompletesUnderBackpressure() throws Exception {
        DiscoveryNode targetNode = pickRemoteDataNode();
        StreamTransportService sts = internalCluster().getInstance(StreamTransportService.class);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger batchesReceived = new AtomicInteger(0);

        long startNanos = System.nanoTime();
        sts.sendRequest(
            targetNode,
            BackpressureTestAction.NAME,
            new BackpressureTestRequest(BATCH_COUNT, ROWS_PER_BATCH, PRODUCER_SLEEP_MS),
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            new ArrowBatchResponseHandler<BackpressureTestResponse>() {
                @Override
                public void handleStreamResponse(StreamTransportResponse<BackpressureTestResponse> stream) {
                    try {
                        BackpressureTestResponse response;
                        while ((response = stream.nextResponse()) != null) {
                            try (VectorSchemaRoot root = response.getRoot()) {
                                batchesReceived.incrementAndGet();
                            }
                            // Slow consumer drives gRPC's outbound past the threshold so
                            // the producer must park rather than race ahead.
                            Thread.sleep(CONSUMER_SLEEP_MS);
                        }
                        stream.close();
                    } catch (Exception e) {
                        failure.compareAndSet(null, e);
                        stream.cancel("consumer error", e);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    failure.compareAndSet(null, exp);
                    latch.countDown();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public BackpressureTestResponse read(StreamInput in) throws IOException {
                    return new BackpressureTestResponse(in);
                }
            }
        );

        assertTrue("Stream should finish within 90s", latch.await(90, TimeUnit.SECONDS));
        assertNull("Producer must not surface any failure under back-pressure: " + failure.get(), failure.get());

        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
        assertEquals("All batches must arrive successfully under back-pressure", BATCH_COUNT, batchesReceived.get());

        // Wall-clock must reflect consumer pacing — without back-pressure the producer
        // would race ahead and finish near-instantly relative to the consumer's sleep
        // budget. 0.4x absorbs startup jitter and CI variance without false negatives.
        long minExpectedMillis = (long) ((BATCH_COUNT * CONSUMER_SLEEP_MS) * 0.4);
        assertTrue(
            "Wall-clock " + elapsedMillis + "ms must reflect consumer pacing (>=" + minExpectedMillis + "ms)",
            elapsedMillis >= minExpectedMillis
        );
    }

    private DiscoveryNode pickRemoteDataNode() {
        String localName = internalCluster().getInstance(StreamTransportService.class).getLocalNode().getName();
        for (DiscoveryNode node : getClusterState().nodes()) {
            if (!node.getName().equals(localName) && node.isDataNode()) {
                return node;
            }
        }
        throw new AssertionError("No remote data node found");
    }

    // ── action / request / response / plugin ──

    public static final Schema SCHEMA = new Schema(List.of(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    public static class BackpressureTestResponse extends ArrowBatchResponse {
        public BackpressureTestResponse(VectorSchemaRoot root) {
            super(root);
        }

        public BackpressureTestResponse(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class BackpressureTestRequest extends ActionRequest {
        private final int batchCount;
        private final int rowsPerBatch;
        private final long perBatchSleepMillis;

        public BackpressureTestRequest(int batchCount, int rowsPerBatch, long perBatchSleepMillis) {
            this.batchCount = batchCount;
            this.rowsPerBatch = rowsPerBatch;
            this.perBatchSleepMillis = perBatchSleepMillis;
        }

        public BackpressureTestRequest(StreamInput in) throws IOException {
            super(in);
            this.batchCount = in.readInt();
            this.rowsPerBatch = in.readInt();
            this.perBatchSleepMillis = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(batchCount);
            out.writeInt(rowsPerBatch);
            out.writeLong(perBatchSleepMillis);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class BackpressureTestAction extends ActionType<BackpressureTestResponse> {
        public static final BackpressureTestAction INSTANCE = new BackpressureTestAction();
        public static final String NAME = "cluster:internal/test/backpressure_producer";

        private BackpressureTestAction() {
            super(NAME, BackpressureTestResponse::new);
        }
    }

    public static class TransportBackpressureTestAction extends TransportAction<BackpressureTestRequest, BackpressureTestResponse> {
        private final BufferAllocator allocator;

        @Inject
        public TransportBackpressureTestAction(
            StreamTransportService streamTransportService,
            ActionFilters actionFilters,
            ArrowNativeAllocator nativeAllocator
        ) {
            super(BackpressureTestAction.NAME, actionFilters, streamTransportService.getTaskManager());
            this.allocator = nativeAllocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT)
                .newChildAllocator("backpressure-it", 0, Long.MAX_VALUE);
            streamTransportService.registerRequestHandler(
                BackpressureTestAction.NAME,
                ThreadPool.Names.GENERIC,
                BackpressureTestRequest::new,
                this::handleStreamRequest
            );
        }

        @Override
        protected void doExecute(Task task, BackpressureTestRequest request, ActionListener<BackpressureTestResponse> listener) {
            listener.onFailure(new UnsupportedOperationException("Use StreamTransportService"));
        }

        private void handleStreamRequest(BackpressureTestRequest request, TransportChannel channel, Task task) throws IOException {
            try {
                for (int b = 0; b < request.batchCount; b++) {
                    if (request.perBatchSleepMillis > 0) {
                        try {
                            Thread.sleep(request.perBatchSleepMillis);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("interrupted", e);
                        }
                    }
                    VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
                    boolean transferred = false;
                    try {
                        IntVector v = (IntVector) root.getVector("value");
                        v.allocateNew(request.rowsPerBatch);
                        for (int i = 0; i < request.rowsPerBatch; i++) {
                            v.setSafe(i, i);
                        }
                        root.setRowCount(request.rowsPerBatch);
                        channel.sendResponseBatch(new BackpressureTestResponse(root));
                        transferred = true;
                    } finally {
                        if (!transferred) {
                            root.close();
                        }
                    }
                }
                channel.completeStream();
            } catch (Exception e) {
                channel.sendResponse(e);
            }
        }
    }

    public static class BackpressureTestPlugin extends Plugin implements ActionPlugin {
        public BackpressureTestPlugin() {}

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(BackpressureTestAction.INSTANCE, TransportBackpressureTestAction.class));
        }
    }
}

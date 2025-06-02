/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.flight.bootstrap.FlightService;
import org.opensearch.arrow.flight.bootstrap.FlightStreamPlugin;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.action.search.SearchTransportService.QUERY_ACTION_NAME;
import static org.opensearch.common.util.FeatureFlags.ARROW_STREAMS;
import static org.opensearch.threadpool.ThreadPool.Names.SAME;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2, maxNumDataNodes = 2)
public class ArrowFlightServerIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(FlightStreamPlugin.class);
    }

    @BeforeClass
    public static void setupSysProperties() {
        System.setProperty("io.netty.allocator.numDirectArenas", "1");
        System.setProperty("io.netty.noUnsafe", "false");
        System.setProperty("io.netty.tryUnsafe", "true");
        System.setProperty("io.netty.tryReflectionSetAccessible", "true");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Create index settings with multiple shards and replicas
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 10)    // Number of primary shards
            .put("index.number_of_replicas", 1)  // Number of replica shards
            .build();

        // Create index request with settings
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("index").settings(indexSettings);

        // Create the index
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest).actionGet();
        assertTrue(createIndexResponse.isAcknowledged());

        // Wait for green status
        client().admin().cluster().prepareHealth("index").setWaitForGreenStatus().setTimeout(TimeValue.timeValueSeconds(30)).get();
        ensureSearchable("index");
        ensureGreen();
        // for (DiscoveryNode node : getClusterState().nodes()) {
        // FlightService flightService = internalCluster().getInstance(FlightService.class, node.getName());
        // FlightClientManager flightClientManager = flightService.getFlightClientManager();
        // assertBusy(() -> {
        // assertTrue(
        // "Flight client should be created successfully before running tests",
        // flightClientManager.getFlightClient(node.getId()).isPresent()
        // );
        // }, 3, TimeUnit.SECONDS);
        // }
    }

    @LockFeatureFlag(ARROW_STREAMS)
    public void testArrowFlightProducer() throws Exception {
        DiscoveryNode previousNode = null;
        for (DiscoveryNode node : getClusterState().nodes()) {
            if (node.isDataNode() == false) {
                continue;
            } else if (previousNode == null) {
                previousNode = node;
                continue;
            }
            IndicesService indicesService = internalCluster().getInstance((IndicesService.class));
            IndexService indexService;
            try {
                indexService = indicesService.indexServiceSafe(resolveIndex("index"));
            } catch (IndexNotFoundException e) {
                continue;
            }
            // FlightService flightService = internalCluster().getInstance(FlightService.class, node.getName());
            // FlightClientManager flightClientManager = flightService.getFlightClientManager();
            // FlightClient flightClient = flightClientManager.getFlightClient(node.getId()).get();
            // assertNotNull(flightClient);
            SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
            IndexShard indexShard = indexService.getShard((Integer) indexService.shardIds().toArray()[0]);
            ShardSearchRequest request = new ShardSearchRequest(
                new OriginalIndices(new String[] { "index" }, IndicesOptions.STRICT_EXPAND_OPEN),
                searchRequest,
                indexShard.shardId(),
                1,
                new AliasFilter(null, Strings.EMPTY_ARRAY),
                1.0f,
                10,
                null,
                new String[0]
            );
            final TransportResponse[] resp = new TransportResponse[1];
            resp[0] = null;
            StreamTransportService transportService = internalCluster().getInstance((StreamTransportService.class));
            TransportResponseHandler handler = new TransportResponseHandler() {
                @Override
                public void handleResponse(TransportResponse response) {
                    resp[0] = response;
                }

                @Override
                public void handleException(TransportException exp) {
                    throw exp;
                }

                @Override
                public String executor() {
                    return SAME;
                }

                @Override
                public Object read(StreamInput in) throws IOException {
                    assertNotNull(in);
                    return null;
                }
            };
            transportService.handleStreamRequest(
                previousNode,
                QUERY_ACTION_NAME,
                request,
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                handler
            );
            assertBusy(() -> { assertNotNull("Timeout waiting for response", resp[0]); }, 100, TimeUnit.SECONDS);
        }
    }

    @LockFeatureFlag(ARROW_STREAMS)
    public void testArrowFlightEndpoint() throws Exception {
        for (DiscoveryNode node : getClusterState().nodes()) {
            FlightService flightService = internalCluster().getInstance(FlightService.class, node.getName());
            FlightClientManager flightClientManager = flightService.getFlightClientManager();
            FlightClient flightClient = flightClientManager.getFlightClient(node.getId()).get();
            assertNotNull(flightClient);
            flightClient.handshake(CallOptions.timeout(5000L, TimeUnit.MILLISECONDS));
            flightClient.handshake(CallOptions.timeout(5000L, TimeUnit.MILLISECONDS));
        }
    }

    @LockFeatureFlag(ARROW_STREAMS)
    public void testFlightStreamReader() throws Exception {
        for (DiscoveryNode node : getClusterState().nodes()) {
            StreamManager streamManagerRandomNode = getStreamManagerRandomNode();
            StreamTicket ticket = streamManagerRandomNode.registerStream(getStreamProducer(), null);
            StreamManager streamManagerCurrentNode = getStreamManager(node.getName());
            // reader should be accessible from any node in the cluster due to the use ProxyStreamProducer
            try (StreamReader<VectorSchemaRoot> reader = streamManagerCurrentNode.getStreamReader(ticket)) {
                int totalBatches = 0;
                assertNotNull(reader.getRoot().getVector("docID"));
                while (reader.next()) {
                    IntVector docIDVector = (IntVector) reader.getRoot().getVector("docID");
                    assertEquals(10, docIDVector.getValueCount());
                    for (int i = 0; i < 10; i++) {
                        assertEquals(docIDVector.toString(), i + (totalBatches * 10L), docIDVector.get(i));
                    }
                    totalBatches++;
                }
                assertEquals(10, totalBatches);
            }
        }
    }

    @LockFeatureFlag(ARROW_STREAMS)
    public void testEarlyCancel() throws Exception {
        DiscoveryNode previousNode = null;
        for (DiscoveryNode node : getClusterState().nodes()) {
            if (previousNode == null) {
                previousNode = node;
                continue;
            }
            StreamManager streamManagerServer = getStreamManager(node.getName());
            TestStreamProducer streamProducer = getStreamProducer();
            StreamTicket ticket = streamManagerServer.registerStream(streamProducer, null);
            StreamManager streamManagerClient = getStreamManager(previousNode.getName());

            CountDownLatch readerComplete = new CountDownLatch(1);
            AtomicReference<Exception> readerException = new AtomicReference<>();
            AtomicReference<StreamReader<VectorSchemaRoot>> readerRef = new AtomicReference<>();

            // Start reader thread
            Thread readerThread = new Thread(() -> {
                try (StreamReader<VectorSchemaRoot> reader = streamManagerClient.getStreamReader(ticket)) {
                    readerRef.set(reader);
                    assertNotNull(reader.getRoot());
                    IntVector docIDVector = (IntVector) reader.getRoot().getVector("docID");
                    assertNotNull(docIDVector);

                    // Read first batch
                    reader.next();
                    assertEquals(10, docIDVector.getValueCount());
                    for (int i = 0; i < 10; i++) {
                        assertEquals(docIDVector.toString(), i, docIDVector.get(i));
                    }
                } catch (Exception e) {
                    readerException.set(e);
                } finally {
                    readerComplete.countDown();
                }
            }, "flight-reader-thread");

            readerThread.start();
            assertTrue("Reader thread did not complete in time", readerComplete.await(5, TimeUnit.SECONDS));

            if (readerException.get() != null) {
                throw readerException.get();
            }

            StreamReader<VectorSchemaRoot> reader = readerRef.get();

            try {
                reader.next();
                fail("Expected FlightRuntimeException");
            } catch (FlightRuntimeException e) {
                assertEquals("CANCELLED", e.status().code().name());
                assertEquals("Stream closed before end", e.getMessage());
                reader.close();
            }

            // Wait for close to complete
            // Due to https://github.com/grpc/grpc-java/issues/5882, there is a logic in FlightStream.java
            // where it exhausts the stream on the server side before it is actually cancelled.
            assertTrue(
                "Timeout waiting for stream cancellation on server [" + node.getName() + "]",
                streamProducer.waitForClose(2, TimeUnit.SECONDS)
            );
            previousNode = node;
        }
    }

    @LockFeatureFlag(ARROW_STREAMS)
    public void testFlightStreamServerError() throws Exception {
        DiscoveryNode previousNode = null;
        for (DiscoveryNode node : getClusterState().nodes()) {
            if (previousNode == null) {
                previousNode = node;
                continue;
            }
            StreamManager streamManagerServer = getStreamManager(node.getName());
            TestStreamProducer streamProducer = getStreamProducer();
            streamProducer.setProduceError(true);
            StreamTicket ticket = streamManagerServer.registerStream(streamProducer, null);
            StreamManager streamManagerClient = getStreamManager(previousNode.getName());
            try (StreamReader<VectorSchemaRoot> reader = streamManagerClient.getStreamReader(ticket)) {
                int totalBatches = 0;
                assertNotNull(reader.getRoot().getVector("docID"));
                try {
                    while (reader.next()) {
                        IntVector docIDVector = (IntVector) reader.getRoot().getVector("docID");
                        assertEquals(10, docIDVector.getValueCount());
                        totalBatches++;
                    }
                    fail("Expected FlightRuntimeException");
                } catch (FlightRuntimeException e) {
                    assertEquals("INTERNAL", e.status().code().name());
                    assertEquals("Unexpected server error", e.getMessage());
                }
                assertEquals(1, totalBatches);
            }
            previousNode = node;
        }
    }

    @LockFeatureFlag(ARROW_STREAMS)
    public void testFlightGetInfo() throws Exception {
        StreamTicket ticket = null;
        for (DiscoveryNode node : getClusterState().nodes()) {
            FlightService flightService = internalCluster().getInstance(FlightService.class, node.getName());
            StreamManager streamManager = flightService.getStreamManager();
            if (ticket == null) {
                ticket = streamManager.registerStream(getStreamProducer(), null);
            }
            FlightClientManager flightClientManager = flightService.getFlightClientManager();
            FlightClient flightClient = flightClientManager.getFlightClient(node.getId()).get();
            assertNotNull(flightClient);
            FlightDescriptor flightDescriptor = FlightDescriptor.command(ticket.toBytes());
            FlightInfo flightInfo = flightClient.getInfo(flightDescriptor, CallOptions.timeout(5000L, TimeUnit.MILLISECONDS));
            assertNotNull(flightInfo);
            assertEquals(100, flightInfo.getRecords());
        }
    }

    private StreamManager getStreamManager(String nodeName) {
        FlightService flightService = internalCluster().getInstance(FlightService.class, nodeName);
        return flightService.getStreamManager();
    }

    private StreamManager getStreamManagerRandomNode() {
        FlightService flightService = internalCluster().getInstance(FlightService.class);
        return flightService.getStreamManager();
    }

    private TestStreamProducer getStreamProducer() {
        return new TestStreamProducer();
    }

    private static class TestStreamProducer implements StreamProducer<VectorSchemaRoot, BufferAllocator> {
        volatile boolean isClosed = false;
        private final CountDownLatch closeLatch = new CountDownLatch(1);
        TimeValue deadline = TimeValue.timeValueSeconds(5);
        private boolean produceError = false;

        public void setProduceError(boolean produceError) {
            this.produceError = produceError;
        }

        TestStreamProducer() {}

        VectorSchemaRoot root;

        @Override
        public VectorSchemaRoot createRoot(BufferAllocator allocator) {
            IntVector docIDVector = new IntVector("docID", allocator);
            FieldVector[] vectors = new FieldVector[] { docIDVector };
            root = new VectorSchemaRoot(Arrays.asList(vectors));
            return root;
        }

        @Override
        public BatchedJob<VectorSchemaRoot> createJob(BufferAllocator allocator) {
            return new BatchedJob<>() {
                @Override
                public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
                    IntVector docIDVector = (IntVector) root.getVector("docID");
                    root.setRowCount(10);
                    for (int i = 0; i < 100; i++) {
                        docIDVector.setSafe(i % 10, i);
                        if ((i + 1) % 10 == 0) {
                            flushSignal.awaitConsumption(TimeValue.timeValueMillis(1000));
                            docIDVector.clear();
                            root.setRowCount(10);
                            if (produceError) {
                                throw new RuntimeException("Server error while producing batch");
                            }
                        }
                    }
                }

                @Override
                public void onCancel() {
                    if (!isClosed && root != null) {
                        root.close();
                    }
                    isClosed = true;
                }

                @Override
                public boolean isCancelled() {
                    return isClosed;
                }
            };
        }

        @Override
        public TimeValue getJobDeadline() {
            return deadline;
        }

        @Override
        public int estimatedRowCount() {
            return 100;
        }

        @Override
        public String getAction() {
            return "";
        }

        @Override
        public void close() {
            if (!isClosed && root != null) {
                root.close();
            }
            closeLatch.countDown();
            isClosed = true;
        }

        public boolean waitForClose(long timeout, TimeUnit unit) throws InterruptedException {
            return closeLatch.await(timeout, unit);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.action.PartialPlanAction;
import org.opensearch.be.datafusion.action.PartialPlanRequest;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugins.Plugin;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Integration test: Coordinator -> PartialPlanRequest via Flight -> Data node Rust/DataFusion -> Arrow batches -> Java merge.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class StreamingTransportIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(StreamingTransportIT.class);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            AnalyticsPlugin.class,
            FlightStreamPlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .build();
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingTransportRoundTrip() throws Exception {
        // Create test parquet at a temp path (all nodes share JVM in test)
        Path parquetDir = Files.createTempDirectory("streaming-transport-it");
        String parquetPath = parquetDir.resolve("test_data.parquet").toString();
        NativeBridge.createTestParquet(parquetPath);
        logger.info("Created test parquet at {}", parquetPath);

        // Pick coordinator and data node
        String coordinatorNodeName = internalCluster().getNodeNames()[0];
        String dataNodeName = internalCluster().getNodeNames()[1];

        StreamTransportService streamTransport = internalCluster().getInstance(StreamTransportService.class, coordinatorNodeName);

        // Find target DiscoveryNode
        DiscoveryNode targetNode = null;
        for (DiscoveryNode node : clusterService().state().nodes()) {
            if (node.getName().equals(dataNodeName)) {
                targetNode = node;
                break;
            }
        }
        assertNotNull("Could not find data node: " + dataNodeName, targetNode);

        // Ensure streaming connection is established
        streamTransport.connectToNode(targetNode);
        Transport.Connection connection = streamTransport.getConnection(targetNode);

        // Send PartialPlanRequest via stream transport with STREAM type options
        String sql = "SELECT category, COUNT(*) as cnt, SUM(CAST(amount AS BIGINT)) as total FROM t GROUP BY category ORDER BY category";
        PartialPlanRequest request = new PartialPlanRequest(parquetPath, sql);

        TransportRequestOptions options = TransportRequestOptions.builder()
            .withType(TransportRequestOptions.Type.STREAM)
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        List<VectorSchemaRoot> collectedBatches = new ArrayList<>();
        AtomicReference<Exception> error = new AtomicReference<>();

        streamTransport.sendRequest(
            connection,
            PartialPlanAction.NAME,
            request,
            options,
            new TransportResponseHandler<PartialPlanAction.Response>() {
                @Override
                public PartialPlanAction.Response read(StreamInput in) throws IOException {
                    return new PartialPlanAction.Response(in);
                }

                @Override
                public void handleResponse(PartialPlanAction.Response response) {
                    logger.info("Received non-stream response (unexpected)");
                    latch.countDown();
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Transport exception", exp);
                    error.set(exp);
                    latch.countDown();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SEARCH;
                }

                @Override
                public void handleStreamResponse(StreamTransportResponse<PartialPlanAction.Response> response) {
                    try {
                        Object batch;
                        while ((batch = response.nextNativeBatch()) != null) {
                            VectorSchemaRoot root = (VectorSchemaRoot) batch;
                            logger.info("Received batch: {} rows, {} cols", root.getRowCount(), root.getFieldVectors().size());
                            collectedBatches.add(root);
                        }
                    } catch (Exception e) {
                        logger.error("Error reading batches", e);
                        error.set(e);
                    } finally {
                        try { response.close(); } catch (Exception ignored) {}
                        latch.countDown();
                    }
                }
            }
        );

        assertTrue("Timed out waiting for stream response", latch.await(30, TimeUnit.SECONDS));
        assertNull("Stream transport error: " + error.get(), error.get());
        assertFalse("No batches received", collectedBatches.isEmpty());

        // Merge batches and verify
        Map<String, long[]> merged = mergeArrowBatches(collectedBatches);
        logger.info("Merged results: {}", merged);

        // Test parquet has 8 rows: A(3):200+300+300=800, B(2):150+300=450, C(2):250+250=500, D(1):175
        assertEquals("Expected 4 categories", 4, merged.size());
        assertArrayEquals(new long[]{3, 800}, merged.get("A"));
        assertArrayEquals(new long[]{2, 450}, merged.get("B"));
        assertArrayEquals(new long[]{2, 500}, merged.get("C"));
        assertArrayEquals(new long[]{1, 175}, merged.get("D"));

        // Cleanup
        for (VectorSchemaRoot root : collectedBatches) {
            root.close();
        }
        Files.deleteIfExists(Path.of(parquetPath));
        Files.deleteIfExists(parquetDir);
    }

    private Map<String, long[]> mergeArrowBatches(List<VectorSchemaRoot> batches) {
        Map<String, long[]> result = new HashMap<>();
        for (VectorSchemaRoot root : batches) {
            FieldVector categoryVec = root.getVector("category");
            FieldVector cntVec = root.getVector("cnt");
            FieldVector totalVec = root.getVector("total");

            assertNotNull("Missing 'category' column", categoryVec);
            assertNotNull("Missing 'cnt' column", cntVec);
            assertNotNull("Missing 'total' column", totalVec);

            for (int i = 0; i < root.getRowCount(); i++) {
                String category;
                if (categoryVec instanceof VarCharVector vc) {
                    category = new String(vc.get(i));
                } else {
                    category = categoryVec.getObject(i).toString();
                }
                long cnt = ((Number) cntVec.getObject(i)).longValue();
                long total = ((Number) totalVec.getObject(i)).longValue();

                result.merge(category, new long[]{cnt, total}, (old, val) -> {
                    old[0] += val[0];
                    old[1] += val[1];
                    return old;
                });
            }
        }
        return result;
    }
}

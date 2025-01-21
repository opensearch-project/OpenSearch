/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.OSFlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.flight.bootstrap.FlightService;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 5)
public class ArrowFlightServerIT extends OpenSearchIntegTestCase {

    private FlightClientManager flightClientManager;
    private FlightService flightService;

    @BeforeClass
    public static void setupFeatureFlags() {
        FeatureFlagSetter.set(FeatureFlags.ARROW_STREAMS_SETTING.getKey());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(FlightStreamPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ensureGreen();
        Thread.sleep(1000);
        flightService = internalCluster().getInstance(FlightService.class);
        flightClientManager = flightService.getFlightClientManager();
    }

    public void testArrowFlightEndpoint() throws Exception {
        for (DiscoveryNode node : getClusterState().nodes()) {
            OSFlightClient flightClient = flightClientManager.getFlightClient(node.getId());
            assertNotNull(flightClient);
            flightClient.handshake(CallOptions.timeout(5000L, TimeUnit.MILLISECONDS));
        }
    }

    public void testFlightGetInfo() throws Exception {
        StreamManager streamManager = flightService.getStreamManager();
        StreamTicket ticket = streamManager.registerStream(getStreamProducer(), null);
        for (DiscoveryNode node : getClusterState().nodes()) {
            OSFlightClient flightClient = flightClientManager.getFlightClient(node.getId());
            assertNotNull(flightClient);
            FlightDescriptor flightDescriptor = FlightDescriptor.command(ticket.toBytes());
            FlightInfo flightInfo = flightClient.getInfo(flightDescriptor, CallOptions.timeout(5000L, TimeUnit.MILLISECONDS));
            assertNotNull(flightInfo);
        }
    }

    private StreamProducer getStreamProducer() {
        return new StreamProducer() {
            @Override
            public VectorSchemaRoot createRoot(BufferAllocator allocator) {
                IntVector docIDVector = new IntVector("docID", allocator);
                FieldVector[] vectors = new FieldVector[] { docIDVector };
                return new VectorSchemaRoot(Arrays.asList(vectors));
            }

            @Override
            public BatchedJob createJob(BufferAllocator allocator) {
                return new BatchedJob() {
                    @Override
                    public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
                        IntVector docIDVector = (IntVector) root.getVector("docID");
                        for (int i = 0; i < 100; i++) {
                            docIDVector.setSafe(i % 10, i);
                            if (i >= 10) {
                                root.setRowCount(10);
                                flushSignal.awaitConsumption(1000);
                            }
                        }
                    }

                    @Override
                    public void onCancel() {

                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }
                };
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
            public void close() throws IOException {

            }
        };
    }
}

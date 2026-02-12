/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.nodes;

import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.example.stream.StreamTransportExamplePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3)
public class StreamNodesTransportExampleIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("opensearch.experimental.feature.transport.stream.enabled", true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(StreamTransportExamplePlugin.class, FlightStreamPlugin.class);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    @AwaitsFix(bugUrl = "flaky")
    public void testStreamNodesAction() throws Exception {
        NodeClient nodeClient = internalCluster().getInstance(NodeClient.class);

        StreamNodesDataRequest request = new StreamNodesDataRequest("_all").count(3).delayMs(100);

        CountDownLatch latch = new CountDownLatch(1);
        List<NodeStreamDataResponse> allResponses = new ArrayList<>();

        nodeClient.executeStream(StreamNodesDataAction.INSTANCE, request, new StreamTransportResponseHandler<StreamNodesDataResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<StreamNodesDataResponse> streamResponse) {
                try {
                    StreamNodesDataResponse response;
                    while ((response = streamResponse.nextResponse()) != null) {
                        synchronized (allResponses) {
                            allResponses.addAll(response.getNodes());
                        }
                    }
                    streamResponse.close();
                    latch.countDown();
                } catch (Exception e) {
                    streamResponse.cancel("Client error", e);
                    logger.error("Stream failed", e);
                    fail("Stream should not fail: " + e.getMessage());
                    latch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                logger.error("Transport exception", exp);
                fail("Stream should not fail: " + exp.getMessage());
                latch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public StreamNodesDataResponse read(StreamInput in) throws IOException {
                return new StreamNodesDataResponse(in);
            }
        });

        assertTrue("Stream should complete", latch.await(30, TimeUnit.SECONDS));
        int numNodes = internalCluster().size();
        assertEquals("Should receive 3 items per node * " + numNodes + " nodes", 3 * numNodes, allResponses.size());
    }
}

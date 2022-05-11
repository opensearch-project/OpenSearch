/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.test.disruption;

import org.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class NetworkDisruptionIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    private static final Settings DISRUPTION_TUNED_SETTINGS = Settings.builder()
        .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "2s")
        .build();

    /**
     * Creates 3 to 5 mixed-node cluster and splits it into 2 parts.
     * The first part is guaranteed to have at least the majority of the nodes,
     * so that cluster-manager could be elected on this side.
     */
    private Tuple<Set<String>, Set<String>> prepareDisruptedCluster() {
        int numOfNodes = randomIntBetween(3, 5);
        internalCluster().setBootstrapClusterManagerNodeIndex(numOfNodes - 1);
        Set<String> nodes = new HashSet<>(internalCluster().startNodes(numOfNodes, DISRUPTION_TUNED_SETTINGS));
        ensureGreen();
        assertThat(nodes.size(), greaterThanOrEqualTo(3));
        int majority = nodes.size() / 2 + 1;
        Set<String> side1 = new HashSet<>(randomSubsetOf(randomIntBetween(majority, nodes.size() - 1), nodes));
        assertThat(side1.size(), greaterThanOrEqualTo(majority));
        Set<String> side2 = new HashSet<>(nodes);
        side2.removeAll(side1);
        assertThat(side2.size(), greaterThanOrEqualTo(1));
        NetworkDisruption networkDisruption = new NetworkDisruption(new TwoPartitions(side1, side2), NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        return Tuple.tuple(side1, side2);
    }

    public void testClearDisruptionSchemeWhenNodeIsDown() throws IOException {
        Tuple<Set<String>, Set<String>> sides = prepareDisruptedCluster();

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(randomFrom(sides.v2())));
        internalCluster().clearDisruptionScheme();
    }

    public void testNetworkPartitionRemovalRestoresConnections() throws Exception {
        Tuple<Set<String>, Set<String>> sides = prepareDisruptedCluster();
        Set<String> side1 = sides.v1();
        Set<String> side2 = sides.v2();

        // sends some requests to the majority side part
        client(randomFrom(side1)).admin().cluster().prepareNodesInfo().get();
        internalCluster().clearDisruptionScheme();
        // check all connections are restored
        for (String nodeA : side1) {
            for (String nodeB : side2) {
                TransportService serviceA = internalCluster().getInstance(TransportService.class, nodeA);
                TransportService serviceB = internalCluster().getInstance(TransportService.class, nodeB);
                // TODO assertBusy should not be here, see https://github.com/elastic/elasticsearch/issues/38348
                assertBusy(() -> {
                    assertTrue(nodeA + " is not connected to " + nodeB, serviceA.nodeConnected(serviceB.getLocalNode()));
                    assertTrue(nodeB + " is not connected to " + nodeA, serviceB.nodeConnected(serviceA.getLocalNode()));
                });
            }
        }
    }

    public void testTransportRespondsEventually() throws InterruptedException {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        internalCluster().ensureAtLeastNumDataNodes(randomIntBetween(3, 5));
        final NetworkDisruption.DisruptedLinks disruptedLinks;
        if (randomBoolean()) {
            disruptedLinks = TwoPartitions.random(random(), internalCluster().getNodeNames());
        } else {
            disruptedLinks = NetworkDisruption.Bridge.random(random(), internalCluster().getNodeNames());
        }

        NetworkDisruption networkDisruption = new NetworkDisruption(
            disruptedLinks,
            randomFrom(NetworkDisruption.UNRESPONSIVE, NetworkDisruption.DISCONNECT, NetworkDisruption.NetworkDelay.random(random()))
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        networkDisruption.startDisrupting();

        int requests = randomIntBetween(1, 200);
        CountDownLatch latch = new CountDownLatch(requests);
        for (int i = 0; i < requests - 1; ++i) {
            sendRequest(
                internalCluster().getInstance(TransportService.class),
                internalCluster().getInstance(TransportService.class),
                latch
            );
        }

        // send a request that is guaranteed disrupted.
        Tuple<TransportService, TransportService> disruptedPair = findDisruptedPair(disruptedLinks);
        sendRequest(disruptedPair.v1(), disruptedPair.v2(), latch);

        // give a bit of time to send something under disruption.
        assertFalse(
            latch.await(500, TimeUnit.MILLISECONDS) && networkDisruption.getNetworkLinkDisruptionType() != NetworkDisruption.DISCONNECT
        );
        networkDisruption.stopDisrupting();

        latch.await(30, TimeUnit.SECONDS);
        assertEquals("All requests must respond, requests: " + requests, 0, latch.getCount());
    }

    private Tuple<TransportService, TransportService> findDisruptedPair(NetworkDisruption.DisruptedLinks disruptedLinks) {
        Optional<Tuple<TransportService, TransportService>> disruptedPair = disruptedLinks.nodes()
            .stream()
            .flatMap(n1 -> disruptedLinks.nodes().stream().map(n2 -> Tuple.tuple(n1, n2)))
            .filter(pair -> disruptedLinks.disrupt(pair.v1(), pair.v2()))
            .map(
                pair -> Tuple.tuple(
                    internalCluster().getInstance(TransportService.class, pair.v1()),
                    internalCluster().getInstance(TransportService.class, pair.v2())
                )
            )
            .findFirst();
        // since we have 3+ nodes, we are sure to find a disrupted pair, also for bridge disruptions.
        assertTrue(disruptedPair.isPresent());
        return disruptedPair.get();
    }

    private void sendRequest(TransportService source, TransportService target, CountDownLatch latch) {
        source.sendRequest(
            target.getLocalNode(),
            ClusterHealthAction.NAME,
            new ClusterHealthRequest(),
            new TransportResponseHandler<TransportResponse>() {
                private AtomicBoolean responded = new AtomicBoolean();

                @Override
                public void handleResponse(TransportResponse response) {
                    assertTrue(responded.compareAndSet(false, true));
                    latch.countDown();
                }

                @Override
                public void handleException(TransportException exp) {
                    assertTrue(responded.compareAndSet(false, true));
                    latch.countDown();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public TransportResponse read(StreamInput in) throws IOException {
                    return ClusterHealthResponse.readResponseFrom(in);
                }
            }
        );
    }
}

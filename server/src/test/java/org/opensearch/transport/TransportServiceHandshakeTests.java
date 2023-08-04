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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.nio.MockNioTransport;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;

public class TransportServiceHandshakeTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private static final long timeout = Long.MAX_VALUE;

    @BeforeClass
    public static void startThreadPool() {
        threadPool = new TestThreadPool(TransportServiceHandshakeTests.class.getSimpleName());
    }

    private List<TransportService> transportServices = new ArrayList<>();

    private NetworkHandle startServices(String nodeNameAndId, Settings settings, Version version) {
        MockNioTransport transport = new MockNioTransport(
            settings,
            Version.CURRENT,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService()
        );
        TransportService transportService = new MockTransportService(
            settings,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            (boundAddress) -> new DiscoveryNode(
                nodeNameAndId,
                nodeNameAndId,
                boundAddress.publishAddress(),
                emptyMap(),
                emptySet(),
                version
            ),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        transportServices.add(transportService);
        return new NetworkHandle(transportService, transportService.getLocalNode());
    }

    @After
    public void tearDown() throws Exception {
        for (TransportService transportService : transportServices) {
            transportService.close();
        }
        super.tearDown();
    }

    @AfterClass
    public static void terminateThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        threadPool = null;
    }

    public void testConnectToNodeLight() throws IOException {
        Settings settings = Settings.builder().put("cluster.name", "test").build();

        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB = startServices(
            "TS_B",
            settings,
            VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT)
        );
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion()
        );
        try (Transport.Connection connection = handleA.transportService.openConnection(discoveryNode, TestProfiles.LIGHT_PROFILE)) {
            DiscoveryNode connectedNode = PlainActionFuture.get(fut -> handleA.transportService.handshake(connection, timeout, fut));
            assertNotNull(connectedNode);
            // the name and version should be updated
            assertEquals(connectedNode.getName(), "TS_B");
            assertEquals(connectedNode.getVersion(), handleB.discoveryNode.getVersion());
            assertFalse(handleA.transportService.nodeConnected(discoveryNode));
        }
    }

    public void testMismatchedClusterName() {

        NetworkHandle handleA = startServices("TS_A", Settings.builder().put("cluster.name", "a").build(), Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", Settings.builder().put("cluster.name", "b").build(), Version.CURRENT);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion()
        );
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> {
            try (Transport.Connection connection = handleA.transportService.openConnection(discoveryNode, TestProfiles.LIGHT_PROFILE)) {
                PlainActionFuture.get(fut -> handleA.transportService.handshake(connection, timeout, ActionListener.map(fut, x -> null)));
            }
        });
        assertThat(
            ex.getMessage(),
            containsString("handshake with [" + discoveryNode + "] failed: remote cluster name [b] does not match local cluster name [a]")
        );
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testIncompatibleVersions() {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB = startServices(
            "TS_B",
            settings,
            VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion())
        );
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion()
        );
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> {
            try (Transport.Connection connection = handleA.transportService.openConnection(discoveryNode, TestProfiles.LIGHT_PROFILE)) {
                PlainActionFuture.get(fut -> handleA.transportService.handshake(connection, timeout, ActionListener.map(fut, x -> null)));
            }
        });
        assertThat(
            ex.getMessage(),
            containsString(
                "handshake with ["
                    + discoveryNode
                    + "] failed: remote node version ["
                    + handleB.discoveryNode.getVersion()
                    + "] is incompatible with local node version ["
                    + Version.CURRENT
                    + "]"
            )
        );
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testNodeConnectWithDifferentNodeId() {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", settings, Version.CURRENT);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            randomAlphaOfLength(10),
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            handleB.discoveryNode.getVersion()
        );
        ConnectTransportException ex = expectThrows(ConnectTransportException.class, () -> {
            handleA.transportService.connectToNode(discoveryNode, TestProfiles.LIGHT_PROFILE);
        });
        assertThat(ex.getMessage(), containsString("unexpected remote node"));
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testNodeConnectWithDifferentNodeIDSkipValidation() throws IllegalAccessException {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", settings, Version.CURRENT);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            randomAlphaOfLength(10),
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            handleB.discoveryNode.getVersion()
        );
        try (MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(LogManager.getLogger(TransportService.class))) {

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "Validation Skipped",
                    "org.opensearch.transport.TransportService",
                    Level.INFO,
                    "Connection validation was skipped"
                )
            );

            handleA.transportService.connectToExtensionNode(discoveryNode, TestProfiles.LIGHT_PROFILE);

            mockLogAppender.assertAllExpectationsMatched();

            assertTrue(handleA.transportService.nodeConnected(discoveryNode));
        }
    }

    private static class NetworkHandle {
        private TransportService transportService;
        private DiscoveryNode discoveryNode;

        NetworkHandle(TransportService transportService, DiscoveryNode discoveryNode) {
            this.transportService = transportService;
            this.discoveryNode = discoveryNode;
        }
    }

}

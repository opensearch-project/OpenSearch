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

package org.opensearch.client.transport;

import org.opensearch.Version;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.node.liveness.LivenessResponse;
import org.opensearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.client.AbstractClientHeadersTestCase;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.env.Environment;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.MockTransportClient;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TransportClientHeadersTests extends AbstractClientHeadersTestCase {

    private MockTransportService transportService;

    @Override
    public void tearDown() throws Exception {
        try {
            // stop this first before we bubble up since
            // transportService uses the threadpool that super.tearDown will close
            transportService.stop();
            transportService.close();
        } finally {
            super.tearDown();
        }

    }

    @Override
    protected Client buildClient(Settings headersSettings, ActionType[] testedActions) {
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null);
        transportService.start();
        transportService.acceptIncomingRequests();
        String transport = getTestTransportType();
        TransportClient client = new MockTransportClient(
            Settings.builder()
                .put("client.transport.sniff", false)
                .put("cluster.name", "cluster1")
                .put("node.name", "transport_client_" + this.getTestName())
                .put(NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), transport)
                .put(headersSettings)
                .build(),
            InternalTransportServiceInterceptor.TestPlugin.class
        );
        InternalTransportServiceInterceptor.TestPlugin plugin = client.injector.getInstance(PluginsService.class)
            .filterPlugins(InternalTransportServiceInterceptor.TestPlugin.class)
            .stream()
            .findFirst()
            .get();
        plugin.instance.threadPool = client.threadPool();
        plugin.instance.address = transportService.boundAddress().publishAddress();
        client.addTransportAddress(transportService.boundAddress().publishAddress());
        return client;
    }

    public void testWithSniffing() throws Exception {
        String transport = getTestTransportType();
        try (
            TransportClient client = new MockTransportClient(
                Settings.builder()
                    .put("client.transport.sniff", true)
                    .put("cluster.name", "cluster1")
                    .put("node.name", "transport_client_" + this.getTestName() + "_1")
                    .put("client.transport.nodes_sampler_interval", "1s")
                    .put(NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), transport)
                    .put(HEADER_SETTINGS)
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build(),
                InternalTransportServiceInterceptor.TestPlugin.class
            )
        ) {
            InternalTransportServiceInterceptor.TestPlugin plugin = client.injector.getInstance(PluginsService.class)
                .filterPlugins(InternalTransportServiceInterceptor.TestPlugin.class)
                .stream()
                .findFirst()
                .get();
            plugin.instance.threadPool = client.threadPool();
            plugin.instance.address = transportService.boundAddress().publishAddress();
            client.addTransportAddress(transportService.boundAddress().publishAddress());

            if (!plugin.instance.clusterStateLatch.await(5, TimeUnit.SECONDS)) {
                fail("takes way too long to get the cluster state");
            }

            assertEquals(1, client.connectedNodes().size());
            assertEquals(client.connectedNodes().get(0).getAddress(), transportService.boundAddress().publishAddress());
        }
    }

    public static class InternalTransportServiceInterceptor implements TransportInterceptor {

        ThreadPool threadPool;
        TransportAddress address;

        public static class TestPlugin extends Plugin implements NetworkPlugin {
            private InternalTransportServiceInterceptor instance = new InternalTransportServiceInterceptor();

            @Override
            public List<TransportInterceptor> getTransportInterceptors(
                NamedWriteableRegistry namedWriteableRegistry,
                ThreadContext threadContext
            ) {
                return Collections.singletonList(new TransportInterceptor() {
                    @Override
                    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                        String action,
                        String executor,
                        boolean forceExecution,
                        TransportRequestHandler<T> actualHandler
                    ) {
                        return instance.interceptHandler(action, executor, forceExecution, actualHandler);
                    }

                    @Override
                    public AsyncSender interceptSender(AsyncSender sender) {
                        return instance.interceptSender(sender);
                    }
                });
            }
        }

        final CountDownLatch clusterStateLatch = new CountDownLatch(1);

        @Override
        public AsyncSender interceptSender(AsyncSender sender) {
            return new AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    final ClusterName clusterName = new ClusterName("cluster1");
                    if (TransportLivenessAction.NAME.equals(action)) {
                        assertHeaders(threadPool);
                        ((TransportResponseHandler<LivenessResponse>) handler).handleResponse(
                            new LivenessResponse(clusterName, connection.getNode())
                        );
                    } else if (ClusterStateAction.NAME.equals(action)) {
                        assertHeaders(threadPool);
                        ClusterName cluster1 = clusterName;
                        ClusterState.Builder builder = ClusterState.builder(cluster1);
                        // the sniffer detects only data nodes
                        builder.nodes(
                            DiscoveryNodes.builder()
                                .add(
                                    new DiscoveryNode(
                                        "node_id",
                                        "someId",
                                        "some_ephemeralId_id",
                                        address.address().getHostString(),
                                        address.getAddress(),
                                        address,
                                        Collections.emptyMap(),
                                        Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
                                        Version.CURRENT
                                    )
                                )
                        );
                        ((TransportResponseHandler<ClusterStateResponse>) handler).handleResponse(
                            new ClusterStateResponse(cluster1, builder.build(), false)
                        );
                        clusterStateLatch.countDown();
                    } else if (TransportService.HANDSHAKE_ACTION_NAME.equals(action)) {
                        ((TransportResponseHandler<TransportService.HandshakeResponse>) handler).handleResponse(
                            new TransportService.HandshakeResponse(connection.getNode(), clusterName, connection.getNode().getVersion())
                        );
                    } else {
                        handler.handleException(new TransportException("", new InternalException(action)));
                    }
                }
            };
        }
    }
}

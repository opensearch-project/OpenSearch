/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.extensions.DiscoveryExtension;
import org.opensearch.extensions.ExtensionBooleanResponse;
import org.opensearch.extensions.RegisterTransportActionsRequest;
import org.opensearch.extensions.rest.RestSendToExtensionActionTests;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ActionNotFoundTransportException;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class ExtensionActionsTests extends OpenSearchTestCase {
    private TransportService transportService;
    private MockNioTransport transport;
    private DiscoveryExtension discoveryExtension;
    private ExtensionActions extensionActions;
    private NodeClient client;
    private final ThreadPool threadPool = new TestThreadPool(RestSendToExtensionActionTests.class.getSimpleName());

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        transport = new MockNioTransport(
            settings,
            Version.CURRENT,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService()
        );
        transportService = new MockTransportService(
            settings,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            (boundAddress) -> new DiscoveryNode(
                "test_node",
                "test_node",
                boundAddress.publishAddress(),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            ),
            null,
            Collections.emptySet()
        );
        discoveryExtension = new DiscoveryExtension(
            "firstExtension",
            "uniqueid1",
            "uniqueid1",
            "myIndependentPluginHost1",
            "127.0.0.0",
            new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
            new HashMap<String, String>(),
            Version.fromString("3.0.0"),
            new PluginInfo(
                "firstExtension",
                "Fake description 1",
                "0.0.7",
                Version.fromString("3.0.0"),
                "14",
                "fakeClass1",
                new ArrayList<String>(),
                false
            )
        );
        client = new NoOpNodeClient(this.getTestName());
        extensionActions = new ExtensionActions(Map.of("uniqueid1", discoveryExtension), transportService, client);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        client.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRegisterAction() {
        String action = "test-action";
        extensionActions.registerAction(action, discoveryExtension);
        assertEquals(discoveryExtension, extensionActions.getExtension(action));

        // Test duplicate action registration
        expectThrows(IllegalArgumentException.class, () -> extensionActions.registerAction(action, discoveryExtension));
        assertEquals(discoveryExtension, extensionActions.getExtension(action));
    }

    public void testRegisterTransportActionsRequest() {
        String action = "test-action";
        RegisterTransportActionsRequest request = new RegisterTransportActionsRequest(
            "uniqueid1",
            Map.of(action, ExtensionActionsTests.class)
        );
        ExtensionBooleanResponse response = (ExtensionBooleanResponse) extensionActions.handleRegisterTransportActionsRequest(request);
        assertTrue(response.getStatus());
        assertEquals(discoveryExtension, extensionActions.getExtension(action));

        // Test duplicate action registration
        response = (ExtensionBooleanResponse) extensionActions.handleRegisterTransportActionsRequest(request);
        assertFalse(response.getStatus());
    }

    public void testTransportActionRequestFromExtension() throws InterruptedException {
        String action = "test-action";
        byte[] requestBytes = "requestBytes".getBytes(StandardCharsets.UTF_8);
        TransportActionRequestFromExtension request = new TransportActionRequestFromExtension(action, requestBytes, "uniqueid1");
        // NoOpNodeClient returns null as response
        expectThrows(NullPointerException.class, () -> extensionActions.handleTransportActionRequestFromExtension(request));
    }

    public void testSendTransportRequestToExtension() throws InterruptedException {
        String action = "test-action";
        byte[] requestBytes = "request-bytes".getBytes(StandardCharsets.UTF_8);
        ExtensionActionRequest request = new ExtensionActionRequest(action, requestBytes);

        // Action not registered, expect exception
        expectThrows(ActionNotFoundTransportException.class, () -> extensionActions.sendTransportRequestToExtension(request));

        // Register Action
        RegisterTransportActionsRequest registerRequest = new RegisterTransportActionsRequest(
            "uniqueid1",
            Map.of(action, ExtensionActionsTests.class)
        );
        ExtensionBooleanResponse response = (ExtensionBooleanResponse) extensionActions.handleRegisterTransportActionsRequest(
            registerRequest
        );
        assertTrue(response.getStatus());

        ExtensionActionResponse extensionResponse = extensionActions.sendTransportRequestToExtension(request);
        assertEquals(
            "Request failed: [firstExtension][127.0.0.0:9300] Node not connected",
            new String(extensionResponse.getResponseBytes(), StandardCharsets.UTF_8)
        );
    }
}

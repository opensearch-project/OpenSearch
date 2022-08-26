/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.extensions.DiscoveryExtension;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;

public class RestSendToExtensionActionTests extends OpenSearchTestCase {

    private TransportService transportService;
    private MockNioTransport transport;
    private DiscoveryExtension discoveryExtension;
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
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRestSendToExtensionAction() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo", "PUT /bar", "POST /baz")
        );
        RestSendToExtensionAction restSendToExtensionAction = new RestSendToExtensionAction(
            registerRestActionRequest,
            discoveryExtension,
            transportService
        );

        assertEquals("send_to_extension_action", restSendToExtensionAction.getName());
        List<Route> expected = new ArrayList<>();
        expected.add(new Route(Method.GET, "/foo"));
        expected.add(new Route(Method.PUT, "/bar"));
        expected.add(new Route(Method.POST, "/baz"));

        List<Route> routes = restSendToExtensionAction.routes();
        assertEquals(expected.size(), routes.size());
        assertTrue(routes.containsAll(expected));
        assertTrue(expected.containsAll(routes));
    }

    public void testRestSendToExtensionActionBadMethod() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("/foo", "PUT /bar", "POST /baz")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(registerRestActionRequest, discoveryExtension, transportService)
        );
    }

    public void testRestSendToExtensionActionMissingUri() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET", "PUT /bar", "POST /baz")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(registerRestActionRequest, discoveryExtension, transportService)
        );
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;

public class RestInitializeExtensionActionTests extends OpenSearchTestCase {

    private TransportService transportService;
    private MockNioTransport transport;
    private final ThreadPool threadPool = new TestThreadPool(RestInitializeExtensionActionTests.class.getSimpleName());

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

    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRestInitializeExtensionActionResponse() throws Exception {
        ExtensionsManager extensionsManager = mock(ExtensionsManager.class);
        RestInitializeExtensionAction restInitializeExtensionAction = new RestInitializeExtensionAction(extensionsManager);
        final String content = "{\"name\":\"ad-extension\",\"uniqueId\":\"ad-extension\",\"hostAddress\":\"127.0.0.1\","
            + "\"port\":\"4532\",\"version\":\"1.0\",\"opensearchVersion\":\"3.0.0\","
            + "\"minimumCompatibleVersion\":\"3.0.0\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), XContentType.JSON)
            .withMethod(RestRequest.Method.POST)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        restInitializeExtensionAction.handleRequest(request, channel, null);

        assertEquals(channel.capturedResponse().status(), RestStatus.ACCEPTED);
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("A request to initialize an extension has been sent."));
    }

    public void testRestInitializeExtensionActionFailure() throws Exception {
        ExtensionsManager extensionsManager = new ExtensionsManager(Set.of());
        RestInitializeExtensionAction restInitializeExtensionAction = new RestInitializeExtensionAction(extensionsManager);

        final String content = "{\"name\":\"ad-extension\",\"uniqueId\":\"\",\"hostAddress\":\"127.0.0.1\","
            + "\"port\":\"4532\",\"version\":\"1.0\",\"opensearchVersion\":\"3.0.0\","
            + "\"minimumCompatibleVersion\":\"3.0.0\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), XContentType.JSON)
            .withMethod(RestRequest.Method.POST)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        restInitializeExtensionAction.handleRequest(request, channel, null);

        assertEquals(1, channel.errors().get());
        assertTrue(
            channel.capturedResponse().content().utf8ToString().contains("Required field [extension uniqueId] is missing in the request")
        );
    }

}

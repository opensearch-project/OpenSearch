/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.extensions.DiscoveryExtensionNode;
import org.opensearch.extensions.ExtensionDependency;
import org.opensearch.extensions.ExtensionsManagerTests;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class InitializeExtensionRequestTest extends OpenSearchTestCase {

    private MockTransportService transportService;
    private MockNioTransport transport;
    private final ThreadPool threadPool = new TestThreadPool(ExtensionsManagerTests.class.getSimpleName());

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

    public void testinitializeExtensionRequest() throws Exception {
        String expectedUniqueId = "test uniqueid";
        Version expectedVersion = Version.fromString("2.0.0");
        ExtensionDependency expectedDependency = new ExtensionDependency(expectedUniqueId, expectedVersion);
        DiscoveryExtensionNode extensionNode = new DiscoveryExtensionNode(
            "firstExtension",
            "uniqueid1",
            new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
            new HashMap<String, String>(),
            Version.fromString("3.0.0"),
            Version.fromString("3.0.0"),
            List.of(expectedDependency)
        );

        System.out.println(extensionNode.getName());
        InitializeExtensionRequest initializeExtensionRequest = new InitializeExtensionRequest(
            transportService.getLocalDiscoNode(),
            extensionNode
        );
        assertEquals(extensionNode, initializeExtensionRequest.getExtension());
        assertEquals(transportService.getLocalNode(), initializeExtensionRequest.getSourceNode());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            initializeExtensionRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                initializeExtensionRequest = new InitializeExtensionRequest(in);

                assertEquals(extensionNode, initializeExtensionRequest.getExtension());
                assertEquals(transportService.getLocalNode(), initializeExtensionRequest.getSourceNode());
            }
        }
    }
}

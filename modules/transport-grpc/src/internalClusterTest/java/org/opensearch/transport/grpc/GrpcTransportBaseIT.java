/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.grpc.health.v1.HealthCheckResponse;

import static org.opensearch.transport.AuxTransport.AUX_TRANSPORT_TYPES_KEY;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;

/**
 * Base test class for gRPC transport integration tests.
 */
public abstract class GrpcTransportBaseIT extends OpenSearchIntegTestCase {

    // Common constants
    protected static final String DEFAULT_INDEX_NAME = "test-grpc-index";
    protected static final String DEFAULT_DOCUMENT_SOURCE = "{\"field1\":\"value1\",\"field2\":42}";

    /**
     * Gets a random gRPC transport address from the cluster.
     *
     * @return A random transport address
     */
    protected TransportAddress randomNetty4GrpcServerTransportAddr() {
        List<TransportAddress> addresses = new ArrayList<>();
        for (Netty4GrpcServerTransport transport : internalCluster().getInstances(Netty4GrpcServerTransport.class)) {
            TransportAddress tAddr = new TransportAddress(transport.getBoundAddress().publishAddress().address());
            addresses.add(tAddr);
        }
        return randomFrom(addresses);
    }

    /**
     * Configures node settings for gRPC transport.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(AUX_TRANSPORT_TYPES_KEY, GRPC_TRANSPORT_SETTING_KEY).build();
    }

    /**
     * Configures plugins for gRPC transport.
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GrpcPlugin.class);
    }

    /**
     * Verifies that the gRPC transport is started on all nodes.
     */
    protected void verifyGrpcTransportStarted() {
        for (String nodeName : internalCluster().getNodeNames()) {
            Netty4GrpcServerTransport transport = internalCluster().getInstance(Netty4GrpcServerTransport.class, nodeName);
            assertNotNull("gRPC transport should be started on node " + nodeName, transport);

            // Verify that the transport is bound to an address
            TransportAddress[] boundAddresses = transport.getBoundAddress().boundAddresses();
            assertTrue("gRPC transport should be bound to at least one address", boundAddresses.length > 0);

            // Log the bound addresses for debugging
            for (TransportAddress address : boundAddresses) {
                InetSocketAddress inetAddress = address.address();
                logger.info("Node {} gRPC transport bound to {}", nodeName, NetworkAddress.format(inetAddress));
            }
        }
    }

    /**
     * Creates and ensures a test index is green.
     *
     * @param indexName The name of the index to create
     */
    protected void createTestIndex(String indexName) {
        createIndex(indexName);
        ensureGreen(indexName);
    }

    /**
     * Creates a gRPC client connected to a random node.
     *
     * @return A new NettyGrpcClient instance
     * @throws javax.net.ssl.SSLException if there's an SSL error
     */
    protected NettyGrpcClient createGrpcClient() throws javax.net.ssl.SSLException {
        return new NettyGrpcClient.Builder().setAddress(randomNetty4GrpcServerTransportAddr()).build();
    }

    /**
     * Indexes a test document and refreshes the index.
     *
     * @param indexName The name of the index
     * @param id The document ID
     * @param source The document source
     * @return The IndexResponse
     */
    protected IndexResponse indexTestDocument(String indexName, String id, String source) {
        IndexResponse response = client().prepareIndex(indexName).setId(id).setSource(source, XContentType.JSON).get();

        assertTrue(
            "Document should be indexed without shard failures",
            response.getShardInfo().getSuccessful() > 0 && response.getShardInfo().getFailed() == 0
        );

        // Refresh to make document searchable
        client().admin().indices().prepareRefresh(indexName).get();

        return response;
    }

    /**
     * Waits for a document to become searchable.
     *
     * @param indexName The name of the index
     * @param id The document ID
     * @throws Exception If the document doesn't become searchable within the timeout
     */
    protected void waitForSearchableDoc(String indexName, String id) throws Exception {
        assertBusy(() -> {
            refresh(indexName);
            assertTrue("Document should be searchable", client().prepareGet(indexName, id).get().isExists());
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Checks the health of the gRPC transport service.
     *
     * @throws Exception If the health check fails
     */
    protected void checkGrpcTransportHealth() throws Exception {
        try (NettyGrpcClient client = createGrpcClient()) {
            assertEquals(client.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugin.transport.grpc.ssl.NettyGrpcClient;
import org.opensearch.plugins.Plugin;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.grpc.ManagedChannel;

import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;
import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_TRANSPORT_TYPES_KEY;

public class SearchServiceIT extends OpenSearchIntegTestCase {

    private TransportAddress randomNetty4GrpcServerTransportAddr() {
        List<TransportAddress> addresses = new ArrayList<>();
        for (Netty4GrpcServerTransport transport : internalCluster().getInstances(Netty4GrpcServerTransport.class)) {
            TransportAddress tAddr = new TransportAddress(transport.getBoundAddress().publishAddress().address());
            addresses.add(tAddr);
        }
        return randomFrom(addresses);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(AUX_TRANSPORT_TYPES_KEY, GRPC_TRANSPORT_SETTING_KEY).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GrpcPlugin.class);
    }

    public void testSearchServiceSearch() throws Exception {
        // Create a test index
        String indexName = "test-search-index";
        createIndex(indexName);
        ensureGreen(indexName);

        // Add a document to the index
        IndexResponse indexResponse = client().prepareIndex(indexName)
            .setId("1")
            .setSource("{\"field1\":\"value1\",\"field2\":42}", XContentType.JSON)
            .get();

        assertEquals("Document should be indexed", 1, indexResponse.getShardInfo().getSuccessful());

        // Refresh the index to make the document searchable
        client().admin().indices().prepareRefresh(indexName).get();

        // Create a gRPC client
        try (NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(randomNetty4GrpcServerTransportAddr()).build()) {
            // Create a SearchService stub
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);

            // Create a search request
            SearchRequestBody requestBody = SearchRequestBody.newBuilder().setFrom(0).setSize(10).build();

            SearchRequest searchRequest = SearchRequest.newBuilder()
                .addIndex(indexName)
                .setRequestBody(requestBody)
                .setQ("field1:value1")
                .build();

            // Execute the search request
            SearchResponse searchResponse = searchStub.search(searchRequest);

            // Verify the response
            assertNotNull("Search response should not be null", searchResponse);
            assertTrue(
                "Search response should have hits",
                searchResponse.getResponseBody().getHits().getTotal().getTotalHits().getValue() > 0
            );
            assertEquals("Search response should have one hit", 1, searchResponse.getResponseBody().getHits().getHitsCount());
            assertEquals("Hit should have correct ID", "1", searchResponse.getResponseBody().getHits().getHits(0).getId());
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugin.transport.grpc.ssl.NettyGrpcClient;
import org.opensearch.plugins.Plugin;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.BulkResponse;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.protobufs.services.DocumentServiceGrpc;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.grpc.ManagedChannel;

import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;
import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_TRANSPORT_TYPES_KEY;

public class DocumentServiceIT extends OpenSearchIntegTestCase {

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

    public void testDocumentServiceBulk() throws Exception {
        // Create a test index
        String indexName = "test-bulk-index";
        createIndex(indexName);
        ensureGreen(indexName);

        // Create a gRPC client
        try (NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(randomNetty4GrpcServerTransportAddr()).build()) {
            // Create a DocumentService stub
            ManagedChannel channel = client.getChannel();
            DocumentServiceGrpc.DocumentServiceBlockingStub documentStub = DocumentServiceGrpc.newBlockingStub(channel);

            // Create a bulk request with an index operation
            IndexOperation indexOp = IndexOperation.newBuilder().setIndex(indexName).setId("1").build();

            BulkRequestBody requestBody = BulkRequestBody.newBuilder()
                .setIndex(indexOp)
                .setDoc(com.google.protobuf.ByteString.copyFromUtf8("{\"field1\":\"value1\",\"field2\":42}"))
                .build();

            BulkRequest bulkRequest = BulkRequest.newBuilder().addRequestBody(requestBody).build();

            // Execute the bulk request
            BulkResponse bulkResponse = documentStub.bulk(bulkRequest);

            // Verify the response
            assertNotNull("Bulk response should not be null", bulkResponse);
            assertFalse("Bulk response should not have errors", bulkResponse.getBulkResponseBody().getErrors());
            assertEquals("Bulk response should have one item", 1, bulkResponse.getBulkResponseBody().getItemsCount());
        }
    }
}

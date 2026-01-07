/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.test;

import com.google.protobuf.ByteString;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.Plugin;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.BulkResponse;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.protobufs.Item;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.OperationContainer;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.Refresh;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.protobufs.services.DocumentServiceGrpc;
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.grpc.GrpcPlugin;
import org.opensearch.transport.grpc.Netty4GrpcServerTransport;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import io.grpc.ManagedChannel;

import static org.opensearch.transport.AuxTransport.AUX_TRANSPORT_TYPES_KEY;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;

/**
 * Base test class for gRPC transport integration tests.
 */
public abstract class GrpcOpenSearchIntegTestCase extends OpenSearchIntegTestCase {

    public static class GrpcTestBulkResponse {
        private final BulkResponse protoBulkResponse;

        GrpcTestBulkResponse(BulkResponse protoBulkResponse) {
            this.protoBulkResponse = protoBulkResponse;
        }

        public int getCount() {
            return protoBulkResponse.getItemsCount();
        }

        public int getErrors() {
            int errors = 0;
            for (int i = 0; i < protoBulkResponse.getItemsCount(); i++) {
                Item item = protoBulkResponse.getItems(i);
                Item.ItemCase itemCase = item.getItemCase();
                switch (itemCase) {
                    case CREATE -> {
                        if (item.hasCreate() && item.getCreate().hasError()) {
                            errors++;
                        }
                    }
                    case DELETE -> {
                        if (item.hasDelete() && item.getDelete().hasError()) {
                            errors++;
                        }
                    }
                    case INDEX -> {
                        if (item.hasIndex() && item.getIndex().hasError()) {
                            errors++;
                        }
                    }
                    case UPDATE -> {
                        if (item.hasUpdate() && item.getUpdate().hasError()) {
                            errors++;
                        }
                    }
                    case ITEM_NOT_SET -> {
                    }
                }
            }
            return errors;
        }
    }

    public static class GrpcTestSearchResponse {
        private final SearchResponse protoSearchResponse;

        GrpcTestSearchResponse(SearchResponse protoSearchResponse) {
            this.protoSearchResponse = protoSearchResponse;
        }

        public long getTotalCount() {
            return protoSearchResponse.getHits().getTotal().getTotalHits().getValue();
        }

        public String getDocumentSource(int i) {
            return protoSearchResponse.getHits().getHits(i).getXSource().toString();
        }
    }

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
     * Creates a gRPC client connected to a random node.
     * @return A new NettyGrpcClient instance
     * @throws javax.net.ssl.SSLException if there's an SSL error
     */
    protected NettyGrpcClient createGrpcClient() throws javax.net.ssl.SSLException {
        return new NettyGrpcClient.Builder().setAddress(randomNetty4GrpcServerTransportAddr()).build();
    }

    /**
     * Helper method to index a number of auto generated test documents.
     * Documents generated with the form: { "field": "doc 2 body" }
     * @param channel The gRPC channel to use for the indexing operation.
     * @param index The index to which documents should be indexed.
     * @param numDocs The number of documents to generate and index.
     * @return A GrpcTestBulkResponse containing the results of the indexing operation.
     */
    protected static GrpcTestBulkResponse doBulk(ManagedChannel channel, String index, long numDocs) {
        BulkRequest.Builder requestBuilder = BulkRequest.newBuilder().setRefresh(Refresh.REFRESH_TRUE).setIndex(index);

        for (int i = 0; (long) i < numDocs; ++i) {
            String docBody = String.format(Locale.ROOT, "{\n    \"field\": \"doc %d body\"\n}\n", i);
            IndexOperation.Builder indexOp = IndexOperation.newBuilder().setXId(String.valueOf(i));
            OperationContainer.Builder opCont = OperationContainer.newBuilder().setIndex(indexOp);
            BulkRequestBody requestBody = BulkRequestBody.newBuilder()
                .setOperationContainer(opCont)
                .setObject(ByteString.copyFromUtf8(docBody))
                .build();
            requestBuilder.addBulkRequestBody(requestBody);
        }

        DocumentServiceGrpc.DocumentServiceBlockingStub stub = DocumentServiceGrpc.newBlockingStub(channel);
        return new GrpcTestBulkResponse(stub.bulk(requestBuilder.build()));
    }

    /**
     * Helper method to index a number of provided test documents.
     * @param channel The gRPC channel to use for the indexing operation.
     * @param index The index to which documents should be indexed.
     * @param docs List of test documents to index.
     * @return A GrpcTestBulkResponse containing the results of the indexing operation.
     */
    protected static GrpcTestBulkResponse doBulk(ManagedChannel channel, String index, List<String> docs) {
        BulkRequest.Builder requestBuilder = BulkRequest.newBuilder().setRefresh(Refresh.REFRESH_TRUE).setIndex(index);

        for (String doc : docs) {
            IndexOperation.Builder indexOp = IndexOperation.newBuilder();
            OperationContainer.Builder opCont = OperationContainer.newBuilder().setIndex(indexOp);
            BulkRequestBody requestBody = BulkRequestBody.newBuilder()
                .setOperationContainer(opCont)
                .setObject(ByteString.copyFromUtf8(doc))
                .build();
            requestBuilder.addBulkRequestBody(requestBody);
        }

        DocumentServiceGrpc.DocumentServiceBlockingStub stub = DocumentServiceGrpc.newBlockingStub(channel);
        return new GrpcTestBulkResponse(stub.bulk(requestBuilder.build()));
    }

    /**
     * Helper method to perform a match-all search operation.
     * @param channel The gRPC channel to use for the search operation.
     * @param index The index to search.
     * @param size The maximum number of results to return.
     * @return A GrpcTestSearchResponse containing the results of the search operation.
     */
    protected static GrpcTestSearchResponse doMatchAll(ManagedChannel channel, String index, int size) {
        QueryContainer query = QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();
        SearchRequestBody requestBody = SearchRequestBody.newBuilder().setSize(size).setQuery(query).build();
        SearchRequest searchRequest = SearchRequest.newBuilder().addIndex(index).setSearchRequestBody(requestBody).build();
        SearchServiceGrpc.SearchServiceBlockingStub stub = SearchServiceGrpc.newBlockingStub(channel);
        return new GrpcTestSearchResponse(stub.search(searchRequest));
    }
}

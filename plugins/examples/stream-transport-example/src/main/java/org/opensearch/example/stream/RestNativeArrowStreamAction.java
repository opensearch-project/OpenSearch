/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class RestNativeArrowStreamAction extends BaseRestHandler {

    private final Supplier<DiscoveryNodes> nodesLookup;
    private final Supplier<StreamTransportService> stsSupplier;

    public RestNativeArrowStreamAction(Supplier<DiscoveryNodes> nodesLookup, Supplier<StreamTransportService> stsSupplier) {
        this.nodesLookup = nodesLookup;
        this.stsSupplier = stsSupplier;
    }

    @Override
    public String getName() {
        return "native_arrow_stream_test";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_native_arrow_test"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        int batches = request.paramAsInt("batches", 2);
        int rows = request.paramAsInt("rows", 3);

        return channel -> {
            StreamTransportService sts = stsSupplier.get();
            DiscoveryNode localNode = nodesLookup.get().getLocalNode();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> result = new AtomicReference<>();
            AtomicReference<Exception> failure = new AtomicReference<>();

            sts.sendRequest(
                localNode,
                NativeArrowStreamDataAction.NAME,
                new NativeArrowStreamDataRequest(batches, rows),
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                new StreamTransportResponseHandler<NativeArrowStreamDataResponse>() {
                    @Override
                    public void handleStreamResponse(StreamTransportResponse<NativeArrowStreamDataResponse> response) {
                        try {
                            int batchCount = 0;
                            int totalRows = 0;
                            NativeArrowStreamDataResponse r;
                            while ((r = response.nextResponse()) != null) {
                                batchCount++;
                                totalRows += r.getRoot().getRowCount();
                            }
                            response.close();
                            result.set("{\"batches\":" + batchCount + ",\"total_rows\":" + totalRows + "}");
                            latch.countDown();
                        } catch (Exception e) {
                            failure.set(e);
                            response.cancel("error", e);
                            latch.countDown();
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        failure.set(exp);
                        latch.countDown();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }

                    @Override
                    public NativeArrowStreamDataResponse read(StreamInput in) throws IOException {
                        return new NativeArrowStreamDataResponse(in);
                    }
                }
            );

            latch.await(10, TimeUnit.SECONDS);
            if (failure.get() != null) {
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "text/plain", failure.get().toString()));
            } else if (result.get() != null) {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", result.get()));
            } else {
                channel.sendResponse(new BytesRestResponse(RestStatus.REQUEST_TIMEOUT, "text/plain", "timeout"));
            }
        };
    }
}

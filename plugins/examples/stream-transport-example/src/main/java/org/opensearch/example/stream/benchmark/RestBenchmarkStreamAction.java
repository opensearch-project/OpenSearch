/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.example.stream.benchmark;

import org.opensearch.example.stream.StreamTransportExamplePlugin;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for benchmark stream action
 */
public class RestBenchmarkStreamAction extends BaseRestHandler {

    /** Constructor */
    public RestBenchmarkStreamAction() {}

    @Override
    public String getName() {
        return "benchmark_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_benchmark/stream"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        BenchmarkStreamRequest benchmarkRequest = new BenchmarkStreamRequest();

        benchmarkRequest.setRows(request.paramAsInt("rows", 100));
        benchmarkRequest.setColumns(request.paramAsInt("columns", 10));
        benchmarkRequest.setAvgColumnLength(request.paramAsInt("avg_column_length", 100));
        benchmarkRequest.setColumnType(request.param("column_type", "string"));
        benchmarkRequest.setUseStreamTransport(request.paramAsBoolean("use_stream_transport", true));
        benchmarkRequest.setParallelRequests(request.paramAsInt("parallel_requests", 1));
        benchmarkRequest.setTotalRequests(request.paramAsInt("total_requests", 0));
        benchmarkRequest.setTargetTps(request.paramAsInt("target_tps", 0));
        benchmarkRequest.setThreadPool(request.param("thread_pool", StreamTransportExamplePlugin.BENCHMARK_THREAD_POOL_NAME));
        benchmarkRequest.setBatchSize(request.paramAsInt("batch_size", 100));

        return channel -> client.execute(BenchmarkStreamAction.INSTANCE, benchmarkRequest, new RestToXContentListener<>(channel));
    }
}

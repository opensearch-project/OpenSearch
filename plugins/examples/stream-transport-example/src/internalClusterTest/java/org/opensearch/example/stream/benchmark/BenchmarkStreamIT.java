/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.example.stream.benchmark;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.example.stream.StreamTransportExamplePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchTestCase.LockFeatureFlag;

import java.util.Collection;
import java.util.List;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class BenchmarkStreamIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(StreamTransportExamplePlugin.class, FlightStreamPlugin.class);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testBasicBenchmark() {
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        BenchmarkStreamRequest request = new BenchmarkStreamRequest();
        request.setRows(10);
        request.setColumns(5);
        request.setAvgColumnLength(100);
        request.setParallelRequests(1);
        request.setUseStreamTransport(true);

        BenchmarkStreamResponse response = client().execute(BenchmarkStreamAction.INSTANCE, request).actionGet();

        assertThat(response.getTotalRows(), equalTo(10L));
        assertThat(response.getTotalBytes(), greaterThan(0L));
        assertThat(response.getDurationMs(), greaterThan(0L));
        assertThat(response.getThroughputRowsPerSec(), greaterThan(0.0));
        assertThat(response.getMinLatencyMs(), greaterThan(0L));
        assertThat(response.getMaxLatencyMs(), greaterThanOrEqualTo(response.getMinLatencyMs()));
        assertThat(response.getParallelRequests(), equalTo(1));
        assertTrue(response.isUsedStreamTransport());
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testParallelRequests() {
        BenchmarkStreamRequest request = new BenchmarkStreamRequest();
        request.setRows(50);
        request.setColumns(10);
        request.setParallelRequests(5);
        request.setUseStreamTransport(true);

        BenchmarkStreamResponse response = client().execute(BenchmarkStreamAction.INSTANCE, request).actionGet();

        assertThat(response.getTotalRows(), equalTo(250L)); // 50 rows × 5 parallel
        assertThat(response.getParallelRequests(), equalTo(5));
        assertThat(response.getP90LatencyMs(), greaterThan(0L));
        assertThat(response.getP99LatencyMs(), greaterThanOrEqualTo(response.getP90LatencyMs()));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamVsRegularTransport() {
        // Test with stream transport
        BenchmarkStreamRequest streamRequest = new BenchmarkStreamRequest();
        streamRequest.setRows(100);
        streamRequest.setColumns(10);
        streamRequest.setParallelRequests(2);
        streamRequest.setUseStreamTransport(true);

        BenchmarkStreamResponse streamResponse = client().execute(BenchmarkStreamAction.INSTANCE, streamRequest).actionGet();
        assertTrue(streamResponse.isUsedStreamTransport());
        assertThat(streamResponse.getTotalRows(), equalTo(200L));

        // Test with regular transport
        BenchmarkStreamRequest regularRequest = new BenchmarkStreamRequest();
        regularRequest.setRows(100);
        regularRequest.setColumns(10);
        regularRequest.setParallelRequests(2);
        regularRequest.setUseStreamTransport(false);

        BenchmarkStreamResponse regularResponse = client().execute(BenchmarkStreamAction.INSTANCE, regularRequest).actionGet();
        assertFalse(regularResponse.isUsedStreamTransport());
        assertThat(regularResponse.getTotalRows(), equalTo(200L));

        // Both should produce results
        assertThat(streamResponse.getThroughputRowsPerSec(), greaterThan(0.0));
        assertThat(regularResponse.getThroughputRowsPerSec(), greaterThan(0.0));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testLargePayload() {
        BenchmarkStreamRequest request = new BenchmarkStreamRequest();
        request.setRows(1000);
        request.setColumns(20);
        request.setAvgColumnLength(500);
        request.setParallelRequests(3);

        BenchmarkStreamResponse response = client().execute(BenchmarkStreamAction.INSTANCE, request).actionGet();

        assertThat(response.getTotalRows(), equalTo(3000L));
        assertThat(response.getTotalBytes(), greaterThan(1_000_000L)); // > 1MB
        assertThat(response.getThroughputMbPerSec(), greaterThan(0.0));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testValidation() {
        BenchmarkStreamRequest request = new BenchmarkStreamRequest();
        request.setRows(-1);

        try {
            client().execute(BenchmarkStreamAction.INSTANCE, request).actionGet();
            fail("Expected validation exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("rows must be > 0"));
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testThreadPoolConfiguration() {
        BenchmarkStreamRequest request = new BenchmarkStreamRequest();
        request.setRows(10);
        request.setThreadPool("generic");
        request.setParallelRequests(1);

        BenchmarkStreamResponse response = client().execute(BenchmarkStreamAction.INSTANCE, request).actionGet();
        assertThat(response.getTotalRows(), equalTo(10L));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testLatencyPercentiles() {
        BenchmarkStreamRequest request = new BenchmarkStreamRequest();
        request.setRows(50);
        request.setParallelRequests(10);

        BenchmarkStreamResponse response = client().execute(BenchmarkStreamAction.INSTANCE, request).actionGet();

        // Verify percentile ordering
        assertThat(response.getMinLatencyMs(), lessThanOrEqualTo(response.getAvgLatencyMs()));
        assertThat(response.getAvgLatencyMs(), lessThanOrEqualTo(response.getP90LatencyMs()));
        assertThat(response.getP90LatencyMs(), lessThanOrEqualTo(response.getP99LatencyMs()));
        assertThat(response.getP99LatencyMs(), lessThanOrEqualTo(response.getMaxLatencyMs()));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testCustomBatchSize() {
        BenchmarkStreamRequest smallBatchRequest = new BenchmarkStreamRequest();
        smallBatchRequest.setRows(100);
        smallBatchRequest.setBatchSize(10);
        smallBatchRequest.setUseStreamTransport(true);

        BenchmarkStreamResponse smallBatchResponse = client().execute(BenchmarkStreamAction.INSTANCE, smallBatchRequest).actionGet();
        assertThat(smallBatchResponse.getTotalRows(), equalTo(100L));
        assertTrue(smallBatchResponse.isUsedStreamTransport());

        BenchmarkStreamRequest largeBatchRequest = new BenchmarkStreamRequest();
        largeBatchRequest.setRows(100);
        largeBatchRequest.setBatchSize(500);
        largeBatchRequest.setUseStreamTransport(true);

        BenchmarkStreamResponse largeBatchResponse = client().execute(BenchmarkStreamAction.INSTANCE, largeBatchRequest).actionGet();
        assertThat(largeBatchResponse.getTotalRows(), equalTo(100L));
        assertTrue(largeBatchResponse.isUsedStreamTransport());
    }
}

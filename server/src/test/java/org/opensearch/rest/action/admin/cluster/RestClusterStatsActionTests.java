/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.object.HasToString.hasToString;
import static org.mockito.Mockito.mock;

public class RestClusterStatsActionTests extends OpenSearchTestCase {

    private RestClusterStatsAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestClusterStatsAction();
    }

    public void testFromRequestBasePath() {
        final HashMap<String, String> params = new HashMap<>();
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_cluster/stats").withParams(params).build();
        ClusterStatsRequest clusterStatsRequest = RestClusterStatsAction.fromRequest(request);
        assertNotNull(clusterStatsRequest);
        assertTrue(clusterStatsRequest.useAggregatedNodeLevelResponses());
        assertFalse(clusterStatsRequest.computeAllMetrics());
        assertNotNull(clusterStatsRequest.requestedMetrics());
        assertFalse(clusterStatsRequest.requestedMetrics().isEmpty());
        for (ClusterStatsRequest.Metric metric : ClusterStatsRequest.Metric.values()) {
            assertTrue(clusterStatsRequest.requestedMetrics().contains(metric));
        }
        assertNotNull(clusterStatsRequest.indicesMetrics());
        assertFalse(clusterStatsRequest.indicesMetrics().isEmpty());
        for (ClusterStatsRequest.IndexMetric indexMetric : ClusterStatsRequest.IndexMetric.values()) {
            assertTrue(clusterStatsRequest.indicesMetrics().contains(indexMetric));
        }
    }

    public void testFromRequestWithNodeStatsMetricsFilter() {
        Set<ClusterStatsRequest.Metric> metricsRequested = Set.of(ClusterStatsRequest.Metric.OS, ClusterStatsRequest.Metric.FS);
        String metricParam = metricsRequested.stream().map(ClusterStatsRequest.Metric::metricName).collect(Collectors.joining(","));
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", metricParam);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_cluster/stats").withParams(params).build();
        ClusterStatsRequest clusterStatsRequest = RestClusterStatsAction.fromRequest(request);
        assertNotNull(clusterStatsRequest);
        assertTrue(clusterStatsRequest.useAggregatedNodeLevelResponses());
        assertFalse(clusterStatsRequest.computeAllMetrics());
        assertFalse(clusterStatsRequest.requestedMetrics().isEmpty());
        assertEquals(2, clusterStatsRequest.requestedMetrics().size());
        assertEquals(metricsRequested, clusterStatsRequest.requestedMetrics());
        assertTrue(clusterStatsRequest.indicesMetrics().isEmpty());
    }

    public void testFromRequestWithIndicesStatsMetricsFilter() {
        Set<ClusterStatsRequest.Metric> metricsRequested = Set.of(
            ClusterStatsRequest.Metric.OS,
            ClusterStatsRequest.Metric.FS,
            ClusterStatsRequest.Metric.INDICES
        );
        Set<ClusterStatsRequest.IndexMetric> indicesMetricsRequested = Set.of(
            ClusterStatsRequest.IndexMetric.SHARDS,
            ClusterStatsRequest.IndexMetric.SEGMENTS
        );
        String metricParam = metricsRequested.stream().map(ClusterStatsRequest.Metric::metricName).collect(Collectors.joining(","));
        String indicesMetricParam = indicesMetricsRequested.stream()
            .map(ClusterStatsRequest.IndexMetric::metricName)
            .collect(Collectors.joining(","));
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", metricParam);
        params.put("index_metric", indicesMetricParam);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_cluster/stats").withParams(params).build();
        ClusterStatsRequest clusterStatsRequest = RestClusterStatsAction.fromRequest(request);
        assertNotNull(clusterStatsRequest);
        assertTrue(clusterStatsRequest.useAggregatedNodeLevelResponses());
        assertFalse(clusterStatsRequest.computeAllMetrics());
        assertFalse(clusterStatsRequest.requestedMetrics().isEmpty());
        assertEquals(3, clusterStatsRequest.requestedMetrics().size());
        assertEquals(metricsRequested, clusterStatsRequest.requestedMetrics());
        assertFalse(clusterStatsRequest.indicesMetrics().isEmpty());
        assertEquals(2, clusterStatsRequest.indicesMetrics().size());
        assertEquals(indicesMetricsRequested, clusterStatsRequest.indicesMetrics());
    }

    public void testUnrecognizedMetric() {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomAlphaOfLength(64);
        params.put("metric", metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_cluster/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_cluster/stats] contains unrecognized metric: [" + metric + "]")));
    }

    public void testUnrecognizedIndexMetric() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "_all,");
        final String indexMetric = randomAlphaOfLength(64);
        params.put("index_metric", indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_cluster/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_cluster/stats] contains unrecognized index metric: [" + indexMetric + "]")));
    }

    public void testAllMetricsRequestWithOtherMetric() {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomSubsetOf(1, RestClusterStatsAction.METRIC_REQUEST_CONSUMER_MAP.keySet()).get(0);
        params.put("metric", "_all," + metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_cluster/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_cluster/stats] contains _all and individual metrics [_all," + metric + "]")));
    }

    public void testAllIndexMetricsRequestWithOtherIndicesMetric() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "_all,");
        final String indexMetric = randomSubsetOf(1, RestClusterStatsAction.INDEX_METRIC_TO_REQUEST_CONSUMER_MAP.keySet()).get(0);
        params.put("index_metric", "_all," + indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_cluster/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(containsString("request [/_cluster/stats] contains _all and individual index metrics [_all," + indexMetric + "]"))
        );
    }

    public void testIndexMetricsRequestWithoutMetricIndices() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "os");
        final String indexMetric = randomSubsetOf(1, RestClusterStatsAction.INDEX_METRIC_TO_REQUEST_CONSUMER_MAP.keySet()).get(0);
        params.put("index_metric", indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_cluster/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(
                containsString("request [/_cluster/stats] contains index metrics [" + indexMetric + "] but indices stats not requested")
            )
        );
    }

}

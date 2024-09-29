/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.HashMap;

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

}

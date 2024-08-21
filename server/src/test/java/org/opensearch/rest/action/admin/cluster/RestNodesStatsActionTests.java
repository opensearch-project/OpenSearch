/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.object.HasToString.hasToString;
import static org.mockito.Mockito.mock;

public class RestNodesStatsActionTests extends OpenSearchTestCase {

    private RestNodesStatsAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestNodesStatsAction();
    }

    public void testUnrecognizedMetric() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomAlphaOfLength(64);
        params.put("metric", metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_nodes/stats] contains unrecognized metric: [" + metric + "]")));
    }

    public void testUnrecognizedMetricDidYouMean() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "os,transprot,unrecognized");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(
                containsString(
                    "request [/_nodes/stats] contains unrecognized metrics: [transprot] -> did you mean [transport]?, [unrecognized]"
                )
            )
        );
    }

    public void testAllRequestWithOtherMetrics() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        final String metric = randomSubsetOf(1, RestNodesStatsAction.METRICS.keySet()).get(0);
        params.put("metric", "_all," + metric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_nodes/stats] contains _all and individual metrics [_all," + metric + "]")));
    }

    public void testUnrecognizedIndexMetric() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "indices");
        final String indexMetric = randomAlphaOfLength(64);
        params.put("index_metric", indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_nodes/stats] contains unrecognized index metric: [" + indexMetric + "]")));
    }

    public void testUnrecognizedIndexMetricDidYouMean() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "indices");
        params.put("index_metric", "indexing,stroe,unrecognized");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(
                containsString(
                    "request [/_nodes/stats] contains unrecognized index metrics: [stroe] -> did you mean [store]?, [unrecognized]"
                )
            )
        );
    }

    public void testIndexMetricsRequestWithoutIndicesAndCachesMetrics() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        final Set<String> metrics = new HashSet<>(RestNodesStatsAction.METRICS.keySet());
        metrics.remove("indices");
        // caches stats is handled separately
        metrics.remove("caches");
        params.put("metric", randomSubsetOf(1, metrics).get(0));
        final String indexMetric = randomSubsetOf(1, RestNodesStatsAction.FLAGS.keySet()).get(0);
        params.put("index_metric", indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(
                containsString("request [/_nodes/stats] contains index metrics [" + indexMetric + "] but indices stats not requested")
            )
        );
    }

    public void testCacheStatsRequestWithInvalidCacheType() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "caches");
        final String cacheType = randomAlphaOfLength(64);
        params.put("index_metric", cacheType);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request [/_nodes/stats] contains unrecognized cache type: [" + cacheType + "]")));
    }

    public void testIndexMetricsRequestOnAllRequest() throws IOException {
        final HashMap<String, String> params = new HashMap<>();
        params.put("metric", "_all");
        final String indexMetric = randomSubsetOf(1, RestNodesStatsAction.FLAGS.keySet()).get(0);
        params.put("index_metric", indexMetric);
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_nodes/stats").withParams(params).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(
            e,
            hasToString(containsString("request [/_nodes/stats] contains index metrics [" + indexMetric + "] but all stats requested"))
        );
    }

}

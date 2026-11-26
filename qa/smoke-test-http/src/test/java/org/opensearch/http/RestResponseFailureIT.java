/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.ArrayList;
import java.util.Collection;

import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.BREAKER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies the REST layer still answers the client when downstream response preparation fails, the regression reported
 * in opensearch-project/OpenSearch#22311.
 * <p>
 * Before the fix, one flag represented both "request bytes released" and "response sent". If a downstream channel
 * rejected the initial response after the breaker release, the error response was rejected as a second send and the
 * client received no status line. The tests assert that the structured error reaches the client with request
 * correlation intact, the node remains responsive, and the breaker reservation is released exactly once.
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class RestResponseFailureIT extends HttpSmokeTestCase {

    private static final String OPAQUE_ID = "response-serialization-failure-it";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestResponseFailurePlugin.class);
        return plugins;
    }

    public void testRequestEntityTooLargeResponseIsSentWhenResponseBodyCannotBeSerialized() throws Exception {
        final ResponseException exception = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(responseFailureRequest())
        );

        final Response response = exception.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.REQUEST_ENTITY_TOO_LARGE.getStatus()));
        assertThat(response.getHeader("X-Opaque-Id"), equalTo(OPAQUE_ID));

        final String body = EntityUtils.toString(response.getEntity());
        assertThat(body, containsString("\"type\":\"status_exception\""));
        assertThat(body, containsString(TestResponseFailureRestAction.FAILURE_MESSAGE));
        assertThat(body, containsString("\"status\":" + RestStatus.REQUEST_ENTITY_TOO_LARGE.getStatus()));

        final Response healthResponse = getRestClient().performRequest(new Request("GET", "/_cluster/health"));
        assertThat(healthResponse.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        EntityUtils.consume(healthResponse.getEntity());
    }

    /**
     * The first response attempt releases the request-byte reservation before the downstream channel rejects it. The
     * follow-up failure response reuses the channel, so the breaker must be released exactly once - neither leaked nor
     * subtracted twice.
     */
    public void testInFlightRequestsBreakerIsReleasedWhenResponseBodyCannotBeSerialized() throws Exception {
        final long baseline = inFlightRequestsEstimatedBytes();

        expectThrows(ResponseException.class, () -> getRestClient().performRequest(responseFailureRequest()));

        assertBusy(() -> assertThat(inFlightRequestsEstimatedBytes(), equalTo(baseline)));
    }

    /**
     * The in-flight-requests breaker is shared with the transport layer, which reserves bytes for the very node stats
     * request used to read it, so the absolute value is never zero. Comparing two readings taken the same way isolates
     * what the HTTP request under test reserved and released.
     */
    private long inFlightRequestsEstimatedBytes() {
        final NodesStatsResponse stats = client().admin().cluster().prepareNodesStats().addMetric(BREAKER.metricName()).get();
        long estimated = 0L;
        for (final NodeStats nodeStats : stats.getNodes()) {
            estimated += nodeStats.getBreaker().getStats(CircuitBreaker.IN_FLIGHT_REQUESTS).getEstimated();
        }
        return estimated;
    }

    /**
     * The request carries a body so the in-flight-requests breaker reserves a non-zero number of bytes for it.
     */
    private static Request responseFailureRequest() {
        final Request request = new Request("POST", TestResponseFailureRestAction.ROUTE);
        request.setJsonEntity("{\"probe\":\"response-serialization-failure\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-Opaque-Id", OPAQUE_ID).build());
        return request;
    }
}

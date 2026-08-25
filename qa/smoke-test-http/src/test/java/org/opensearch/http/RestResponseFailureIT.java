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
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.ArrayList;
import java.util.Collection;

import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.BREAKER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies the REST layer still answers the client when serializing a response body fails, the regression reported in
 * opensearch-project/OpenSearch#22311.
 * <p>
 * Before the fix, {@code RestController.ResourceHandlingHttpChannel#close} threw
 * {@code IllegalStateException("Channel is already closed")} on the second {@code sendResponse}, so the error response
 * was dropped, {@code RestActionListener} logged "failed to send failure response", and the client was left waiting on
 * a connection that never received a status line. The test asserts the client gets a real HTTP response instead; an
 * unfixed build fails here on the REST client's socket timeout rather than passing.
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class RestResponseFailureIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestResponseFailurePlugin.class);
        return plugins;
    }

    public void testFailureResponseIsSentWhenResponseBodyCannotBeSerialized() throws Exception {
        final ResponseException exception = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(responseFailureRequest())
        );

        final Response response = exception.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(500));

        final String body = EntityUtils.toString(response.getEntity());
        assertThat(body, containsString("arithmetic_exception"));
        assertThat(body, containsString(TestResponseFailureRestAction.FAILURE_MESSAGE));
    }

    /**
     * Sending the failure response now goes through an already-closed channel, so the request bytes reserved on the
     * in-flight-requests breaker must still be released exactly once - neither leaked nor subtracted twice.
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
        return request;
    }
}

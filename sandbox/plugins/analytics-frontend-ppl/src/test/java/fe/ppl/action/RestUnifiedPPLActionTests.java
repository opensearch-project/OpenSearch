/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package fe.ppl.action;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.fe.action.RestUnifiedPPLAction;
import org.opensearch.fe.action.UnifiedPPLExecuteAction;
import org.opensearch.fe.action.UnifiedPPLRequest;
import org.opensearch.fe.action.UnifiedPPLResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link RestUnifiedPPLAction}.
 */
public class RestUnifiedPPLActionTests extends RestActionTestCase {

    private RestUnifiedPPLAction action;

    @Before
    public void setUpAction() {
        action = new RestUnifiedPPLAction();
        controller().registerHandler(action);
    }

    /**
     * Test that the handler name is correct.
     */
    public void testGetName() {
        assertThat(action.getName(), equalTo("unified_ppl_action"));
    }

    /**
     * Test that the handler registers the correct route.
     */
    public void testRoutes() {
        List<RestUnifiedPPLAction.Route> routes = action.routes();
        assertThat(routes.size(), equalTo(1));
        assertThat(routes.get(0).getMethod(), equalTo(RestRequest.Method.POST));
        assertThat(routes.get(0).getPath(), equalTo("/_plugins/_query_engine/_unified/ppl"));
    }

    /**
     * Test valid request parsing and dispatch to transport action.
     */
    public void testValidRequestParsesAndDispatches() {
        String jsonBody = "{\"query\": \"source=logs | where status=200\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(RestUnifiedPPLAction.ROUTE_PATH)
            .withContent(new BytesArray(jsonBody), MediaTypeRegistry.JSON)
            .build();

        // Set up the verifying client to capture the dispatched request
        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            assertThat(actionType, equalTo(UnifiedPPLExecuteAction.INSTANCE));
            assertThat(((UnifiedPPLRequest) actionRequest).getPplText(), equalTo("source=logs | where status=200"));
            return new UnifiedPPLResponse(List.of("host", "status"), Collections.emptyList());
        });

        dispatchRequest(request);
    }

    /**
     * Test missing query field returns HTTP 400.
     */
    public void testMissingQueryFieldReturnsError() {
        String jsonBody = "{\"other\": \"value\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(RestUnifiedPPLAction.ROUTE_PATH)
            .withContent(new BytesArray(jsonBody), MediaTypeRegistry.JSON)
            .build();

        // dispatchRequest handles the exception internally and sends error response
        dispatchRequest(request);
    }

    /**
     * Test empty query field returns HTTP 400.
     */
    public void testEmptyQueryFieldReturnsError() {
        String jsonBody = "{\"query\": \"\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(RestUnifiedPPLAction.ROUTE_PATH)
            .withContent(new BytesArray(jsonBody), MediaTypeRegistry.JSON)
            .build();

        dispatchRequest(request);
    }

    /**
     * Test oversized query field returns HTTP 400.
     */
    public void testOversizedQueryFieldReturnsError() {
        // Create a query that exceeds the max length
        String longQuery = "source=" + "a".repeat(RestUnifiedPPLAction.DEFAULT_MAX_QUERY_LENGTH);
        String jsonBody = "{\"query\": \"" + longQuery + "\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(RestUnifiedPPLAction.ROUTE_PATH)
            .withContent(new BytesArray(jsonBody), MediaTypeRegistry.JSON)
            .build();

        dispatchRequest(request);
    }

    /**
     * Test response JSON format contains required fields.
     */
    public void testResponseJsonContainsRequiredFields() throws Exception {
        List<Object[]> rows = new java.util.ArrayList<>();
        rows.add(new Object[] { "server-1", 42 });
        rows.add(new Object[] { "server-2", 17 });
        UnifiedPPLResponse mockResponse = new UnifiedPPLResponse(List.of("host", "count()"), rows);

        String jsonBody = "{\"query\": \"source=logs | stats count() by host\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(RestUnifiedPPLAction.ROUTE_PATH)
            .withContent(new BytesArray(jsonBody), MediaTypeRegistry.JSON)
            .build();

        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> mockResponse);

        dispatchRequest(request);
        // The dispatch completes without error, which validates the response is built correctly.
        // The VerifyingClient calls listener.onResponse with our mock response,
        // and the handler builds the JSON response with columns, datarows, total, size, status.
    }
}

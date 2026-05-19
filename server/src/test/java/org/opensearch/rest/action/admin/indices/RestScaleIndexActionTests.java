/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class RestScaleIndexActionTests extends OpenSearchTestCase {

    private RestScaleIndexAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestScaleIndexAction();
    }

    public void testMissingIndexParam() {
        // Build a fake request with no "index" parameter
        Map<String, String> params = new HashMap<>();
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("index is required"));
    }

    public void testEmptyIndexParam() {
        // Build a fake request with an empty "index" parameter
        Map<String, String> params = new HashMap<>();
        params.put("index", "   ");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("index is required"));
    }

    public void testUnknownParameterInBody() {
        String json = "{\"unknown\": \"value\"}";
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .withContent(new BytesArray(json), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Unknown parameter [unknown]. Only [search_only] is allowed."));
    }

    public void testEmptyBody() {
        String json = "{}";
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .withContent(new BytesArray(json), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Parameter [search_only] is required"));
    }

    public void testInvalidSearchOnlyType() {
        String json = "{\"search_only\": \"not_a_boolean\"}";
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .withContent(new BytesArray(json), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Parameter [search_only] must be a boolean (true or false)"));
    }

    public void testValidRequestWithSearchOnlyTrue() throws Exception {
        String json = "{\"search_only\": true}";
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .withContent(new BytesArray(json), XContentType.JSON)
            .build();
        Object consumer = action.prepareRequest(restRequest, mock(NodeClient.class));
        assertThat(consumer, notNullValue());
    }

    public void testValidRequestWithSearchOnlyFalse() throws Exception {
        String json = "{\"search_only\": false}";
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .withContent(new BytesArray(json), XContentType.JSON)
            .build();
        Object consumer = action.prepareRequest(restRequest, mock(NodeClient.class));
        assertThat(consumer, notNullValue());
    }

    public void testInvalidJson() {
        String json = "{\"search_only\": fa}"; // Invalid JSON
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .withContent(new BytesArray(json), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Request body must be valid JSON"));
    }

    public void testParseScaleDownValueMultipleFields() {
        String json = "{\"search_only\": true, \"unknown_field\": \"value\"}";
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .withContent(new BytesArray(json), XContentType.JSON)
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Unknown parameter [unknown_field]. Only [search_only] is allowed."));
    }
}

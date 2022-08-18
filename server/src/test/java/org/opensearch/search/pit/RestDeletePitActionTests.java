/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pit;

import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestDeletePitAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

/**
 * Tests to verify the behavior of rest delete pit action for list delete and delete all PIT endpoints
 */
public class RestDeletePitActionTests extends OpenSearchTestCase {
    public void testParseDeletePitRequestWithInvalidJsonThrowsException() throws Exception {
        RestDeletePitAction action = new RestDeletePitAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(e.getMessage(), equalTo("Failed to parse request body"));
    }

    public void testDeletePitWithBody() throws Exception {
        RestDeletePitAction action = new RestDeletePitAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{\"pit_id\": [\"BODY\"]}"),
            XContentType.JSON
        ).build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        DeletePitRequest deletePITRequest = new DeletePitRequest();
        deletePITRequest.fromXContent(request.contentParser());
        action.prepareRequest(request, mock(NodeClient.class));
        assertEquals("BODY", deletePITRequest.getPitIds().get(0));
    }

    public void testDeleteAllPit() throws Exception {
        RestDeletePitAction action = new RestDeletePitAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_all").build();
        action.prepareRequest(request, mock(NodeClient.class));
        assertEquals("/_all", request.path());
        assertEquals(0, request.params().size());
    }

    public void testDeleteAllPitWithBody() {
        RestDeletePitAction action = new RestDeletePitAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{\"pit_id\": [\"BODY\"]}"),
            XContentType.JSON
        ).withPath("/_all").build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> action.handleRequest(request, channel, mock(NodeClient.class))
        );
        assertTrue(ex.getMessage().contains("request [GET /_all] does not support having a body"));
    }

    public void testDeletePitQueryStringParamsShouldThrowException() {
        RestDeletePitAction action = new RestDeletePitAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(
            Collections.singletonMap("pit_id", "QUERY_STRING,QUERY_STRING_1")
        ).build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> action.handleRequest(request, channel, mock(NodeClient.class))
        );
        assertTrue(ex.getMessage().contains("unrecognized param"));
    }
}

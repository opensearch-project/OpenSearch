/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pit;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.SetOnce;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestDeletePitAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

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
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse request body"));
    }

    public void testDeletePitWithBody() throws Exception {
        SetOnce<Boolean> pitCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void deletePits(DeletePitRequest request, ActionListener<DeletePitResponse> listener) {
                pitCalled.set(true);
                assertThat(request.getPitIds(), hasSize(1));
                assertThat(request.getPitIds().get(0), equalTo("BODY"));
            }
        }) {
            RestDeletePitAction action = new RestDeletePitAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
                new BytesArray("{\"pit_id\": [\"BODY\"]}"),
                XContentType.JSON
            ).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(pitCalled.get(), equalTo(true));
        }
    }

    public void testDeleteAllPit() throws Exception {
        SetOnce<Boolean> pitCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void deletePits(DeletePitRequest request, ActionListener<DeletePitResponse> listener) {
                pitCalled.set(true);
                assertThat(request.getPitIds(), hasSize(1));
                assertThat(request.getPitIds().get(0), equalTo("_all"));
            }
        }) {
            RestDeletePitAction action = new RestDeletePitAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_all").build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(pitCalled.get(), equalTo(true));
        }
    }

    public void testDeleteAllPitWithBody() {
        SetOnce<Boolean> pitCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void deletePits(DeletePitRequest request, ActionListener<DeletePitResponse> listener) {
                pitCalled.set(true);
                assertThat(request.getPitIds(), hasSize(1));
                assertThat(request.getPitIds().get(0), equalTo("_all"));
            }
        }) {
            RestDeletePitAction action = new RestDeletePitAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
                new BytesArray("{\"pit_id\": [\"BODY\"]}"),
                XContentType.JSON
            ).withPath("/_all").build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);

            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> action.handleRequest(request, channel, nodeClient)
            );
            assertTrue(ex.getMessage().contains("request [GET /_all] does not support having a body"));
        }
    }

    public void testDeletePitQueryStringParamsShouldThrowException() {
        SetOnce<Boolean> pitCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void deletePits(DeletePitRequest request, ActionListener<DeletePitResponse> listener) {
                pitCalled.set(true);
                assertThat(request.getPitIds(), hasSize(2));
                assertThat(request.getPitIds().get(0), equalTo("QUERY_STRING"));
                assertThat(request.getPitIds().get(1), equalTo("QUERY_STRING_1"));
            }
        }) {
            RestDeletePitAction action = new RestDeletePitAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(
                Collections.singletonMap("pit_id", "QUERY_STRING,QUERY_STRING_1")
            ).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> action.handleRequest(request, channel, nodeClient)
            );
            assertTrue(ex.getMessage().contains("unrecognized param"));
        }
    }
}

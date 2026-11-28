/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pit;

import org.opensearch.action.search.PitUsageRequest;
import org.opensearch.action.search.PitUsageResponse;
import org.opensearch.common.SetOnce;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestPitUsageAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests to verify the behavior of rest pit usage action for list and all PIT endpoints
 */
public class RestPitUsageActionTests extends OpenSearchTestCase {
    public void testParsePitUsageRequestWithInvalidJsonThrowsException() throws Exception {
        RestPitUsageAction action = new RestPitUsageAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            MediaTypeRegistry.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse request body"));
    }

    public void testPitUsageWithBody() throws Exception {
        SetOnce<Boolean> pitCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void pitUsage(PitUsageRequest request, ActionListener<PitUsageResponse> listener) {
                pitCalled.set(true);
                assertThat(request.getPitIds(), contains("BODY", "BODY_1"));
            }
        }) {
            RestPitUsageAction action = new RestPitUsageAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
                new BytesArray("{\"pit_id\": [\"BODY\", \"BODY_1\"]}"),
                MediaTypeRegistry.JSON
            ).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);
            assertThat(pitCalled.get(), equalTo(true));
        }
    }

    public void testPitUsageWithoutBodyFailsValidation() throws Exception {
        SetOnce<Boolean> pitCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void pitUsage(PitUsageRequest request, ActionListener<PitUsageResponse> listener) {
                pitCalled.set(true);
                assertThat(request.getPitIds(), empty());
                assertThat(request.validate().validationErrors(), contains("no pit ids specified"));
            }
        }) {
            RestPitUsageAction action = new RestPitUsageAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_search/point_in_time/_usage").build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);
            assertThat(pitCalled.get(), equalTo(true));
        }
    }

    public void testPitUsageAll() throws Exception {
        SetOnce<Boolean> pitCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void pitUsage(PitUsageRequest request, ActionListener<PitUsageResponse> listener) {
                pitCalled.set(true);
                assertThat(request.getPitIds(), contains("_all"));
            }
        }) {
            RestPitUsageAction action = new RestPitUsageAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_search/point_in_time/_all/_usage").build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);
            assertThat(pitCalled.get(), equalTo(true));
        }
    }
}

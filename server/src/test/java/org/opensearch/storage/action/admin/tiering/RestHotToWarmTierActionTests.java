/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering;

import org.opensearch.rest.RestRequest;
import org.opensearch.storage.action.tiering.RestHotToWarmTierAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class RestHotToWarmTierActionTests extends OpenSearchTestCase {
    private RestHotToWarmTierAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestHotToWarmTierAction();
    }

    public void testInvalidInput() {
        // Test empty index
        final RestRequest requestWithEmptyIndex = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_tier/warm")
            .withParams(Map.of("index", ""))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(requestWithEmptyIndex, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Index parameter is required"));

        // Test blocklisted index
        final RestRequest requestWithBlocklistedIndex = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_tier/warm")
            .withParams(Map.of("index", ".kibana"))
            .build();
        e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(requestWithBlocklistedIndex, mock(NodeClient.class)));

        assertThat(e.getMessage(), containsString("Index is blocklisted for migrations"));

        // Test multiple indices
        final RestRequest requestWithMultipleIndices = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_tier/warm")
            .withParams(Map.of("index", "foo,bar"))  // explicitly set multiple indices
            .build();
        e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(requestWithMultipleIndices, mock(NodeClient.class)));
        assertThat(e.getMessage(), containsString("hot to warm tiering request should contain exactly one index"));
    }

    public void testValidInput() {
        // Test valid single index
        final RestRequest validRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath("/test-index/_tier/warm")
            .withParams(Map.of("index", "test-index"))  // explicitly set valid index
            .build();

        // This should not throw an exception
        action.prepareRequest(validRequest, mock(NodeClient.class));
    }

    public void testGetName() {
        assertEquals("warm_tier_action", action.getName());
    }

    public void testRoutes() {
        assertEquals(1, action.routes().size());
        assertEquals("/{index}/_tier/warm", action.routes().get(0).getPath());
        assertEquals(RestRequest.Method.POST, action.routes().get(0).getMethod());
    }
}

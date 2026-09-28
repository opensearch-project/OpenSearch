/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering;

import org.opensearch.rest.RestRequest;
import org.opensearch.storage.action.tiering.RestCancelTierAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for RestCancelTierAction.
 * Tests REST endpoint validation, parameter handling, and error conditions.
 */
public class RestCancelTierActionTests extends OpenSearchTestCase {
    private RestCancelTierAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestCancelTierAction();
    }

    public void testGetName() {
        assertEquals("cancel_tier_action", action.getName());
    }

    public void testRoutes() {
        assertEquals(1, action.routes().size());
        assertEquals("/_tier/_cancel/{index}", action.routes().get(0).getPath());
        assertEquals(RestRequest.Method.POST, action.routes().get(0).getMethod());
    }

    public void testInvalidInputEmptyIndex() {
        final RestRequest requestWithEmptyIndex = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_tier/_cancel/")
            .withParams(Map.of("index", ""))
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(requestWithEmptyIndex, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Index parameter is required"));
    }

    public void testInvalidInputNullIndex() {
        final RestRequest requestWithNullIndex = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_tier/_cancel/").build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(requestWithNullIndex, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Index parameter is required"));
    }

    public void testInvalidInputBlocklistedIndex() {
        final RestRequest requestWithBlocklistedIndex = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_tier/_cancel/.kibana")
            .withParams(Map.of("index", ".kibana"))
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(requestWithBlocklistedIndex, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Index is blocklisted for migrations"));
    }

    public void testInvalidInputMultipleIndices() {
        final RestRequest requestWithMultipleIndices = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_tier/_cancel/foo,bar")
            .withParams(Map.of("index", "foo,bar"))
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(requestWithMultipleIndices, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Cancel tiering request should contain exactly one index"));
    }

    public void testValidSingleIndex() {
        final RestRequest validRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_tier/_cancel/test-index")
            .withParams(Map.of("index", "test-index"))
            .build();

        // This should not throw an exception
        assertNotNull(action.prepareRequest(validRequest, mock(NodeClient.class)));
    }

    public void testValidIndexWithTimeoutParameters() {
        final RestRequest requestWithTimeouts = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_tier/_cancel/test-index")
            .withParams(Map.of("index", "test-index", "timeout", "30s", "cluster_manager_timeout", "60s"))
            .build();

        // This should not throw an exception and should handle timeout parameters
        assertNotNull(action.prepareRequest(requestWithTimeouts, mock(NodeClient.class)));
    }

    public void testValidDataStreamBackingIndex() {
        // Test that data stream backing indices (which start with .ds-) are allowed
        final RestRequest validDataStreamRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath(
            "/_tier/_cancel/.ds-logs-2023.01.01-000001"
        ).withParams(Map.of("index", ".ds-logs-2023.01.01-000001")).build();

        // This should not throw an exception as data stream indices are allowlisted
        assertNotNull(action.prepareRequest(validDataStreamRequest, mock(NodeClient.class)));
    }

    public void testInvalidBlankIndexName() {
        final RestRequest requestWithBlankIndex = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_tier/_cancel/ /")
            .withParams(Map.of("index", " "))
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(requestWithBlankIndex, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("Index name cannot be null or empty"));
    }

    public void testValidIndexWithSpecialCharacters() {
        // Test index names with valid special characters
        final RestRequest validSpecialCharRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath(
            "/_tier/_cancel/test-index_123"
        ).withParams(Map.of("index", "test-index_123")).build();

        assertNotNull(action.prepareRequest(validSpecialCharRequest, mock(NodeClient.class)));
    }
}

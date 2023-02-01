/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission;

import org.apache.lucene.util.SetOnce;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.collect.Map;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.rest.action.permission.check.RestCheckPermissionAction;
import org.opensearch.identity.rest.action.permission.add.RestAddPermissionAction;
import org.opensearch.identity.rest.action.permission.delete.RestDeletePermissionAction;
import org.opensearch.identity.rest.action.permission.add.AddPermissionRequest;
import org.opensearch.identity.rest.action.permission.delete.DeletePermissionRequest;
import org.opensearch.identity.rest.action.permission.check.CheckPermissionRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests to verify the behavior of rest create user action
 */
public class RestPermissionTests extends OpenSearchTestCase {
    public void testParseAddPermissionRequestWithInvalidJsonThrowsException() {
        RestAddPermissionAction action = new RestAddPermissionAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse add permission request body"));
    }

    public void testParseAddPermissionWithValidJson() throws Exception {
        SetOnce<Boolean> addPermissionCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                AddPermissionRequest req = (AddPermissionRequest) request;
                addPermissionCalled.set(true);
                assertThat(req.getPermissionString(), equalTo("test"));
            }
        }) {
            RestAddPermissionAction action = new RestAddPermissionAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("permissionString", "test")).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(addPermissionCalled.get(), equalTo(true));
        }
    }

    public void testParseCheckPermissionRequestWithInvalidJsonThrowsException() {
        RestCheckPermissionAction action = new RestCheckPermissionAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse check permission request body"));
    }

    public void testParseCheckPermissionWithValidJson() throws Exception {
        SetOnce<Boolean> checkPermissionCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                CheckPermissionRequest req = (CheckPermissionRequest) request;
                checkPermissionCalled.set(true);
                assertThat(req.getPrincipalString(), equalTo("test"));
            }
        }) {
            RestAddPermissionAction action = new RestAddPermissionAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("permissionString", "test")).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(checkPermissionCalled.get(), equalTo(true));
        }
    }

    public void testParseDeletePermissionRequestWithInvalidJsonThrowsException() {
        RestDeletePermissionAction action = new RestDeletePermissionAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse delete permission request body"));
    }

    public void testParseDeletePermissionWithValidJson() throws Exception {
        SetOnce<Boolean> deletePermissionCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                DeletePermissionRequest req = (DeletePermissionRequest) request;
                deletePermissionCalled.set(true);
                assertThat(req.getPermissionString(), equalTo("test"));
            }
        }) {
            RestDeletePermissionAction action = new RestDeletePermissionAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("permissionString", "test")).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(deletePermissionCalled.get(), equalTo(true));
        }
    }
}

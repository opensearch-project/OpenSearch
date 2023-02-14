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
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.rest.permission.delete.DeletePermissionRequest;
import org.opensearch.identity.rest.permission.delete.RestDeletePermissionAction;
import org.opensearch.identity.rest.permission.get.GetPermissionRequest;
import org.opensearch.identity.rest.permission.get.RestGetPermissionAction;
import org.opensearch.identity.rest.permission.put.RestPutPermissionAction;
import org.opensearch.identity.rest.permission.put.PutPermissionRequest;
import org.opensearch.identity.utils.ErrorType;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests to verify the behavior of rest create user action
 */
public class RestPermissionTests extends OpenSearchTestCase {
    /**
     * Tests to verify the behavior of rest create user action
     */
    public void testParsePutPermissionRequestWithInvalidJsonThrowsException() {
        RestPutPermissionAction action = new RestPutPermissionAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo(ErrorType.BODY_NOT_PARSEABLE.getMessage() + "PUT Permission"));
    }

    public void testPutPermissionWithValidJson() throws Exception {
        SetOnce<Boolean> putPermissionCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                PutPermissionRequest req = (PutPermissionRequest) request;
                putPermissionCalled.set(true);
                assertThat(req.getPermission(), equalTo("test_permission"));
                assertThat(req.getUsername(), equalTo("test"));
            }
        }) {
            RestPutPermissionAction action = new RestPutPermissionAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("username", "test"))
                .withContent(new BytesArray("{ \"permission\" : \"test_permission\" }\n"), XContentType.JSON)
                .build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(putPermissionCalled.get(), equalTo(true));
        }
    }

    public void testGetPermissionWithValidJson() throws Exception {
        SetOnce<Boolean> getPermissionCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                GetPermissionRequest req = (GetPermissionRequest) request;
                getPermissionCalled.set(true);
                assertThat(req.getUsername(), equalTo("test"));
            }
        }) {
            RestGetPermissionAction action = new RestGetPermissionAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("username", "test")).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);
            assertThat(getPermissionCalled.get(), equalTo(true));
        }
    }

    public void testParseDeletePermissionRequestWithInvalidJsonThrowsException() {
        RestDeletePermissionAction action = new RestDeletePermissionAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo(ErrorType.BODY_NOT_PARSEABLE.getMessage() + "DELETE Permission"));
    }

    public void testDeletePermissionWithValidJson() throws Exception {
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
                assertThat(req.getPermissionString(), equalTo("test_permission"));
                assertThat(req.getUsername(), equalTo("test"));
            }
        }) {
            RestDeletePermissionAction action = new RestDeletePermissionAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("username", "test"))
                .withContent(new BytesArray("{ \"permissionString\" : \"test_permission\" }\n"), XContentType.JSON)
                .build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(deletePermissionCalled.get(), equalTo(true));
        }
    }

}

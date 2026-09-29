/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.indices.datastream.DataStreamAction;
import org.opensearch.action.admin.indices.datastream.ModifyDataStreamsAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RestModifyDataStreamsActionTests extends RestActionTestCase {

    private RestModifyDataStreamsAction action;

    @Before
    public void setupAction() {
        action = new RestModifyDataStreamsAction();
        controller().registerHandler(action);
    }

    public void testGetName() {
        assertThat(action.getName(), equalTo("modify_data_stream_action"));
    }

    public void testRoutes() {
        List<RestHandler.Route> routes = action.routes();
        assertThat(routes, hasSize(1));
        RestHandler.Route route = routes.get(0);
        assertThat(route.getMethod(), equalTo(RestRequest.Method.POST));
        assertThat(route.getPath(), equalTo("/_data_stream/_modify"));
    }

    public void testAddBackingIndex() throws Exception {
        String body = "{\"actions\":[{\"add_backing_index\":{\"data_stream\":\"logs-foo\",\"index\":\".ds-logs-foo-000001\"}}]}";
        ModifyDataStreamsAction.Request request = executeAndCapture(buildRestRequest(body, emptyParams()));

        assertThat(request.getActions(), hasSize(1));
        DataStreamAction dsAction = request.getActions().get(0);
        assertThat(dsAction.type(), equalTo(DataStreamAction.Type.ADD_BACKING_INDEX));
        assertThat(dsAction.dataStream(), equalTo("logs-foo"));
        assertThat(dsAction.index(), equalTo(".ds-logs-foo-000001"));
        assertNull(request.validate());
    }

    public void testRemoveBackingIndex() throws Exception {
        String body = "{\"actions\":[{\"remove_backing_index\":{\"data_stream\":\"logs-bar\",\"index\":\".ds-logs-bar-000003\"}}]}";
        ModifyDataStreamsAction.Request request = executeAndCapture(buildRestRequest(body, emptyParams()));

        assertThat(request.getActions(), hasSize(1));
        DataStreamAction dsAction = request.getActions().get(0);
        assertThat(dsAction.type(), equalTo(DataStreamAction.Type.REMOVE_BACKING_INDEX));
        assertThat(dsAction.dataStream(), equalTo("logs-bar"));
        assertThat(dsAction.index(), equalTo(".ds-logs-bar-000003"));
    }

    public void testMultipleActionsInOneBody() throws Exception {
        String body = "{\"actions\":["
            + "{\"remove_backing_index\":{\"data_stream\":\"logs-foo\",\"index\":\".ds-logs-foo-000001\"}},"
            + "{\"add_backing_index\":{\"data_stream\":\"logs-bar\",\"index\":\".ds-logs-foo-000001\"}},"
            + "{\"add_backing_index\":{\"data_stream\":\"logs-bar\",\"index\":\"standalone-index\"}}"
            + "]}";
        ModifyDataStreamsAction.Request request = executeAndCapture(buildRestRequest(body, emptyParams()));

        assertThat(request.getActions(), hasSize(3));
        assertThat(request.getActions().get(0), equalTo(DataStreamAction.removeBackingIndex("logs-foo", ".ds-logs-foo-000001")));
        assertThat(request.getActions().get(1), equalTo(DataStreamAction.addBackingIndex("logs-bar", ".ds-logs-foo-000001")));
        assertThat(request.getActions().get(2), equalTo(DataStreamAction.addBackingIndex("logs-bar", "standalone-index")));
    }

    public void testEmptyActionsArrayProducesEmptyRequestThatFailsValidation() throws Exception {
        ModifyDataStreamsAction.Request request = executeAndCapture(buildRestRequest("{\"actions\":[]}", emptyParams()));

        assertThat(request.getActions(), hasSize(0));
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString("at least one data stream action must be specified"));
    }

    public void testEmptyObjectBodyProducesEmptyRequest() throws Exception {
        ModifyDataStreamsAction.Request request = executeAndCapture(buildRestRequest("{}", emptyParams()));

        assertThat(request.getActions(), hasSize(0));
        assertNotNull(request.validate());
    }

    public void testDefaultTimeoutsWhenNoParams() throws Exception {
        String body = "{\"actions\":[{\"add_backing_index\":{\"data_stream\":\"logs-foo\",\"index\":\"i1\"}}]}";
        ModifyDataStreamsAction.Request request = executeAndCapture(buildRestRequest(body, emptyParams()));

        ModifyDataStreamsAction.Request untouched = new ModifyDataStreamsAction.Request(
            List.of(DataStreamAction.addBackingIndex("logs-foo", "i1"))
        );
        assertThat(request.clusterManagerNodeTimeout(), equalTo(untouched.clusterManagerNodeTimeout()));
        assertThat(request.timeout(), equalTo(untouched.timeout()));
    }

    public void testTimeoutsReadFromParams() throws Exception {
        String body = "{\"actions\":[{\"add_backing_index\":{\"data_stream\":\"logs-foo\",\"index\":\"i1\"}}]}";
        Map<String, String> params = new HashMap<>();
        params.put("cluster_manager_timeout", "45s");
        params.put("timeout", "17s");
        ModifyDataStreamsAction.Request request = executeAndCapture(buildRestRequest(body, params));

        assertThat(request.clusterManagerNodeTimeout().millis(), equalTo(45_000L));
        assertThat(request.timeout().millis(), equalTo(17_000L));
    }

    public void testMissingBodyIsRejected() {
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_data_stream/_modify")
            .build();

        OpenSearchParseException e = expectThrows(
            OpenSearchParseException.class,
            () -> action.prepareRequest(restRequest, verifyingClient)
        );
        assertThat(e.getMessage(), containsString("request body is required"));
    }

    public void testBodyThatIsNotAnObjectIsRejected() {
        RestRequest restRequest = buildRestRequest("[]", emptyParams());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, verifyingClient)
        );
        assertThat(e.getMessage(), equalTo("expected an object with an [actions] array"));
    }

    public void testUnexpectedTopLevelFieldIsRejected() {
        RestRequest restRequest = buildRestRequest("{\"not_actions\":\"whatever\"}", emptyParams());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, verifyingClient)
        );
        assertThat(e.getMessage(), equalTo("unexpected field [not_actions], only [actions] is supported"));
    }

    public void testActionsFieldThatIsNotAnArrayIsRejected() {
        RestRequest restRequest = buildRestRequest("{\"actions\":\"not-an-array\"}", emptyParams());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, verifyingClient)
        );
        assertThat(e.getMessage(), equalTo("unexpected field [actions], only [actions] is supported"));
    }

    public void testUnknownActionTypeIsRejected() {
        RestRequest restRequest = buildRestRequest(
            "{\"actions\":[{\"rename_backing_index\":{\"data_stream\":\"logs-foo\",\"index\":\"i1\"}}]}",
            emptyParams()
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(restRequest, verifyingClient)
        );
        assertThat(e.getMessage(), containsString("rename_backing_index"));
    }

    public void testMalformedJsonIsRejected() {
        RestRequest restRequest = buildRestRequest("{\"actions\":[", emptyParams());

        expectThrows(Exception.class, () -> action.prepareRequest(restRequest, verifyingClient));
    }

    /**
     * Runs {@link RestModifyDataStreamsAction#prepareRequest} and the returned consumer against the verifying client,
     * returning the {@link ModifyDataStreamsAction.Request} that was handed to the client.
     */
    private ModifyDataStreamsAction.Request executeAndCapture(RestRequest restRequest) throws Exception {
        AtomicReference<ModifyDataStreamsAction.Request> captured = new AtomicReference<>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(actionType, equalTo(ModifyDataStreamsAction.INSTANCE));
            captured.set((ModifyDataStreamsAction.Request) request);
            return new AcknowledgedResponse(true);
        });

        CheckedConsumer<RestChannel, Exception> consumer = action.prepareRequest(restRequest, verifyingClient);
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 1);
        consumer.accept(channel);

        assertThat(channel.responses().get(), equalTo(1));
        assertThat(channel.errors().get(), equalTo(0));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        assertNotNull(captured.get());
        return captured.get();
    }

    private Map<String, String> emptyParams() {
        return new HashMap<>();
    }

    private RestRequest buildRestRequest(String content, Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_data_stream/_modify")
            .withParams(params)
            .withContent(new BytesArray(content), MediaTypeRegistry.JSON)
            .build();
    }
}

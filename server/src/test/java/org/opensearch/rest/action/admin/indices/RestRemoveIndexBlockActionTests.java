/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.readonly.RemoveIndexBlockAction;
import org.opensearch.action.admin.indices.readonly.RemoveIndexBlockRequest;
import org.opensearch.action.admin.indices.readonly.RemoveIndexBlockResponse;
import org.opensearch.action.admin.indices.readonly.RemoveIndexBlockResponse.RemoveBlockResult;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class RestRemoveIndexBlockActionTests extends RestActionTestCase {

    @BeforeClass
    public static void initializeIndexMetadataFirst() {
        // IndexMetadata and its nested APIBlock enum initialize each other and only work when IndexMetadata is initialized first, as it
        // always is on a running node. Do not let the first test that mentions APIBlock in this JVM decide.
        assertNotNull(IndexMetadata.INDEX_READ_ONLY_BLOCK);
    }

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestRemoveIndexBlockAction());
    }

    private static FakeRestRequest request(String path, Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath(path)
            .withMethod(RestRequest.Method.DELETE)
            .withParams(new HashMap<>(params))
            .build();
    }

    private FakeRestChannel dispatch(FakeRestRequest request) {
        final FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        controller().dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));
        return channel;
    }

    public void testRouteAndName() {
        final RestRemoveIndexBlockAction action = new RestRemoveIndexBlockAction();
        final List<Route> routes = action.routes();
        assertThat(routes, hasSize(1));
        assertThat(routes.get(0).getMethod(), equalTo(RestRequest.Method.DELETE));
        assertThat(routes.get(0).getPath(), equalTo("/{index}/_block/{block}"));
        assertThat(action.getName(), equalTo("remove_index_block_action"));
    }

    public void testRequestParametersAreParsed() {
        final AtomicReference<RemoveIndexBlockRequest> captured = new AtomicReference<>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(actionType, sameInstance(RemoveIndexBlockAction.INSTANCE));
            captured.set((RemoveIndexBlockRequest) request);
            return new RemoveIndexBlockResponse(true, List.of(new RemoveBlockResult(new Index("logs-1", "uuid-1"))));
        });
        final Map<String, String> params = new HashMap<>();
        params.put("timeout", "5s");
        params.put("cluster_manager_timeout", "6s");
        params.put("ignore_unavailable", "true");
        params.put("allow_no_indices", "false");
        params.put("expand_wildcards", "open,closed");

        final FakeRestChannel channel = dispatch(request("/logs-1,logs-2/_block/write", params));

        final RemoveIndexBlockRequest request = captured.get();
        assertNotNull(request);
        assertThat(request.indices(), arrayContaining("logs-1", "logs-2"));
        assertThat(request.getBlock(), equalTo(APIBlock.WRITE));
        assertThat(request.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        assertThat(request.clusterManagerNodeTimeout(), equalTo(TimeValue.timeValueSeconds(6)));
        final IndicesOptions options = request.indicesOptions();
        assertTrue(options.ignoreUnavailable());
        assertFalse(options.allowNoIndices());
        assertTrue(options.expandWildcardsOpen());
        assertTrue(options.expandWildcardsClosed());
        assertFalse(options.expandWildcardsHidden());

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        assertThat(
            channel.capturedResponse().content().utf8ToString(),
            equalTo("{\"acknowledged\":true,\"indices\":[{\"name\":\"logs-1\",\"unblocked\":true}]}")
        );
    }

    public void testDefaultsWhenNoOptionalParametersAreGiven() {
        final AtomicReference<RemoveIndexBlockRequest> captured = new AtomicReference<>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            captured.set((RemoveIndexBlockRequest) request);
            return new RemoveIndexBlockResponse(true, List.of());
        });

        dispatch(request("/logs/_block/read_only", Map.of()));

        final RemoveIndexBlockRequest request = captured.get();
        final RemoveIndexBlockRequest expectedDefaults = new RemoveIndexBlockRequest(APIBlock.READ_ONLY, "logs");
        assertThat(request.indices(), arrayContaining("logs"));
        assertThat(request.getBlock(), equalTo(APIBlock.READ_ONLY));
        assertThat(request.indicesOptions(), equalTo(IndicesOptions.strictExpandOpen()));
        assertThat(request.timeout(), equalTo(expectedDefaults.timeout()));
        assertThat(request.clusterManagerNodeTimeout(), equalTo(expectedDefaults.clusterManagerNodeTimeout()));
    }

    public void testEveryBlockNameThePutApiAcceptsIsParsed() throws Exception {
        final AtomicReference<RemoveIndexBlockRequest> captured = new AtomicReference<>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            captured.set((RemoveIndexBlockRequest) request);
            return new RemoveIndexBlockResponse(true, List.of());
        });
        final RestRemoveIndexBlockAction action = new RestRemoveIndexBlockAction();
        for (String block : List.of("read_only", "read", "write", "metadata", "read_only_allow_delete", "search_only")) {
            captured.set(null);
            final FakeRestRequest request = request("/logs/_block/" + block, Map.of("index", "logs", "block", block));

            action.handleRequest(request, new FakeRestChannel(request, true, 1), verifyingClient);

            assertNotNull(block, captured.get());
            // the name is resolved exactly as the add block API does; invalid combinations are rejected by request validation
            assertThat(captured.get().getBlock(), equalTo(APIBlock.fromName(block)));
        }
    }

    public void testUnknownBlockNameIsABadRequest() {
        final FakeRestChannel channel = dispatch(request("/logs/_block/not_a_block", Map.of()));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("No block found with name not_a_block"));
    }

    public void testDeprecatedMasterTimeoutIsNotAcceptedByANewApi() {
        final FakeRestChannel channel = dispatch(request("/logs/_block/write", Map.of("master_timeout", "5s")));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("unrecognized parameter: [master_timeout]"));
    }

    public void testInvalidTimeoutIsABadRequest() {
        final FakeRestChannel channel = dispatch(request("/logs/_block/write", Map.of("timeout", "soon")));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class RestClusterStateActionTests extends RestActionTestCase {

    public void testRejectsPersistenceContextParameters() {
        RestClusterStateAction handler = new RestClusterStateAction(new SettingsFilter(List.of()));
        for (String context : List.of(Metadata.CONTEXT_MODE_GATEWAY, Metadata.CONTEXT_MODE_SNAPSHOT)) {
            RestRequest request = requestWithContext(context);
            FakeRestChannel channel = new FakeRestChannel(request, false, 1);

            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> handler.handleRequest(request, channel, verifyingClient)
            );

            assertThat(exception.getMessage(), containsString("unrecognized parameter: [" + Metadata.CONTEXT_MODE_PARAM + "]"));
            assertNull(channel.capturedResponse());
        }
    }

    public void testResponseRenderingForcesApiContext() throws Exception {
        Map<String, String> privateData = Map.of("private-field", "private-value");
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(IndexFieldDomainMetadata.CUSTOM_KEY, privateData)
            .putCustom("public_custom", Map.of("key", "value"))
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexMetadata, false)).build();
        verifyingClient.setExecuteVerifier((action, request) -> {
            assertSame(ClusterStateAction.INSTANCE, action);
            // Supply unfiltered state so transport filtering cannot mask a rendering regression.
            return new ClusterStateResponse(state.getClusterName(), state, false);
        });
        RestClusterStateAction handler = new RestClusterStateAction(new SettingsFilter(List.of()));

        for (String context : List.of(Metadata.CONTEXT_MODE_GATEWAY, Metadata.CONTEXT_MODE_SNAPSHOT)) {
            RestRequest request = requestWithContext(context);
            FakeRestChannel channel = new FakeRestChannel(request, false, 1);
            // Exercise rendering independently of BaseRestHandler's unknown-parameter rejection.
            CheckedConsumer<RestChannel, Exception> responseConsumer = handler.prepareRequest(request, verifyingClient);
            responseConsumer.accept(channel);

            RestResponse response = channel.capturedResponse();
            assertNotNull(response);
            assertEquals(RestStatus.OK, response.status());
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, response.content())) {
                Map<?, ?> metadata = (Map<?, ?>) parser.map().get("metadata");
                assertNotNull(metadata);
                Map<?, ?> indices = (Map<?, ?>) metadata.get("indices");
                assertNotNull(indices);
                Map<?, ?> index = (Map<?, ?>) indices.get("test");
                assertNotNull(index);
                assertFalse(index.containsKey(IndexFieldDomainMetadata.CUSTOM_KEY));
                assertEquals(Map.of("key", "value"), index.get("public_custom"));
            }
            assertEquals(privateData, state.metadata().index("test").getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
        }
    }

    private RestRequest requestWithContext(String context) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_cluster/state/metadata")
            .withParams(new HashMap<>(Map.of("metric", "metadata", Metadata.CONTEXT_MODE_PARAM, context)))
            .build();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.settings.describe.IndicesDescribeSettingsAction;
import org.opensearch.action.admin.indices.settings.describe.IndicesDescribeSettingsRequest;
import org.opensearch.action.admin.indices.settings.describe.IndicesDescribeSettingsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class RestIndicesDescribeSettingsActionTests extends RestActionTestCase {
    @Before
    public void setUpAction() {
        controller().registerHandler(new RestIndicesDescribeSettingsAction());
    }

    public void testSettingsParameterIsForwarded() {
        var executed = new AtomicReference<IndicesDescribeSettingsRequest>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertSame(IndicesDescribeSettingsAction.INSTANCE, actionType);
            executed.set((IndicesDescribeSettingsRequest) request);
            return new IndicesDescribeSettingsResponse(Map.of());
        });
        var request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_settings/_describe")
            .withParams(new HashMap<>(Map.of("settings", "index.number_of_shards,index.routing.allocation.*")))
            .build();
        var channel = new FakeRestChannel(request, true, 1);
        controller().dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));
        assertEquals(1, channel.responses().get());
        assertEquals(0, channel.errors().get());
        assertArrayEquals(new String[] { "index.number_of_shards", "index.routing.allocation.*" }, executed.get().names());
    }

    public void testGetSettingsParametersAreRejected() {
        // GET /_settings/{name} accepts include_defaults and friends; the literal /_settings/_describe route does not, so a
        // client that used to reach get-settings through this path with those parameters now gets the standard 400.
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            fail("request with an unrecognized parameter must not reach the transport action");
            return null;
        });
        var request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_settings/_describe")
            .withParams(new HashMap<>(Map.of("settings", "index.number_of_shards", "include_defaults", "true")))
            .build();
        var channel = new FakeRestChannel(request, true, 1);
        controller().dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));
        assertEquals(1, channel.errors().get());
        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
        assertTrue(
            channel.capturedResponse().content().utf8ToString(),
            channel.capturedResponse().content().utf8ToString().contains("contains unrecognized parameter: [include_defaults]")
        );
    }
}

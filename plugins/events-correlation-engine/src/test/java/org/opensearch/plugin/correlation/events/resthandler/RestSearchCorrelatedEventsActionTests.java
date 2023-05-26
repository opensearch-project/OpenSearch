/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.resthandler;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.SetOnce;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.correlation.utils.TestHelpers;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

public class RestSearchCorrelatedEventsActionTests extends OpenSearchTestCase {

    public void testPrepareRequest() throws Exception {
        SetOnce<Boolean> transportActionCalled = new SetOnce<>();
        try (NodeClient client = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                transportActionCalled.set(true);
                super.doExecute(action, request, listener);
            }
        }) {
            RestSearchCorrelatedEventsAction action = new RestSearchCorrelatedEventsAction();
            Map<String, String> params = new HashMap<>();
            params.put("index", "app_logs");
            params.put("event", "l2UZD4kBSz-dGVL1EZFJ");
            params.put("timestamp_field", "@timestamp");

            RestRequest request = new FakeRestRequest.Builder(TestHelpers.xContentRegistry()).withParams(params).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, client);
            Assert.assertEquals(true, transportActionCalled.get());
        }
    }
}

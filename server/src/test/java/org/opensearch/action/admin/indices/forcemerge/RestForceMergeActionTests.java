/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.admin.indices.RestForceMergeAction;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class RestForceMergeActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestForceMergeAction());
    }

    public void testAllShardsRejectedReturns429() {
        int total = randomIntBetween(1, 8);
        ForceMergeResponse response = new ForceMergeResponse(total, 0, total, rejected(total));

        RestResponse restResponse = forceMerge(response);

        assertEquals(RestStatus.TOO_MANY_REQUESTS, restResponse.status());
        // the body is unchanged: the caller can still see which shards were rejected
        assertThat(restResponse.content().utf8ToString(), containsString("\"failed\":" + total));
        assertThat(restResponse.content().utf8ToString(), containsString("\"failures\""));
    }

    public void testPartialRejectionReturns200() {
        int total = randomIntBetween(2, 8);
        int failed = randomIntBetween(1, total - 1);
        ForceMergeResponse response = new ForceMergeResponse(total, total - failed, failed, rejected(failed));

        RestResponse restResponse = forceMerge(response);

        assertEquals(RestStatus.OK, restResponse.status());
        assertThat(restResponse.content().utf8ToString(), containsString("\"failed\":" + failed));
    }

    public void testNoFailuresReturns200() {
        int total = randomIntBetween(1, 8);
        ForceMergeResponse response = new ForceMergeResponse(total, total, 0, List.of());

        assertEquals(RestStatus.OK, forceMerge(response).status());
    }

    public void testAllShardsFailedForMixedReasonsReturns200() {
        // no shard succeeded, but not every failure is a rejection: the status is unchanged from before admission control
        List<DefaultShardOperationFailedException> failures = new ArrayList<>(rejected(1));
        failures.add(new DefaultShardOperationFailedException("test", 1, new IOException("simulated merge failure")));
        ForceMergeResponse response = new ForceMergeResponse(2, 0, 2, failures);

        assertEquals(RestStatus.OK, forceMerge(response).status());
    }

    public void testAllShardsFailedForNonRejectionReasonReturns200() {
        int total = randomIntBetween(1, 8);
        List<DefaultShardOperationFailedException> failures = new ArrayList<>();
        for (int shard = 0; shard < total; shard++) {
            failures.add(new DefaultShardOperationFailedException("test", shard, new IOException("simulated merge failure")));
        }
        ForceMergeResponse response = new ForceMergeResponse(total, 0, total, failures);

        RestResponse restResponse = forceMerge(response);

        assertEquals(RestStatus.OK, restResponse.status());
        assertThat(restResponse.content().utf8ToString(), containsString("\"failed\":" + total));
    }

    /**
     * Dispatches a synchronous force merge request whose transport response is the given one and returns the REST response.
     */
    private RestResponse forceMerge(ForceMergeResponse response) {
        verifyingClient.setExecuteVerifier((action, request) -> response);
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/test/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        controller().dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));
        assertEquals("exactly one response must be sent", 1, channel.responses().get() + channel.errors().get());
        return channel.capturedResponse();
    }

    private static List<DefaultShardOperationFailedException> rejected(int count) {
        List<DefaultShardOperationFailedException> failures = new ArrayList<>();
        for (int shard = 0; shard < count; shard++) {
            failures.add(
                new DefaultShardOperationFailedException(
                    "test",
                    shard,
                    new OpenSearchRejectedExecutionException("CPU usage admission controller rejected the request")
                )
            );
        }
        return failures;
    }

    public void testDeprecationMessage() {
        final Map<String, String> params = new HashMap<>();
        params.put("only_expunge_deletes", Boolean.TRUE.toString());
        params.put("max_num_segments", Integer.toString(randomIntBetween(0, 10)));
        params.put("flush", Boolean.toString(randomBoolean()));

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        dispatchRequest(request);
        assertWarnings(
            "setting only_expunge_deletes and max_num_segments at the same time is deprecated " + "and will be rejected in a future version"
        );
    }

    public void testUpgradeWithMaxNumSegmentsIsAccepted() {
        // "upgrade" now composes with "max_num_segments" (upgrade segments, then consolidate),
        // so this combination must be accepted rather than rejected.
        final Map<String, String> params = new HashMap<>();
        params.put("upgrade", Boolean.TRUE.toString());
        params.put("max_num_segments", Integer.toString(randomIntBetween(1, 10)));

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        dispatchRequest(request);
    }

    public void testUpgradeWithOnlyExpungeDeletesNotRejectedAtRestLayer() {
        // The upgrade + only_expunge_deletes conflict is now validated in ForceMergeRequest#validate()
        // (covered by ForceMergeRequestTests), not in the REST handler, so that transport/plugin callers
        // are also subject to it. The REST layer must therefore build the request without throwing.
        final Map<String, String> params = new HashMap<>();
        params.put("upgrade", Boolean.TRUE.toString());
        params.put("only_expunge_deletes", Boolean.TRUE.toString());

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        dispatchRequest(request);
    }

    public void testUpgradeAloneIsAccepted() {
        final Map<String, String> params = new HashMap<>();
        params.put("upgrade", Boolean.TRUE.toString());

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        dispatchRequest(request);
    }

    public void testDeprecationWithOnlyExpungeDeletesAndMaxNumSegmentsWhenUpgradeSet() {
        final Map<String, String> params = new HashMap<>();
        params.put("upgrade", Boolean.TRUE.toString());
        params.put("max_num_segments", Integer.toString(randomIntBetween(1, 10)));
        params.put("only_expunge_deletes", Boolean.TRUE.toString());

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        // The REST layer no longer rejects upgrade + only_expunge_deletes (moved to ForceMergeRequest#validate());
        // it still emits the only_expunge_deletes + max_num_segments deprecation warning.
        dispatchRequest(request);
        assertWarnings(
            "setting only_expunge_deletes and max_num_segments at the same time is deprecated and will be rejected in a future version"
        );
    }

    public void testUpgradeFalseWithOtherParamsIsAccepted() {
        final Map<String, String> params = new HashMap<>();
        params.put("upgrade", Boolean.FALSE.toString());
        params.put("max_num_segments", Integer.toString(randomIntBetween(1, 10)));

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        dispatchRequest(request);
    }
}

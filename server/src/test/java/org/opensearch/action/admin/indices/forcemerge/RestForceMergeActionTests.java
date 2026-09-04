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

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.admin.indices.RestForceMergeAction;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class RestForceMergeActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestForceMergeAction());
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

    public void testOnlyUpgradeLuceneWithMaxNumSegmentsRejected() {
        final Map<String, String> params = new HashMap<>();
        params.put("only_upgrade_lucene", Boolean.TRUE.toString());
        params.put("max_num_segments", Integer.toString(randomIntBetween(0, 10)));

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        RestForceMergeAction action = new RestForceMergeAction();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertEquals("cannot set only_upgrade_lucene and max_num_segments at the same time", e.getMessage());
    }

    public void testOnlyUpgradeLuceneWithOnlyExpungeDeletesRejected() {
        final Map<String, String> params = new HashMap<>();
        params.put("only_upgrade_lucene", Boolean.TRUE.toString());
        params.put("only_expunge_deletes", Boolean.TRUE.toString());

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        RestForceMergeAction action = new RestForceMergeAction();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertEquals("cannot set only_upgrade_lucene and only_expunge_deletes at the same time", e.getMessage());
    }

    public void testOnlyUpgradeLuceneAloneIsAccepted() {
        final Map<String, String> params = new HashMap<>();
        params.put("only_upgrade_lucene", Boolean.TRUE.toString());

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        dispatchRequest(request);
    }

    public void testOnlyUpgradeLuceneWithAllConflictingParamsRejected() {
        final Map<String, String> params = new HashMap<>();
        params.put("only_upgrade_lucene", Boolean.TRUE.toString());
        params.put("max_num_segments", Integer.toString(randomIntBetween(0, 10)));
        params.put("only_expunge_deletes", Boolean.TRUE.toString());

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        RestForceMergeAction action = new RestForceMergeAction();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        // Should reject on the first conflicting param it encounters (max_num_segments)
        assertEquals("cannot set only_upgrade_lucene and max_num_segments at the same time", e.getMessage());
        // The deprecation warning for only_expunge_deletes + max_num_segments fires before the only_upgrade_lucene check
        assertWarnings(
            "setting only_expunge_deletes and max_num_segments at the same time is deprecated and will be rejected in a future version"
        );
    }

    public void testOnlyUpgradeLuceneFalseWithOtherParamsIsAccepted() {
        final Map<String, String> params = new HashMap<>();
        params.put("only_upgrade_lucene", Boolean.FALSE.toString());
        params.put("max_num_segments", Integer.toString(randomIntBetween(1, 10)));

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        dispatchRequest(request);
    }
}

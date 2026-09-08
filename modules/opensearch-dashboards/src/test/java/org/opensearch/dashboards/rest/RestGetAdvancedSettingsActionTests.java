/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.rest;

import org.opensearch.dashboards.action.AdvancedSettingsResponse;
import org.opensearch.dashboards.action.GetAdvancedSettingsAction;
import org.opensearch.dashboards.action.GetAdvancedSettingsRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestGetAdvancedSettingsActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGetAdvancedSettingsAction());
    }

    public void testPrepareRequest() {
        Map<String, String> params = new HashMap<>();
        params.put("index", ".opensearch_dashboards");
        params.put("id", "config:3.7.0");

        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(actionType, instanceOf(GetAdvancedSettingsAction.class));
            assertThat(request, instanceOf(GetAdvancedSettingsRequest.class));
            GetAdvancedSettingsRequest getRequest = (GetAdvancedSettingsRequest) request;
            assertThat(getRequest.getIndex(), equalTo(".opensearch_dashboards"));
            assertThat(getRequest.getDocumentId(), equalTo("config:3.7.0"));
            return new AdvancedSettingsResponse(Map.of("theme:darkMode", true));
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_opensearch_dashboards/advanced_settings/.opensearch_dashboards/config:3.7.0")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.rest;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.dashboards.action.AdvancedSettingsResponse;
import org.opensearch.dashboards.action.WriteAdvancedSettingsAction;
import org.opensearch.dashboards.action.WriteAdvancedSettingsRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestWriteAdvancedSettingsActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestWriteAdvancedSettingsAction());
    }

    public void testPrepareRequestUpdate() {
        Map<String, String> params = new HashMap<>();
        params.put("index", ".opensearch_dashboards");
        params.put("id", "config:3.7.0");

        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(actionType, instanceOf(WriteAdvancedSettingsAction.class));
            assertThat(request, instanceOf(WriteAdvancedSettingsRequest.class));
            WriteAdvancedSettingsRequest writeRequest = (WriteAdvancedSettingsRequest) request;
            assertThat(writeRequest.getIndex(), equalTo(".opensearch_dashboards"));
            assertThat(writeRequest.getDocumentId(), equalTo("config:3.7.0"));
            assertThat(writeRequest.getOperationType(), equalTo(WriteAdvancedSettingsRequest.OperationType.UPDATE));
            assertFalse(writeRequest.isCreateOperation());
            return new AdvancedSettingsResponse(writeRequest.getDocument());
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_opensearch_dashboards/advanced_settings/.opensearch_dashboards/config:3.7.0")
            .withParams(params)
            .withContent(new BytesArray("{\"theme:darkMode\":true}"), MediaTypeRegistry.JSON)
            .build();

        dispatchRequest(request);
    }

    public void testPrepareRequestCreate() {
        Map<String, String> params = new HashMap<>();
        params.put("index", ".opensearch_dashboards");
        params.put("id", "config:3.7.0");
        params.put("operation", "CREATE");

        verifyingClient.setExecuteVerifier((actionType, request) -> {
            WriteAdvancedSettingsRequest writeRequest = (WriteAdvancedSettingsRequest) request;
            assertThat(writeRequest.getOperationType(), equalTo(WriteAdvancedSettingsRequest.OperationType.CREATE));
            assertTrue(writeRequest.isCreateOperation());
            return new AdvancedSettingsResponse(writeRequest.getDocument());
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_opensearch_dashboards/advanced_settings/.opensearch_dashboards/config:3.7.0")
            .withParams(params)
            .withContent(new BytesArray("{\"dateFormat\":\"YYYY-MM-DD\"}"), MediaTypeRegistry.JSON)
            .build();

        dispatchRequest(request);
    }

    public void testPrepareRequestCaseInsensitiveOperation() {
        Map<String, String> params = new HashMap<>();
        params.put("index", ".opensearch_dashboards");
        params.put("id", "config:3.7.0");
        params.put("operation", "create");

        verifyingClient.setExecuteVerifier((actionType, request) -> {
            WriteAdvancedSettingsRequest writeRequest = (WriteAdvancedSettingsRequest) request;
            assertThat(writeRequest.getOperationType(), equalTo(WriteAdvancedSettingsRequest.OperationType.CREATE));
            return new AdvancedSettingsResponse(writeRequest.getDocument());
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_opensearch_dashboards/advanced_settings/.opensearch_dashboards/config:3.7.0")
            .withParams(params)
            .withContent(new BytesArray("{\"key\":\"value\"}"), MediaTypeRegistry.JSON)
            .build();

        dispatchRequest(request);
    }
}

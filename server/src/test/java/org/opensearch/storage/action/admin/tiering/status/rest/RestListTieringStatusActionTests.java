/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering.status.rest;

import org.opensearch.common.Table;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusResponse;
import org.opensearch.storage.action.tiering.status.model.TieringStatus;
import org.opensearch.storage.action.tiering.status.rest.RestListTieringStatusAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RestListTieringStatusActionTests extends OpenSearchTestCase {
    private RestListTieringStatusAction handler;
    @Mock
    private NodeClient client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        handler = new RestListTieringStatusAction();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testRoutes() {
        List<RestHandler.Route> routes = handler.routes();
        assertEquals(1, routes.size());
        RestHandler.Route route = routes.get(0);
        assertEquals(RestRequest.Method.GET, route.getMethod());
        assertEquals("/_tier/all", route.getPath());
    }

    public void testGetName() {
        assertEquals("list_tiering_status", handler.getName());
    }

    public void testDocumentation() {
        StringBuilder sb = new StringBuilder();
        handler.documentation(sb);
        assertEquals("/_tier/all\n", sb.toString());
    }

    public void testGetTableWithHeader() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
        Table table = handler.getTableWithHeader(request);

        List<Table.Cell> headers = table.getHeaders();
        assertEquals("index", headers.get(0).value);
        assertEquals("state", headers.get(1).value);
        assertEquals("source", headers.get(2).value);
        assertEquals("target", headers.get(3).value);
    }

    public void testValidateTargetTier() {
        // Test valid tiers
        assertEquals("HOT", handler.parseAndValidateTargetTier("_hot"));
        assertEquals("WARM", handler.parseAndValidateTargetTier("_warm"));
        assertNull(handler.parseAndValidateTargetTier(null));

        // Test invalid tiers
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> handler.parseAndValidateTargetTier("_cold"));
        assertEquals("Invalid target tier. Target Tier must be either '_hot' or '_warm'", e.getMessage());

        // Test invalid tiers
        e = assertThrows(IllegalArgumentException.class, () -> handler.parseAndValidateTargetTier("warm"));
        assertEquals("Invalid format. Target Tier must be in the form '_tier'", e.getMessage());

    }

    public void testBuildTable() {
        List<TieringStatus> statusList = new ArrayList<>();
        TieringStatus status1 = createTieringStatus("index1", "RUNNING", "_hot", "_warm");
        TieringStatus status2 = createTieringStatus("index2", "COMPLETED", "_warm", "_hot");
        statusList.add(status1);
        statusList.add(status2);

        ListTieringStatusResponse response = new ListTieringStatusResponse(statusList);
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();

        Table table = handler.buildTable(request, response);

        List<List<Table.Cell>> rows = table.getRows();
        assertEquals(2, rows.size());

        // Verify first row
        List<Table.Cell> row1 = rows.get(0);
        assertEquals("index1", row1.get(0).value);
        assertEquals("RUNNING", row1.get(1).value);
        assertEquals("_hot", row1.get(2).value);
        assertEquals("_warm", row1.get(3).value);

        // Verify second row
        List<Table.Cell> row2 = rows.get(1);
        assertEquals("index2", row2.get(0).value);
        assertEquals("COMPLETED", row2.get(1).value);
        assertEquals("_warm", row2.get(2).value);
        assertEquals("_hot", row2.get(3).value);
    }

    public void testDoCatRequest() {
        // Test with valid target tier
        Map<String, String> params = new HashMap<>();
        params.put("target", "_hot");
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        assertNotNull(handler.doCatRequest(request, client));

        // Test with invalid target tier
        params.put("target", "_invalid");
        final RestRequest request2 = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> handler.doCatRequest(request2, client));
        assertEquals("Invalid target tier. Target Tier must be either '_hot' or '_warm'", e.getMessage());
    }

    public void testDoCatRequestWithNoTarget() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
        assertNotNull(handler.doCatRequest(request, client));
    }

    public void testBuildTableWithEmptyResponse() {
        ListTieringStatusResponse response = new ListTieringStatusResponse(Collections.emptyList());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();

        Table table = handler.buildTable(request, response);
        assertTrue(table.getRows().isEmpty());
    }

    public void testBuildTableWithNullValues() {
        List<TieringStatus> statusList = new ArrayList<>();
        TieringStatus status = new TieringStatus();
        statusList.add(status);

        ListTieringStatusResponse response = new ListTieringStatusResponse(statusList);
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();

        Table table = handler.buildTable(request, response);
        List<List<Table.Cell>> rows = table.getRows();
        assertEquals(1, rows.size());
    }

    private TieringStatus createTieringStatus(String indexName, String status, String source, String target) {
        TieringStatus tieringStatus = new TieringStatus();
        tieringStatus.setIndexName(indexName);
        tieringStatus.setStatus(status);
        tieringStatus.setSource(source);
        tieringStatus.setTarget(target);
        return tieringStatus;
    }
}

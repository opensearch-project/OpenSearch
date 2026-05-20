/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering.status.rest;

import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.storage.action.tiering.status.rest.RestGetTieringStatusAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.rest.RestRequest.Method.GET;

public class RestGetTieringStatusActionTests extends OpenSearchTestCase {
    private RestGetTieringStatusAction handler;
    @Mock
    private NodeClient client;

    private ThreadPool threadPool;

    public void setUp() throws Exception {
        super.setUp();
        handler = new RestGetTieringStatusAction();
        threadPool = new TestThreadPool("test");
        MockitoAnnotations.openMocks(this);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testRoutes() {
        List<RestHandler.Route> routes = handler.routes();
        assertEquals(1, routes.size());
        RestHandler.Route route = routes.get(0);
        assertEquals(GET, route.getMethod());
        assertEquals("/{index}/_tier", route.getPath());
    }

    public void testGetName() {
        assertEquals("get_index_tiering_status", handler.getName());
    }

    public void testPrepareRequestSingleIndex() throws IOException {
        // Arrange
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        // Act
        assertNotNull(handler.prepareRequest(request, client));

    }

    public void testPrepareRequestMultipleIndices() {
        // Arrange
        Map<String, String> params = new HashMap<>();
        params.put("index", "index1,index2");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> handler.prepareRequest(request, client));
        assertEquals("request should contain single index", exception.getMessage());
    }

    public void testPrepareRequestNoIndex() {
        // Arrange
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> handler.prepareRequest(request, client));
        assertEquals("request should contain single index", exception.getMessage());
    }

    public void testPrepareRequestEmptyIndex() {
        // Arrange
        Map<String, String> params = new HashMap<>();
        params.put("index", "");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> handler.prepareRequest(request, client));
        assertEquals("request should contain single index", exception.getMessage());
    }

    public void testExecuteRequest() throws IOException {
        // Arrange
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        assertNotNull(handler.prepareRequest(request, client));

    }
}

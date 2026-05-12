/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action;

import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DataFusionStatsAction} and {@link DataFusionPlugin} REST handler registration.
 *
 * Validates: Requirements 1.1, 1.2, 1.3, 1.4, 6.1, 6.2
 */
public class DataFusionStatsActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private NodeClient nodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        nodeClient = new NodeClient(Settings.EMPTY, threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        nodeClient.close();
    }

    // ---- Test: routes() returns GET _plugins/datafusion/stats (Requirement 1.1) ----

    public void testRoutesReturnsStatsEndpoint() {
        DataFusionService mockService = mock(DataFusionService.class);
        DataFusionStatsAction action = new DataFusionStatsAction(mockService);

        List<Route> routes = action.routes();
        assertEquals(1, routes.size());
        assertEquals(RestRequest.Method.GET, routes.get(0).getMethod());
        assertEquals("_plugins/analytics_backend_datafusion/stats", routes.get(0).getPath());
    }

    // ---- Test: getName() returns "datafusion_stats_action" (Requirement 1.1) ----

    public void testGetNameReturnsExpectedName() {
        DataFusionService mockService = mock(DataFusionService.class);
        DataFusionStatsAction action = new DataFusionStatsAction(mockService);

        assertEquals("datafusion_stats_action", action.getName());
    }

    // ---- Test: prepareRequest returns 200 with valid JSON when service returns stats (Requirement 1.3) ----

    public void testPrepareRequestReturns200WithValidJson() throws Exception {
        // Build a known DataFusionStats via direct constructors
        RuntimeMetrics io = new RuntimeMetrics(1, 2, 3, 4, 5, 6, 7, 8, 0);
        RuntimeMetrics cpu = new RuntimeMetrics(9, 10, 11, 12, 13, 14, 15, 16, 0);
        Map<String, TaskMonitorStats> taskMonitors = new LinkedHashMap<>();
        taskMonitors.put("query_execution", new TaskMonitorStats(17, 18, 19));
        taskMonitors.put("stream_next", new TaskMonitorStats(20, 21, 22));
        taskMonitors.put("fetch_phase", new TaskMonitorStats(23, 24, 25));
        DataFusionStats stats = new DataFusionStats(new NativeExecutorsStats(io, cpu, taskMonitors));

        DataFusionService mockService = mock(DataFusionService.class);
        when(mockService.getStats()).thenReturn(stats);

        DataFusionStatsAction action = new DataFusionStatsAction(mockService);

        FakeRestRequest request = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        // Execute the handler — prepareRequest returns a consumer, then handleRequest invokes it
        action.handleRequest(request, channel, nodeClient);

        // Verify the response
        assertEquals(200, channel.capturedResponse().status().getStatus());
        String responseBody = channel.capturedResponse().content().utf8ToString();
        assertFalse("Response should NOT contain native_executors wrapper", responseBody.contains("native_executors"));
        assertFalse("Response should NOT contain task_monitors wrapper", responseBody.contains("task_monitors"));
        assertTrue("Response should contain io_runtime at top level", responseBody.contains("io_runtime"));
        assertTrue("Response should contain cpu_runtime at top level", responseBody.contains("cpu_runtime"));
        assertTrue("Response should contain query_execution at top level", responseBody.contains("query_execution"));
    }

    // ---- Test: prepareRequest returns 500 when service throws exception (Requirement 6.1) ----

    public void testPrepareRequestReturns500WhenServiceThrows() throws Exception {
        DataFusionService mockService = mock(DataFusionService.class);
        when(mockService.getStats()).thenThrow(new IllegalStateException("DataFusionService has not been started"));

        DataFusionStatsAction action = new DataFusionStatsAction(mockService);

        FakeRestRequest request = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, nodeClient);

        assertEquals(500, channel.capturedResponse().status().getStatus());
        String responseBody = channel.capturedResponse().content().utf8ToString();
        assertTrue("Error response should contain exception type", responseBody.contains("illegal_state_exception"));
    }

    // ---- Test: DataFusionPlugin.getRestHandlers() returns list containing DataFusionStatsAction (Requirement 1.2) ----

    @SuppressForbidden(reason = "reflection needed to inject mock DataFusionService into plugin for testing")
    public void testPluginGetRestHandlersReturnsStatsAction() throws Exception {
        DataFusionPlugin plugin = new DataFusionPlugin();

        // Use reflection to set the dataFusionService field to a non-null mock
        DataFusionService mockService = mock(DataFusionService.class);
        Field serviceField = DataFusionPlugin.class.getDeclaredField("dataFusionService");
        serviceField.setAccessible(true);
        serviceField.set(plugin, mockService);

        List<RestHandler> handlers = plugin.getRestHandlers(Settings.EMPTY, null, null, null, null, null, null);

        assertEquals(1, handlers.size());
        assertTrue("Handler should be DataFusionStatsAction", handlers.get(0) instanceof DataFusionStatsAction);
    }

    // ---- Test: DataFusionPlugin.getRestHandlers() returns empty list when dataFusionService is null (Requirement 1.4) ----

    public void testPluginGetRestHandlersReturnsEmptyWhenServiceNull() {
        DataFusionPlugin plugin = new DataFusionPlugin();
        // dataFusionService is null by default (createComponents not called)

        List<RestHandler> handlers = plugin.getRestHandlers(Settings.EMPTY, null, null, null, null, null, null);

        assertTrue("Should return empty list when service is null", handlers.isEmpty());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import org.opensearch.action.ActionRequest;
import org.opensearch.client.node.PluginAwareNodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.insights.core.listener.QueryInsightsListener;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.resthandler.top_queries.RestTopQueriesAction;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.rest.RestHandler;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;

public class QueryInsightsPluginTests extends OpenSearchTestCase {

    private QueryInsightsPlugin queryInsightsPlugin;

    private final PluginAwareNodeClient client = mock(PluginAwareNodeClient.class);
    private ClusterService clusterService;
    private final ThreadPool threadPool = mock(ThreadPool.class);

    @Before
    public void setup() {
        queryInsightsPlugin = new QueryInsightsPlugin();
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        clusterService = ClusterServiceUtils.createClusterService(settings, clusterSettings, threadPool);
    }

    public void testGetSettings() {
        assertEquals(
            Arrays.asList(
                QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED,
                QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE,
                QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE,
                QueryInsightsSettings.TOP_N_LATENCY_EXPORTER_SETTINGS,
                QueryInsightsSettings.TOP_N_CPU_QUERIES_ENABLED,
                QueryInsightsSettings.TOP_N_CPU_QUERIES_SIZE,
                QueryInsightsSettings.TOP_N_CPU_QUERIES_WINDOW_SIZE,
                QueryInsightsSettings.TOP_N_CPU_EXPORTER_SETTINGS,
                QueryInsightsSettings.TOP_N_MEMORY_QUERIES_ENABLED,
                QueryInsightsSettings.TOP_N_MEMORY_QUERIES_SIZE,
                QueryInsightsSettings.TOP_N_MEMORY_QUERIES_WINDOW_SIZE,
                QueryInsightsSettings.TOP_N_MEMORY_EXPORTER_SETTINGS
            ),
            queryInsightsPlugin.getSettings()
        );
    }

    public void testCreateComponent() {
        List<Object> components = (List<Object>) queryInsightsPlugin.createComponents(
            client,
            clusterService,
            threadPool,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertEquals(2, components.size());
        assertTrue(components.get(0) instanceof QueryInsightsService);
        assertTrue(components.get(1) instanceof QueryInsightsListener);
    }

    public void testGetExecutorBuilders() {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        List<ExecutorBuilder<?>> executorBuilders = queryInsightsPlugin.getExecutorBuilders(settings);
        assertEquals(1, executorBuilders.size());
        assertTrue(executorBuilders.get(0) instanceof ScalingExecutorBuilder);
    }

    public void testGetRestHandlers() {
        List<RestHandler> components = queryInsightsPlugin.getRestHandlers(Settings.EMPTY, null, null, null, null, null, null);
        assertEquals(1, components.size());
        assertTrue(components.get(0) instanceof RestTopQueriesAction);
    }

    public void testGetActions() {
        List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> components = queryInsightsPlugin.getActions();
        assertEquals(1, components.size());
        assertTrue(components.get(0).getAction() instanceof TopQueriesAction);
    }

}

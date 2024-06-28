/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.top_queries;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesResponse;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.util.List;

import static org.mockito.Mockito.mock;

public class TransportTopQueriesActionTests extends OpenSearchTestCase {

    private final ThreadPool threadPool = mock(ThreadPool.class);

    private final Settings.Builder settingsBuilder = Settings.builder();
    private final Settings settings = settingsBuilder.build();
    private final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final ClusterService clusterService = ClusterServiceUtils.createClusterService(settings, clusterSettings, threadPool);
    private final TransportService transportService = mock(TransportService.class);
    private final QueryInsightsService topQueriesByLatencyService = mock(QueryInsightsService.class);
    private final ActionFilters actionFilters = mock(ActionFilters.class);
    private final TransportTopQueriesAction transportTopQueriesAction = new TransportTopQueriesAction(
        threadPool,
        clusterService,
        transportService,
        topQueriesByLatencyService,
        actionFilters
    );
    private final DummyParentAction dummyParentAction = new DummyParentAction(
        threadPool,
        clusterService,
        transportService,
        topQueriesByLatencyService,
        actionFilters
    );

    class DummyParentAction extends TransportTopQueriesAction {
        public DummyParentAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            QueryInsightsService topQueriesByLatencyService,
            ActionFilters actionFilters
        ) {
            super(threadPool, clusterService, transportService, topQueriesByLatencyService, actionFilters);
        }

        public TopQueriesResponse createNewResponse() {
            TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY);
            return newResponse(request, List.of(), List.of());
        }
    }

    @Before
    public void setup() {
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);
    }

    public void testNewResponse() {
        TopQueriesResponse response = dummyParentAction.createNewResponse();
        assertNotNull(response);
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.Mockito;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.ResponseCollectorService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchService;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

public class SearchServiceTest {
    private static final Logger logger = LogManager.getLogger(SearchServiceTest.class);

    public static void main(String[] args) {
        // Mock or create the necessary dependencies
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        ThreadPool threadPool = Mockito.mock(ThreadPool.class);

        // Create settings with desired values for the test
        Settings settings = Settings.builder()
            .put("indices.request.cache.compute_intensive.iteration_count", 10000)
            .put("indices.request.cache.memory_overhead.per_iteration", 1024)
            .build();

        Mockito.when(clusterService.getSettings()).thenReturn(settings);
        // Initialize SearchService with mocked dependencies
        SearchService searchService = new SearchService(
            clusterService,
            Mockito.mock(IndicesService.class),
            threadPool,
            Mockito.mock(ScriptService.class),
            Mockito.mock(BigArrays.class),
            Mockito.mock(QueryPhase.class),
            Mockito.mock(FetchPhase.class),
            Mockito.mock(ResponseCollectorService.class),
            Mockito.mock(CircuitBreakerService.class),
            Mockito.mock(Executor.class),
            Mockito.mock(TaskResourceTrackingService.class)
        );

        // Trigger the compute-intensive task
        searchService.performComputeIntensiveTask();
    }
}

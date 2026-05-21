/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Verifies that the fragment execution handler is registered with SAME executor
 * (admission control runs on transport thread) but forks execution internally
 * via {@link AnalyticsSearchService#executeFragmentStreamingAsync}.
 */
public class AnalyticsSearchTransportServiceTests extends OpenSearchTestCase {

    public void testFragmentHandlerRegisteredWithSameExecutor() {
        StreamTransportService transportService = mock(StreamTransportService.class);
        AnalyticsSearchService searchService = mock(AnalyticsSearchService.class);
        IndicesService indicesService = mock(IndicesService.class);
        ClusterService clusterService = mock(ClusterService.class);
        TaskResourceTrackingService taskResourceTrackingService = mock(TaskResourceTrackingService.class);

        new AnalyticsSearchTransportService(transportService, clusterService, searchService, indicesService, taskResourceTrackingService);

        verify(transportService).registerRequestHandler(
            eq(FragmentExecutionAction.NAME),
            eq(ThreadPool.Names.SAME),
            anyBoolean(),
            anyBoolean(),
            any(),
            any(),
            any()
        );
    }
}

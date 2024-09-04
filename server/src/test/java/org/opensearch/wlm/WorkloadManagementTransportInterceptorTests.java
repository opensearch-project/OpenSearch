/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.wlm.WorkloadManagementTransportInterceptor.RequestHandler;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService;

import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.threadpool.ThreadPool.Names.SAME;

public class WorkloadManagementTransportInterceptorTests extends OpenSearchTestCase {
    private QueryGroupResourceUsageTrackerService mockQueryGroupUsageTracker;
    private ClusterService mockClusterService;
    private ThreadPool mockThreadPool;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    private ThreadPool threadPool;
    private WorkloadManagementTransportInterceptor sut;

    public void setUp() throws Exception {
        super.setUp();
        mockQueryGroupUsageTracker = mock(QueryGroupResourceUsageTrackerService.class);
        mockClusterService = mock(ClusterService.class);
        mockThreadPool = mock(ThreadPool.class);
        mockWorkloadManagementSettings = mock(WorkloadManagementSettings.class);
        threadPool = new TestThreadPool(getTestName());
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(metadata);
        when(metadata.queryGroups()).thenReturn(Collections.emptyMap());
        sut = new WorkloadManagementTransportInterceptor(threadPool,
            new QueryGroupService(
                mockQueryGroupUsageTracker,
                mockClusterService,
                mockThreadPool,
                mockWorkloadManagementSettings,
                new HashMap<>()
            )
        );
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testInterceptHandler() {
        TransportRequestHandler<TransportRequest> requestHandler = sut.interceptHandler("Search", SAME, false, null);
        assertTrue(requestHandler instanceof RequestHandler);
    }
}

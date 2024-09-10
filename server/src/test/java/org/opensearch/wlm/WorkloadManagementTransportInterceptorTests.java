/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

<<<<<<< HEAD
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
=======
>>>>>>> b5cbfa4de9e (changelog)
import org.opensearch.common.SetOnce;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.WorkloadManagementTransportInterceptor.RequestHandler;
import org.opensearch.wlm.cancellation.QueryGroupTaskCancellationService;

import java.util.Collections;

import static org.opensearch.threadpool.ThreadPool.Names.SAME;
import static org.mockito.Mockito.mock;
<<<<<<< HEAD
import static org.mockito.Mockito.when;
=======
>>>>>>> b5cbfa4de9e (changelog)

public class WorkloadManagementTransportInterceptorTests extends OpenSearchTestCase {
    private QueryGroupTaskCancellationService mockTaskCancellationService;
    private TransportService mockTransportService;
    private ClusterService mockClusterService;
    private ThreadPool mockThreadPool;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    private ThreadPool threadPool;
    private WorkloadManagementTransportInterceptor sut;
    private QueryGroupsStateAccessor stateAccessor;

    public void setUp() throws Exception {
        super.setUp();
        mockTaskCancellationService = mock(QueryGroupTaskCancellationService.class);
        mockClusterService = mock(ClusterService.class);
        mockThreadPool = mock(ThreadPool.class);
        mockWorkloadManagementSettings = mock(WorkloadManagementSettings.class);
        threadPool = new TestThreadPool(getTestName());
<<<<<<< HEAD
        mockTransportService = mock(TransportService.class);
        stateAccessor = new QueryGroupsStateAccessor();

        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(metadata);
        when(metadata.queryGroups()).thenReturn(Collections.emptyMap());
        SetOnce<QueryGroupService> queryGroupServiceSetOnce = new SetOnce<>();
        sut = new WorkloadManagementTransportInterceptor(threadPool, queryGroupServiceSetOnce);
=======
        sut = new WorkloadManagementTransportInterceptor(threadPool, new SetOnce<>(mock(QueryGroupService.class)));
>>>>>>> b5cbfa4de9e (changelog)
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

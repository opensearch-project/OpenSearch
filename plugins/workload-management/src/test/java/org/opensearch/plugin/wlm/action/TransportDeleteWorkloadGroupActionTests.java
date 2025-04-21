/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportDeleteWorkloadGroupActionTests extends OpenSearchTestCase {

    ClusterService clusterService = mock(ClusterService.class);
    TransportService transportService = mock(TransportService.class);
    ActionFilters actionFilters = mock(ActionFilters.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
    WorkloadGroupPersistenceService workloadGroupPersistenceService = mock(WorkloadGroupPersistenceService.class);

    TransportDeleteWorkloadGroupAction action = new TransportDeleteWorkloadGroupAction(
        clusterService,
        transportService,
        actionFilters,
        threadPool,
        indexNameExpressionResolver,
        workloadGroupPersistenceService
    );

    /**
     * Test case to validate the construction for TransportDeleteWorkloadGroupAction
     */
    public void testConstruction() {
        assertNotNull(action);
        assertEquals(ThreadPool.Names.SAME, action.executor());
    }

    /**
     * Test case to validate the clusterManagerOperation function in TransportDeleteWorkloadGroupAction
     */
    public void testClusterManagerOperation() throws Exception {
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest("testGroup");
        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        ClusterState clusterState = mock(ClusterState.class);
        action.clusterManagerOperation(request, clusterState, listener);
        verify(workloadGroupPersistenceService).deleteInClusterStateMetadata(eq(request), eq(listener));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.clusterState;
import static org.mockito.Mockito.mock;

public class TransportGetWorkloadGroupActionTests extends OpenSearchTestCase {

    /**
     * Test case for ClusterManagerOperation function
     */
    @SuppressWarnings("unchecked")
    public void testClusterManagerOperation() throws Exception {
        GetWorkloadGroupRequest getWorkloadGroupRequest1 = new GetWorkloadGroupRequest(NAME_NONE_EXISTED);
        GetWorkloadGroupRequest getWorkloadGroupRequest2 = new GetWorkloadGroupRequest(NAME_ONE);
        TransportGetWorkloadGroupAction transportGetWorkloadGroupAction = new TransportGetWorkloadGroupAction(
            mock(ClusterService.class),
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            mock(IndexNameExpressionResolver.class)
        );
        assertThrows(
            ResourceNotFoundException.class,
            () -> transportGetWorkloadGroupAction.clusterManagerOperation(
                getWorkloadGroupRequest1,
                clusterState(),
                mock(ActionListener.class)
            )
        );
        transportGetWorkloadGroupAction.clusterManagerOperation(getWorkloadGroupRequest2, clusterState(), mock(ActionListener.class));
    }
}

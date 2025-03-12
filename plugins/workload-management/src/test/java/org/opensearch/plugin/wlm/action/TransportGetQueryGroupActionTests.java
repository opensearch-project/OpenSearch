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
import org.opensearch.plugin.wlm.action.GetWorkloadGroupRequest;
import org.opensearch.plugin.wlm.action.TransportGetWorkloadGroupAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

<<<<<<<< HEAD:plugins/workload-management/src/test/java/org/opensearch/plugin/wlm/querygroup/action/TransportGetWorkloadGroupActionTests.java
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.clusterState;
========
import static org.opensearch.plugin.wlm.querygroup.QueryGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.querygroup.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.querygroup.QueryGroupTestUtils.clusterState;
>>>>>>>> c83500db863 (add update rule api logic):plugins/workload-management/src/test/java/org/opensearch/plugin/wlm/querygroup/action/TransportGetQueryGroupActionTests.java
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

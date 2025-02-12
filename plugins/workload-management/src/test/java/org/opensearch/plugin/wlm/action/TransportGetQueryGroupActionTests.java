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

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.clusterState;
import static org.mockito.Mockito.mock;

public class TransportGetQueryGroupActionTests extends OpenSearchTestCase {

    /**
     * Test case for ClusterManagerOperation function
     */
    @SuppressWarnings("unchecked")
    public void testClusterManagerOperation() throws Exception {
        GetQueryGroupRequest getQueryGroupRequest1 = new GetQueryGroupRequest(NAME_NONE_EXISTED);
        GetQueryGroupRequest getQueryGroupRequest2 = new GetQueryGroupRequest(NAME_ONE);
        TransportGetQueryGroupAction transportGetQueryGroupAction = new TransportGetQueryGroupAction(
            mock(ClusterService.class),
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            mock(IndexNameExpressionResolver.class)
        );
        assertThrows(
            ResourceNotFoundException.class,
            () -> transportGetQueryGroupAction.clusterManagerOperation(getQueryGroupRequest1, clusterState(), mock(ActionListener.class))
        );
        transportGetQueryGroupAction.clusterManagerOperation(getQueryGroupRequest2, clusterState(), mock(ActionListener.class));
    }
}

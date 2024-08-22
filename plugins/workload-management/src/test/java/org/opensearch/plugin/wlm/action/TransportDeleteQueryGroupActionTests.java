/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.service.QueryGroupPersistenceService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportDeleteQueryGroupActionTests extends OpenSearchTestCase {

    ClusterService clusterService = mock(ClusterService.class);
    TransportService transportService = mock(TransportService.class);
    ActionFilters actionFilters = mock(ActionFilters.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
    QueryGroupPersistenceService queryGroupPersistenceService = mock(QueryGroupPersistenceService.class);

    TransportDeleteQueryGroupAction action = new TransportDeleteQueryGroupAction(
        clusterService,
        transportService,
        actionFilters,
        threadPool,
        indexNameExpressionResolver,
        queryGroupPersistenceService
    );

    /**
     * Test case to validate the construction for TransportDeleteQueryGroupAction
     */
    public void testConstruction() {
        assertNotNull(action);
        assertEquals(ThreadPool.Names.SAME, action.executor());
    }

    /**
     * Test case to validate the clusterManagerOperation function in TransportDeleteQueryGroupAction
     */
    public void testClusterManagerOperation() throws Exception {
        DeleteQueryGroupRequest request = new DeleteQueryGroupRequest("testGroup");
        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        ClusterState clusterState = mock(ClusterState.class);
        action.clusterManagerOperation(request, clusterState, listener);
        verify(queryGroupPersistenceService).deleteInClusterStateMetadata(eq(request), eq(listener));
    }
}

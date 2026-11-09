/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.storage.action.tiering.CancelTieringAction;
import org.opensearch.storage.action.tiering.CancelTieringRequest;
import org.opensearch.storage.action.tiering.TransportCancelTierAction;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.storage.tiering.WarmToHotTieringService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for TransportCancelTierAction.
 * Tests basic action functionality, construction, and public interface.
 */
public class TransportCancelTieringActionTests extends OpenSearchTestCase {

    private TransportCancelTierAction action;
    private HotToWarmTieringService hotToWarmTieringService;
    private WarmToHotTieringService warmToHotTieringService;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        TransportService transportService = mock(TransportService.class);
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        hotToWarmTieringService = mock(HotToWarmTieringService.class);
        warmToHotTieringService = mock(WarmToHotTieringService.class);

        action = new TransportCancelTierAction(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            hotToWarmTieringService,
            warmToHotTieringService
        );
    }

    public void testActionName() {
        assertEquals(CancelTieringAction.NAME, "indices:admin/_tier/cancel");
    }

    public void testRequestModelValidation() {
        // Test that the request model works correctly with the action
        CancelTieringRequest validRequest = new CancelTieringRequest();
        validRequest.setIndex("test-index");
        assertEquals("Request should contain correct index", "test-index", validRequest.getIndex());

        // Test request validation
        assertNull("Valid request should pass validation", validRequest.validate());
    }

    public void testRequestModelInvalidValidation() {
        // Test invalid request
        CancelTieringRequest invalidRequest = new CancelTieringRequest();
        invalidRequest.setIndex("");
        assertNotNull("Empty index should fail validation", invalidRequest.validate());

        CancelTieringRequest nullRequest = new CancelTieringRequest();
        nullRequest.setIndex(null);
        assertNotNull("Null index should fail validation", nullRequest.validate());
    }
}

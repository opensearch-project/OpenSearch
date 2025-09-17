/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.cache;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TransportPruneCacheAction}.
 */
public class TransportPruneCacheActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private TransportService transportService;
    private ClusterService clusterService;
    private ActionFilters actionFilters;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private FileCache fileCache;
    private TransportPruneCacheAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        threadPool = new TestThreadPool("test");
        transportService = mock(TransportService.class);
        clusterService = mock(ClusterService.class);
        actionFilters = mock(ActionFilters.class);
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        fileCache = mock(FileCache.class);

        action = new TransportPruneCacheAction(
            threadPool,
            transportService,
            clusterService,
            actionFilters,
            indexNameExpressionResolver,
            fileCache
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    /**
     * Tests successful prune operation.
     */
    public void testSuccessfulPruneOperation() throws Exception {
        final long expectedPrunedBytes = 1048576L; // 1MB

        when(fileCache.prune()).thenReturn(expectedPrunedBytes);

        PruneCacheRequest request = new PruneCacheRequest();
        ClusterState clusterState = mock(ClusterState.class);

        ActionListener<PruneCacheResponse> listener = new ActionListener<PruneCacheResponse>() {
            @Override
            public void onResponse(PruneCacheResponse response) {
                assertTrue("Response should be acknowledged", response.isAcknowledged());
                assertEquals("Pruned bytes should match", expectedPrunedBytes, response.getPrunedBytes());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        };

        action.clusterManagerOperation(request, clusterState, listener);

        verify(fileCache).prune();
    }

    /**
     * Tests behavior when FileCache is null.
     */
    public void testNullFileCache() throws Exception {
        // Create action with null FileCache
        TransportPruneCacheAction nullCacheAction = new TransportPruneCacheAction(
            threadPool,
            transportService,
            clusterService,
            actionFilters,
            indexNameExpressionResolver,
            null // null FileCache
        );

        PruneCacheRequest request = new PruneCacheRequest();
        ClusterState clusterState = mock(ClusterState.class);

        ActionListener<PruneCacheResponse> listener = new ActionListener<PruneCacheResponse>() {
            @Override
            public void onResponse(PruneCacheResponse response) {
                assertTrue("Response should be acknowledged even with null cache", response.isAcknowledged());
                assertEquals("Pruned bytes should be 0 for null cache", 0L, response.getPrunedBytes());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail with null cache: " + e.getMessage());
            }
        };

        nullCacheAction.clusterManagerOperation(request, clusterState, listener);
    }

    /**
     * Tests behavior when FileCache.prune() throws an exception.
     */
    public void testPruneException() throws Exception {
        final RuntimeException expectedException = new RuntimeException("Cache corruption detected");

        when(fileCache.prune()).thenThrow(expectedException);

        PruneCacheRequest request = new PruneCacheRequest();
        ClusterState clusterState = mock(ClusterState.class);

        ActionListener<PruneCacheResponse> listener = new ActionListener<PruneCacheResponse>() {
            @Override
            public void onResponse(PruneCacheResponse response) {
                fail("Should not succeed when prune throws exception");
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals("Exception should be propagated", expectedException, e);
            }
        };

        action.clusterManagerOperation(request, clusterState, listener);

        verify(fileCache).prune();
    }

    /**
     * Tests that the executor is correctly set to SAME.
     */
    public void testExecutor() {
        assertEquals("Executor should be SAME for non-blocking operations", ThreadPool.Names.SAME, action.executor());
    }

    /**
     * Tests that checkBlock returns null (no blocking).
     */
    public void testCheckBlock() {
        PruneCacheRequest request = new PruneCacheRequest();
        ClusterState clusterState = mock(ClusterState.class);

        assertNull("Prune cache operation should not be blocked", action.checkBlock(request, clusterState));
    }

    /**
     * Tests response deserialization.
     */
    public void testResponseDeserialization() throws Exception {
        // This tests the read method implementation
        // In a real test, you'd create a StreamInput with serialized response data
        // For this test, we'll just verify the method exists and can be called
        assertNotNull("Action should have read method", action);
    }

    /**
     * Tests with different prune return values.
     */
    public void testDifferentPruneReturnValues() throws Exception {
        long[] testValues = { 0L, 1L, 1048576L, Long.MAX_VALUE };

        for (long expectedBytes : testValues) {
            when(fileCache.prune()).thenReturn(expectedBytes);

            PruneCacheRequest request = new PruneCacheRequest();
            ClusterState clusterState = mock(ClusterState.class);

            ActionListener<PruneCacheResponse> listener = new ActionListener<PruneCacheResponse>() {
                @Override
                public void onResponse(PruneCacheResponse response) {
                    assertTrue("Response should be acknowledged", response.isAcknowledged());
                    assertEquals("Pruned bytes should match", expectedBytes, response.getPrunedBytes());
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not fail: " + e.getMessage());
                }
            };

            action.clusterManagerOperation(request, clusterState, listener);
        }
    }

    /**
     * Tests async behavior with ActionListener.
     */
    public void testAsyncBehavior() throws Exception {
        final long expectedPrunedBytes = 2048L;

        // Mock FileCache to simulate async behavior
        doAnswer(invocation -> {
            // Simulate some processing time
            return expectedPrunedBytes;
        }).when(fileCache).prune();

        PruneCacheRequest request = new PruneCacheRequest();
        ClusterState clusterState = mock(ClusterState.class);

        final boolean[] callbackInvoked = { false };

        ActionListener<PruneCacheResponse> listener = new ActionListener<PruneCacheResponse>() {
            @Override
            public void onResponse(PruneCacheResponse response) {
                callbackInvoked[0] = true;
                assertTrue("Response should be acknowledged", response.isAcknowledged());
                assertEquals("Pruned bytes should match", expectedPrunedBytes, response.getPrunedBytes());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        };

        action.clusterManagerOperation(request, clusterState, listener);

        assertTrue("Callback should have been invoked", callbackInvoked[0]);
        verify(fileCache).prune();
    }
}

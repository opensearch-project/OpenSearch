/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.canmatch.AnalyticsCanMatchResponse;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.arrow.spi.PoolGroup;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class AnalyticsSearchServiceTests extends OpenSearchTestCase {

    private static final String BACKEND_ID = "test-backend";

    private ArrowNativeAllocator nativeAllocator;
    private AnalyticsSearchBackendPlugin backend;
    private TestAnalyticsSearchService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        nativeAllocator = new ArrowNativeAllocator();
        nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_QUERY, 0, Long.MAX_VALUE, PoolGroup.SEARCH);
        backend = mock(AnalyticsSearchBackendPlugin.class);
        service = new TestAnalyticsSearchService(Map.of(BACKEND_ID, backend), nativeAllocator);
    }

    @Override
    public void tearDown() throws Exception {
        service.close();
        nativeAllocator.close();
        super.tearDown();
    }

    public void testCanMatchWaitsForShardSearchActivationBeforeDispatch() {
        IndexShard shard = mock(IndexShard.class);
        Executor executor = mock(Executor.class);
        @SuppressWarnings("unchecked")
        ActionListener<AnalyticsCanMatchResponse> listener = mock(ActionListener.class);

        service.canMatch(shard, new byte[] { 1 }, BACKEND_ID, "timestamp", executor, listener);

        verifyNoInteractions(executor, backend, listener);
        service.completeActivation();

        verify(executor).execute(any(Runnable.class));
        verifyNoInteractions(backend, listener);
    }

    public void testCanMatchEvaluatesOnlyAfterActivationCallbackRuns() {
        IndexShard shard = mock(IndexShard.class);
        Executor executor = Runnable::run;
        byte[] filters = new byte[] { 1 };
        @SuppressWarnings("unchecked")
        ActionListener<AnalyticsCanMatchResponse> listener = mock(ActionListener.class);
        when(backend.canMatchWithBounds(shard, filters, "timestamp")).thenReturn(AnalyticsSearchBackendPlugin.CanMatchResult.pruned());

        service.canMatch(shard, filters, BACKEND_ID, "timestamp", executor, listener);
        verifyNoInteractions(backend, listener);

        service.completeActivation();

        verify(backend).canMatchWithBounds(shard, filters, "timestamp");
        verify(listener).onResponse(argThat(response -> response.canMatch() == false));
    }

    public void testFragmentWaitsForShardSearchActivationBeforeDispatch() {
        IndexShard shard = mock(IndexShard.class);
        Executor executor = mock(Executor.class);
        FragmentExecutionRequest request = mock(FragmentExecutionRequest.class);
        AnalyticsShardTask task = mock(AnalyticsShardTask.class);
        AnalyticsSearchService.StreamingFragmentResponseHandler responseHandler = mock(
            AnalyticsSearchService.StreamingFragmentResponseHandler.class
        );

        service.executeFragmentStreamingAsync(request, shard, task, responseHandler, executor);

        verifyNoInteractions(executor, responseHandler);
        service.completeActivation();

        verify(executor).execute(any(Runnable.class));
        verifyNoInteractions(responseHandler);
    }

    private static class TestAnalyticsSearchService extends AnalyticsSearchService {
        private Consumer<Boolean> activationListener;

        TestAnalyticsSearchService(Map<String, AnalyticsSearchBackendPlugin> backends, ArrowNativeAllocator nativeAllocator) {
            super(backends, nativeAllocator);
        }

        @Override
        void awaitShardSearchActive(IndexShard shard, Consumer<Boolean> listener) {
            assertNull(activationListener);
            activationListener = listener;
        }

        void completeActivation() {
            assertNotNull(activationListener);
            Consumer<Boolean> listener = activationListener;
            activationListener = null;
            listener.accept(true);
        }
    }
}

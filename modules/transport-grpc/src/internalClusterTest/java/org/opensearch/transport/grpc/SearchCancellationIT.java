/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tasks.Task;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * Integration test verifying that cancelling a gRPC search call causes the server to cancel the underlying
 * search task, instead of running it to completion. This is the gRPC counterpart to the HTTP behavior in
 * {@code RestCancellableNodeClient} (exercised by {@code SearchRestCancellationIT}).
 * <p>
 * The fix's contract is narrow: an abandoned gRPC call must trigger a {@code CancelTasks} for that search's
 * task. This test asserts exactly that, and nothing more -- that {@code CancelTasks} then cancels the task is
 * core behavior already covered by {@code server}'s {@code SearchCancellationIT}. It is fully event-driven
 * (latches on the search entering the query phase and on the cancellation being issued), so it does not
 * depend on timing; the timeouts are only safety nets so a regression fails fast rather than hanging.
 */
public class SearchCancellationIT extends GrpcTransportBaseIT {

    private static final long SAFETY_TIMEOUT_SECONDS = 30;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // GrpcPlugin provides the transport; SearchBlockPlugin holds a search in the query phase and signals
        // when the cancellation is issued for its task.
        return Arrays.asList(GrpcPlugin.class, SearchBlockPlugin.class);
    }

    public void testGrpcClientCancellationCancelsSearchTask() throws Exception {
        String indexName = "test-grpc-cancellation";
        createTestIndex(indexName);
        indexTestDocument(indexName, "1", DEFAULT_DOCUMENT_SOURCE);

        SearchBlockPlugin.reset();

        try (NettyGrpcClient client = createGrpcClient()) {
            SearchServiceGrpc.SearchServiceStub asyncStub = SearchServiceGrpc.newStub(client.getChannel());
            SearchRequest searchRequest = SearchRequest.newBuilder()
                .addIndex(indexName)
                .setSearchRequestBody(SearchRequestBody.newBuilder().setSize(10).build())
                .build();

            try (Context.CancellableContext cancellableContext = Context.current().withCancellation()) {
                // Bind the RPC to a cancellable context so we can abandon it mid-flight, as a real client (or a
                // deadline) would -- the gRPC analogue of the HTTP client's Cancellable.cancel().
                cancellableContext.run(() -> asyncStub.search(searchRequest, new NoopObserver()));

                // Wait until the search is actually executing in the query phase
                assertTrue(
                    "search did not reach the query phase",
                    SearchBlockPlugin.blockEntered.await(SAFETY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                );
                SearchBlockPlugin.expectedTaskId = SearchBlockPlugin.coordinatorTaskId;
                assertNotNull("coordinator task id was not captured", SearchBlockPlugin.expectedTaskId);

                // Abandon the call.
                cancellableContext.cancel(Status.CANCELLED.withDescription("client cancelled").asRuntimeException());

                // The fix must translate that into a CancelTasks for the search task. Without it, this latch never
                // fires and the await returns false, failing the test.
                assertTrue(
                    "gRPC cancellation was not propagated to a task cancellation",
                    SearchBlockPlugin.cancelObserved.await(SAFETY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                );
            } finally {
                // Release the (now cancelled) search so it can unwind and the node shuts down cleanly.
                SearchBlockPlugin.releaseBlock();
            }
        }
    }

    /** A gRPC observer that ignores all signals; the test drives cancellation out-of-band via the context. */
    private static class NoopObserver implements StreamObserver<SearchResponse> {
        @Override
        public void onNext(SearchResponse value) {}

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {}
    }

    /**
     * Test plugin that (1) blocks every shard-level query phase until released, holding a search in flight
     * independent of the query DSL, and (2) installs an action filter that signals when a {@code CancelTasks}
     * targeting the blocked search's coordinator task is issued.
     */
    public static class SearchBlockPlugin extends Plugin implements ActionPlugin {
        static volatile CountDownLatch blockEntered = new CountDownLatch(1);
        static volatile CountDownLatch blockReleased = new CountDownLatch(1);
        static volatile CountDownLatch cancelObserved = new CountDownLatch(1);
        static volatile TaskId coordinatorTaskId;
        static volatile TaskId expectedTaskId;

        static void reset() {
            blockEntered = new CountDownLatch(1);
            blockReleased = new CountDownLatch(1);
            cancelObserved = new CountDownLatch(1);
            coordinatorTaskId = null;
            expectedTaskId = null;
        }

        static void releaseBlock() {
            blockReleased.countDown();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    // Capture the coordinator task (parent of this shard task) and signal that the search is in flight.
                    coordinatorTaskId = searchContext.getTask().getParentTaskId();
                    blockEntered.countDown();
                    try {
                        // Hold the query phase open until the test releases it. The timeout is only a safety net;
                        // correctness comes from releaseBlock(), not from elapsed time.
                        if (blockReleased.await(SAFETY_TIMEOUT_SECONDS, TimeUnit.SECONDS) == false) {
                            throw new RuntimeException("search block was not released");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return Collections.singletonList(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionRequestMetadata<Request, Response> actionRequestMetadata,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (CancelTasksAction.NAME.equals(action) && request instanceof CancelTasksRequest) {
                        TaskId cancelledTaskId = ((CancelTasksRequest) request).getTaskId();
                        TaskId expected = expectedTaskId;
                        if (expected != null && expected.equals(cancelledTaskId)) {
                            cancelObserved.countDown();
                        }
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }
}

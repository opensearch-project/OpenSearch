/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.OpenSearchNetty4IntegTestCase;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Verifies that a task that times out while being waited on via {@code GET _tasks/<id>?wait_for_completion=true}
 * surfaces as an HTTP 504 (Gateway Timeout), not a 500, now that {@link org.opensearch.OpenSearchTimeoutException}
 * overrides {@code status()}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class TasksWaitForTimeoutIT extends OpenSearchNetty4IntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable a real http transport so we can assert on the actual REST status code
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(BlockingActionPlugin.class);
        return plugins;
    }

    public void testGetTaskWaitForCompletionReturnsGatewayTimeoutOverHttp() throws Exception {
        ActionFuture<BlockingAction.Response> future = client().execute(BlockingAction.INSTANCE, new BlockingAction.Request());
        try {
            assertBusy(
                () -> assertThat(
                    client().admin().cluster().prepareListTasks().setActions(BlockingAction.NAME).get().getTasks(),
                    hasSize(1)
                )
            );
            String taskId = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(BlockingAction.NAME)
                .get()
                .getTasks()
                .get(0)
                .getTaskId()
                .toString();

            RestClient restClient = getRestClient();
            ResponseException e = expectThrows(
                ResponseException.class,
                () -> restClient.performRequest(new Request("GET", "/_tasks/" + taskId + "?wait_for_completion=true&timeout=100ms"))
            );
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(504));
        } finally {
            BlockingActionPlugin.LATCH.countDown();
        }
        future.get();
    }

    public static class BlockingActionPlugin extends Plugin implements ActionPlugin {
        static final CountDownLatch LATCH = new CountDownLatch(1);

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Collections.singletonList(new ActionHandler<>(BlockingAction.INSTANCE, TransportBlockingAction.class));
        }
    }

    public static class TransportBlockingAction extends HandledTransportAction<BlockingAction.Request, BlockingAction.Response> {
        private final ThreadPool threadPool;

        @Inject
        public TransportBlockingAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
            super(BlockingAction.NAME, transportService, actionFilters, BlockingAction.Request::new);
            this.threadPool = threadPool;
        }

        @Override
        protected void doExecute(Task task, BlockingAction.Request request, ActionListener<BlockingAction.Response> listener) {
            // NodeClient executes local actions synchronously on the calling thread, so we must hand
            // off to the generic pool ourselves rather than block here, or callers (and this test's
            // main thread) would deadlock waiting on the future before it's even returned.
            threadPool.generic().execute(() -> {
                try {
                    BlockingActionPlugin.LATCH.await();
                    listener.onResponse(new BlockingAction.Response());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    listener.onFailure(e);
                }
            });
        }
    }

    public static class BlockingAction extends ActionType<BlockingAction.Response> {
        static final String NAME = "cluster:admin/test/blocking";
        static final BlockingAction INSTANCE = new BlockingAction();

        private BlockingAction() {
            super(NAME, Response::new);
        }

        public static class Request extends ActionRequest {
            public Request() {}

            public Request(StreamInput in) throws IOException {
                super(in);
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }

        public static class Response extends ActionResponse {
            public Response() {}

            public Response(StreamInput in) throws IOException {
                super(in);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {}
        }
    }
}

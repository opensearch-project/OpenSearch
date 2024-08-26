/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import java.util.Optional;

/**
 * This class is used to intercept search traffic requests and populate the queryGroupId header in task headers
 */
public class WorkloadManagementTransportInterceptor implements TransportInterceptor {
    private final ThreadPool threadPool;
    private final QueryGroupService queryGroupService;

    public WorkloadManagementTransportInterceptor(final ThreadPool threadPool, final QueryGroupService queryGroupService) {
        this.threadPool = threadPool;
        this.queryGroupService = queryGroupService;
    }

    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler
    ) {
        return new RequestHandler<T>(threadPool, actualHandler, queryGroupService);
    }

    /**
     * This class is mainly used to populate the queryGroupId header
     * @param <T> T is Search related request
     */
    public static class RequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

        private final ThreadPool threadPool;
        TransportRequestHandler<T> actualHandler;
        private final QueryGroupService queryGroupService;

        public RequestHandler(ThreadPool threadPool, TransportRequestHandler<T> actualHandler, QueryGroupService queryGroupService) {
            this.threadPool = threadPool;
            this.actualHandler = actualHandler;
            this.queryGroupService = queryGroupService;
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
            if (isSearchWorkloadRequest(task)) {
                ((QueryGroupTask) task).setQueryGroupId(threadPool.getThreadContext());
                final String queryGroupId = ((QueryGroupTask) (task)).getQueryGroupId();
                Optional<String> reason = queryGroupService.shouldRejectFor(queryGroupId);

                if (reason.isPresent()) {
                    throw new OpenSearchRejectedExecutionException("QueryGroup " + queryGroupId + " is already contended." + reason.get());
                }
            }
            actualHandler.messageReceived(request, channel, task);
        }

        boolean isSearchWorkloadRequest(Task task) {
            return task instanceof QueryGroupTask;
        }
    }
}

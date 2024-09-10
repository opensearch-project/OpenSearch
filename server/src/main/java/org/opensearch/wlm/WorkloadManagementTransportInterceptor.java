/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.SetOnce;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

/**
 * This class is used to intercept search traffic requests and populate the queryGroupId header in task headers
 */
public class WorkloadManagementTransportInterceptor implements TransportInterceptor {
    private final ThreadPool threadPool;
    private final SetOnce<QueryGroupService> queryGroupService;

    public WorkloadManagementTransportInterceptor(final ThreadPool threadPool, final SetOnce<QueryGroupService> queryGroupServiceSetOnce) {
        this.threadPool = threadPool;
        this.queryGroupService = queryGroupServiceSetOnce;
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
        private final SetOnce<QueryGroupService> queryGroupService;

        public RequestHandler(
            ThreadPool threadPool,
            TransportRequestHandler<T> actualHandler,
            SetOnce<QueryGroupService> queryGroupService
        ) {
            this.threadPool = threadPool;
            this.actualHandler = actualHandler;
            this.queryGroupService = queryGroupService;
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
            if (isSearchWorkloadRequest(task)) {
                ((QueryGroupTask) task).setQueryGroupId(threadPool.getThreadContext());
                final String queryGroupId = ((QueryGroupTask) (task)).getQueryGroupId();
                assert queryGroupService.get() != null;
                queryGroupService.get().rejectIfNeeded(queryGroupId);
            }
            actualHandler.messageReceived(request, channel, task);
        }

        boolean isSearchWorkloadRequest(Task task) {
            return task instanceof QueryGroupTask;
        }
    }
}

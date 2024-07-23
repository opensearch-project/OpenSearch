/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.search.fetch.ShardFetchRequest;
import org.opensearch.search.internal.InternalScrollSearchRequest;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

/**
 * This class is mainly used to populate the queryGroupId header
 * @param <T> T is Search related request
 */
public class SearchWorkloadTransportHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

    private final ThreadPool threadPool;
    TransportRequestHandler<T> actualHandler;

    public SearchWorkloadTransportHandler(ThreadPool threadPool, TransportRequestHandler<T> actualHandler) {
        this.threadPool = threadPool;
        this.actualHandler = actualHandler;
    }

    @Override
    public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
        if (isSearchWorkloadRequest(request)) {
            task.addHeader(
                QueryGroupConstants.QUERY_GROUP_ID_HEADER,
                threadPool.getThreadContext(),
                QueryGroupConstants.DEFAULT_QUERY_GROUP_ID_SUPPLIER
            );
        }
        actualHandler.messageReceived(request, channel, task);
    }

    private boolean isSearchWorkloadRequest(TransportRequest request) {
        return (request instanceof ShardSearchRequest)
            || (request instanceof ShardFetchRequest)
            || (request instanceof InternalScrollSearchRequest)
            || (request instanceof QuerySearchRequest);
    }
}

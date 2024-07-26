/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.client.Client;
import org.opensearch.client.FilterClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;

import java.util.Map;

/**
 * This class is used for simulating request headers for testing. Used in TasksIT to
 * assert that request headers are carried to task info
 *
 * @opensearch.internal
 */
public class StashAndMergeHeadersFilterClient extends FilterClient {
    private Map<String, String> headers;

    public StashAndMergeHeadersFilterClient(Client in, Map<String, String> headers) {
        super(in);
        this.headers = headers;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        ThreadContext threadContext = threadPool().getThreadContext();
        try (ThreadContext.StoredContext ctx = threadContext.stashAndMergeHeaders(headers)) {
            super.doExecute(action, request, listener);
        }
    }
}

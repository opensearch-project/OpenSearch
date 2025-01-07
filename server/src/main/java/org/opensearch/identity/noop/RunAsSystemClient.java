/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.client.Client;
import org.opensearch.client.FilterClient;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;

/**
 * Implementation of client that will run transport actions in a stashed context
 * <p>
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * This class is used by the NoopIdentityPlugin to initialize IdentityAwarePlugins
 *
 * @opensearch.internal
 */
@InternalApi
public class RunAsSystemClient extends FilterClient {
    public RunAsSystemClient(Client delegate) {
        super(delegate);
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> actionListener
    ) {
        ThreadContext threadContext = threadPool().getThreadContext();

        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {

            ActionListener<Response> wrappedListener = ActionListener.wrap(r -> {
                ctx.restore();
                actionListener.onResponse(r);
            }, e -> {
                ctx.restore();
                actionListener.onFailure(e);
            });

            super.doExecute(action, request, wrappedListener);
        }
    }
}

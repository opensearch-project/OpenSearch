/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.client.Client;
import org.opensearch.client.FilterClient;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;

/**
 * Implementation of client that will run transport actions in a stashed context and inject the name of the provided
 * subject into the context.
 *
 * @opensearch.internal
 */
@InternalApi
public class RunAsSubjectClient extends FilterClient {

    private static final Logger logger = LogManager.getLogger(RunAsSubjectClient.class);

    public static final String SUBJECT_TRANSIENT_NAME = "subject.name";

    private final Subject subject;

    public RunAsSubjectClient(Client delegate, Subject subject) {
        super(delegate);
        this.subject = subject;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        ThreadContext threadContext = threadPool().getThreadContext();
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            threadContext.putTransient(SUBJECT_TRANSIENT_NAME, subject.getPrincipal().getName());
            logger.info("Running transport action with subject: {}", subject.getPrincipal().getName());
            super.doExecute(action, request, ActionListener.runBefore(listener, ctx::restore));
        }
    }
}
